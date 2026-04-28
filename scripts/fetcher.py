"""Shared Open-Meteo fetch + per-point ingest for the Day 1/2/3 updaters.

Day 1 (HRRR) and Days 2–3 (GFS) both read a CONUS grid from Open-Meteo
and feed the same `common.tornado_probability` blend. The only moving
parts between them are:

  * which model family to ask for (`None` → API auto-picks HRRR/RAP/GFS;
    `"gfs_seamless"` → force GFS for days 2 and 3)
  * how many hours to request (`forecast_hours`, `past_hours`)
  * the starting hour offset inside the returned hourly array

Everything else — batching, retries, bisect-on-"no data", the
derive_fields physics extraction — is identical.
"""
from __future__ import annotations

import dataclasses as _dc
import datetime as dt
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import numpy as np

import common


API_URL = "https://api.open-meteo.com/v1/forecast"

# Open-Meteo returns this on a 400 when even one coordinate in a
# multi-point request falls outside the selected model's coverage
# (e.g. open ocean, far-north Canada). One bad point nukes the whole
# batch — we detect it and bisect the pairs until only the offending
# cells are dropped.
NO_DATA_MSG = "No data is available for this location"

HOURLY_VARS = [
    "cape",
    "convective_inhibition",
    "temperature_2m",
    "dew_point_2m",
    "temperature_850hPa",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_speed_850hPa",
    "wind_direction_850hPa",
    "wind_speed_700hPa",
    "wind_direction_700hPa",
    "wind_speed_500hPa",
    "wind_direction_500hPa",
    "temperature_700hPa",
    "dew_point_700hPa",
    "temperature_500hPa",
    "freezing_level_height",
    "precipitation",
    "weather_code",
]

BATCH_SIZE = 100             # coords per Open-Meteo call
INTER_BATCH_SLEEP_S = 3.0    # gap between consecutive batches; the
                             # free tier has a per-minute throttle,
                             # not a per-day cap, so a tiny pause
                             # buys us nearly 100% success
RETRY_BACKOFF_S = [6, 18]    # ~24s worst case per batch
RATE_LIMIT_BACKOFF_S = 65    # 429 = "minutely limit" — wait it out
PER_REQUEST_TIMEOUT = 30     # seconds
MAX_BATCH_ERROR_FRAC = 0.25


@_dc.dataclass
class FetchConfig:
    forecast_hours: int          # how many forecast hours to request
    past_hours: int = 1          # how much history to include in response
    models: str | None = None    # None → Open-Meteo auto; else e.g. "gfs_seamless"


class APIError(Exception):
    """HTTPError with the response body surfaced. Open-Meteo puts
    `{"error": true, "reason": "..."}` in the body on validation
    failures and urllib otherwise drops it."""
    def __init__(self, code, reason, body=""):
        super().__init__(f"HTTP {code}: {reason} :: {body[:300]}")
        self.code = code
        self.reason = reason
        self.body = body


def _get(params, timeout=PER_REQUEST_TIMEOUT):
    qs = urllib.parse.urlencode(params)
    url = f"{API_URL}?{qs}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "thunderstorm1q/1.0 (github.com actions)",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if r.status != 200:
                raise APIError(r.status, r.reason or "non-200", "")
            return json.loads(r.read())
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        raise APIError(exc.code, exc.reason or "http-error", body) from exc


def _fetch_once(pairs, cfg: FetchConfig):
    params = {
        "latitude":  ",".join(f"{p[0]:.4f}" for p in pairs),
        "longitude": ",".join(f"{p[1]:.4f}" for p in pairs),
        "hourly":    ",".join(HOURLY_VARS),
        "forecast_hours": cfg.forecast_hours,
        "past_hours": cfg.past_hours,
        "timezone": "UTC",
        "wind_speed_unit": "ms",
    }
    if cfg.models:
        params["models"] = cfg.models
    last_err = None
    for delay in [0] + RETRY_BACKOFF_S:
        if delay:
            time.sleep(delay)
        try:
            data = _get(params)
            if isinstance(data, dict):
                data = [data]
            return data
        except APIError as exc:
            last_err = exc
            if exc.code == 429:
                # Minutely throttle. Sleep past the next minute boundary
                # so the next attempt starts in a fresh window.
                print(f"  429: sleeping {RATE_LIMIT_BACKOFF_S}s for rate-limit reset",
                      file=sys.stderr)
                time.sleep(RATE_LIMIT_BACKOFF_S)
                continue
            if exc.code == 400:
                # Structural — retrying won't help. Surface to caller
                # so the bisect path can kick in if it's "no data".
                raise
        except Exception as exc:
            last_err = exc
    raise RuntimeError(f"batch failed: {last_err!r}")


def fetch_batch(pairs, cfg: FetchConfig):
    """Fetch HOURLY_VARS for a list of (lat, lon) points. Returns a
    list of the same length, with `None` where Open-Meteo reports no
    coverage for that specific point. Bisects on the "no data" 400 so
    only bad cells are dropped."""
    if not pairs:
        return []
    try:
        return _fetch_once(pairs, cfg)
    except APIError as exc:
        if exc.code == 400 and NO_DATA_MSG in (exc.body or ""):
            if len(pairs) == 1:
                return [None]
            mid = len(pairs) // 2
            return fetch_batch(pairs[:mid], cfg) + fetch_batch(pairs[mid:], cfg)
        raise RuntimeError(f"batch failed: {exc!r}") from exc


def fetch_grid(grid, cfg: FetchConfig):
    """Fetch the whole CONUS grid serially with a small inter-batch
    pause. Open-Meteo's free tier has a per-minute throttle (not a
    per-day cap), so concurrency triggers 429s on a CONUS-scale grid;
    serializing with a few seconds of breathing room is the simplest
    way to stay safely under it.

    Raises if too many batches fail (cached JSON keeps serving the page)."""
    pairs = grid.flat_pairs()
    batches = [
        (start, pairs[start:start + BATCH_SIZE])
        for start in range(0, len(pairs), BATCH_SIZE)
    ]
    results = [None] * len(pairs)
    failures = 0
    t0 = time.monotonic()
    print(f"{len(pairs)} grid points in {len(batches)} batches "
          f"(serial, batch={BATCH_SIZE}, sleep={INTER_BATCH_SLEEP_S}s, "
          f"models={cfg.models or 'auto'}, hours={cfg.forecast_hours})")

    for i, (start, chunk) in enumerate(batches):
        if i > 0:
            time.sleep(INTER_BATCH_SLEEP_S)
        try:
            data = fetch_batch(chunk, cfg)
        except Exception as exc:
            failures += 1
            print(f"  batch {i + 1}/{len(batches)} failed: {exc!r}",
                  file=sys.stderr)
            continue
        for k, point in enumerate(data):
            results[start + k] = point

    elapsed = time.monotonic() - t0
    print(f"fetch complete in {elapsed:.1f}s "
          f"({failures}/{len(batches)} batches failed)")

    if failures / max(1, len(batches)) > MAX_BATCH_ERROR_FRAC:
        raise RuntimeError(
            f"too many batch failures: {failures}/{len(batches)}")
    if not any(r for r in results):
        raise RuntimeError("all batches failed; no data to process")

    return results, failures, len(batches)


# ---------- per-point physics extraction ----------

def derive_fields(point_hourly, hour_idx):
    """Extract the per-cell inputs needed by the tornado blend for one
    forecast hour from one Open-Meteo point's `hourly` block."""
    def g(name):
        v = point_hourly.get(name) or []
        if hour_idx < len(v) and v[hour_idx] is not None:
            return float(v[hour_idx])
        return float("nan")

    cape = g("cape")
    cin = g("convective_inhibition")
    t2  = g("temperature_2m")
    td2 = g("dew_point_2m")
    t85 = g("temperature_850hPa")
    t70 = g("temperature_700hPa")
    td70 = g("dew_point_700hPa")
    t50 = g("temperature_500hPa")
    fz  = g("freezing_level_height")
    s10 = g("wind_speed_10m");   d10 = g("wind_direction_10m")
    s85 = g("wind_speed_850hPa"); d85 = g("wind_direction_850hPa")
    s70 = g("wind_speed_700hPa"); d70 = g("wind_direction_700hPa")
    s50 = g("wind_speed_500hPa"); d50 = g("wind_direction_500hPa")
    precip = g("precipitation")
    wc_raw = g("weather_code")
    wc = int(wc_raw) if not np.isnan(wc_raw) else 0

    def uv(speed, deg):
        if np.isnan(speed) or np.isnan(deg):
            return 0.0, 0.0
        # Meteorological wind direction (FROM). Convert to components
        # of the wind vector (direction TOWARD): add 180 before project.
        rad = np.deg2rad((deg + 180.0) % 360.0)
        return speed * np.sin(rad), speed * np.cos(rad)

    u10, v10 = uv(s10, d10)
    u85, v85 = uv(s85, d85)
    u70, v70 = uv(s70, d70)
    u50, v50 = uv(s50, d50)

    shear_01 = common.shear_ms(u10, v10, u85, v85)
    shear_03 = common.shear_ms(u10, v10, u70, v70)
    shear_06 = common.shear_ms(u10, v10, u50, v50)

    # Bunkers right-mover storm motion -> true layer SRH instead of
    # the older cross-product proxy. The 0-6 km mean is approximated
    # by averaging the four wind levels we have (10 m / 850 / 700 /
    # 500 hPa); the deep-layer shear vector is 500 hPa - 10 m.
    u_mean = 0.25 * (u10 + u85 + u70 + u50)
    v_mean = 0.25 * (v10 + v85 + v70 + v50)
    cu, cv = common.bunkers_right_mover(u_mean, v_mean,
                                        u50 - u10, v50 - v10)
    srh01 = common.srh_layer(u10, v10, u85, v85, cu, cv)
    srh03 = common.srh_layer(u10, v10, u70, v70, cu, cv)
    lcl = common.lcl_height_m(t2, td2)
    low_lapse = common.low_level_lapse_rate(t2, t85)

    # Mean 0-6 km wind magnitude (cloud-layer steering speed) — the
    # severe-wind blend wants stronger gust-front speeds when the
    # whole column is screaming.
    mean6 = float(np.hypot(u_mean, v_mean))

    return dict(cape=cape, cin=cin, lcl=lcl,
                srh01=srh01, srh03=srh03,
                shear_01=shear_01, shear_03=shear_03, shear_06=shear_06,
                low_lapse=low_lapse, precip=precip, weather_code=wc,
                t_700=t70, td_700=td70, t_500=t50,
                freezing_level=fz, mean6=mean6)


def find_hour_indices(sample_hourly, first_valid_utc, n_hours):
    """Open-Meteo's hourly.time is an array of ISO timestamps. Return
    the indices for first_valid_utc, first_valid_utc+1h, ..., +n_hours-1h.
    Missing hours come back as None so callers can skip them."""
    times = sample_hourly.get("time") or []
    index = {}
    for i, t in enumerate(times):
        try:
            ts = dt.datetime.fromisoformat(t).replace(tzinfo=dt.timezone.utc)
        except ValueError:
            continue
        index[ts.replace(minute=0, second=0, microsecond=0)] = i

    indices = []
    base = first_valid_utc.replace(minute=0, second=0, microsecond=0)
    for h in range(n_hours):
        want = base + dt.timedelta(hours=h)
        indices.append(index.get(want))
    return indices


def score_hour(grid, results, src_idx, valid_dt=None):
    """Run one forecast hour through all three hazard blends and
    return a dict {tornado, wind, hail} of smoothed 2-D probability
    fields. Returns zero-fields if the hour is missing from the
    Open-Meteo response.

    If `valid_dt` is supplied, the diurnal climatology weight (peaks
    late afternoon LST, troughs around dawn) is applied to each
    hazard so a 3 a.m. warm sector doesn't read the same as a 5 pm
    one.
    """
    zeros = np.zeros(grid.shape, dtype=np.float64)
    if src_idx is None:
        return {"tornado": zeros, "wind": zeros, "hail": zeros}

    f = extract_hour_fields(grid, results, src_idx)

    tor = common.tornado_probability(
        f["cape"], f["cin"], f["lcl"], f["srh01"], f["srh03"],
        f["s01"], f["s03"], f["s06"],
        f["precip"], f["wc"], f["lapse"])
    wnd = common.wind_probability(
        f["cape"], f["cin"], f["s06"], f["mean6"],
        f["t700"], f["td700"], f["t500"],
        f["precip"], f["wc"], f["s01"], f["s03"], f["s06"])
    hl = common.hail_probability(
        f["cape"], f["cin"], f["s06"],
        f["t700"], f["t500"], f["fzlvl"], f["lcl"],
        f["precip"], f["wc"], f["s01"], f["s03"], f["s06"])

    # Sub-cell smoothing only — just enough to damp single-point
    # numerical noise without painting half a continent.
    tor = common.gaussian_smooth_2d(tor, sigma_cells=0.55)
    wnd = common.gaussian_smooth_2d(wnd, sigma_cells=0.55)
    hl  = common.gaussian_smooth_2d(hl,  sigma_cells=0.55)

    if valid_dt is not None:
        _, lons2d = np.meshgrid(grid.lats, grid.lons, indexing="ij")
        utc_h = valid_dt.hour + valid_dt.minute / 60.0
        lsh = common.local_solar_hour(utc_h, lons2d)
        diurnal = common.diurnal_factor(lsh)
        tor = tor * diurnal
        wnd = wnd * diurnal
        hl  = hl  * diurnal

    return {
        "tornado": np.clip(tor, 0.0, 1.0),
        "wind":    np.clip(wnd, 0.0, 1.0),
        "hail":    np.clip(hl,  0.0, 1.0),
    }


# Hazard keys we ship in every JSON output, in display order.
HAZARDS = ("tornado", "wind", "hail")


def compute_gfs_day(grid, results, first_valid_utc, day_offset_hours,
                    attenuation):
    """Build the 4-frame, 6-hour-max day payload used by Day 2 and
    Day 3. Each frame holds three hazard fields (tornado / wind /
    hail), each the per-cell MAX across the six hours in the window
    multiplied by `attenuation` to reflect GFS skill at long lead
    times.

    Returns (hours_payload, peaks) where peaks is keyed per hazard.
    """
    sample = next((r for r in results if r), None)
    if sample is None:
        raise RuntimeError("compute_gfs_day: no sample point available")

    day_start = first_valid_utc + dt.timedelta(hours=day_offset_hours)
    indices = find_hour_indices(sample["hourly"], day_start, 24)

    # Per-hour hazard fields (dicts {tornado, wind, hail}) for the day.
    hour_fields = [
        score_hour(grid, results, idx,
                   valid_dt=day_start + dt.timedelta(hours=h))
        for h, idx in enumerate(indices)
    ]

    hours_payload = []
    peak_fh = {h: 0 for h in HAZARDS}
    peak_score = {h: 0.0 for h in HAZARDS}

    for w in range(4):
        window = hour_fields[w * 6:(w + 1) * 6]
        midpoint = day_start + dt.timedelta(hours=w * 6 + 3)
        frame = {
            "fh": w + 1,
            "valid": common.isoformat(midpoint),
        }
        log_bits = []
        for hazard in HAZARDS:
            stacked = np.stack([wf[hazard] for wf in window], axis=0)
            win_max = np.max(stacked, axis=0) * attenuation
            cells = common.sparse_cells(grid, win_max)
            mx = float(np.nanmax(win_max)) if win_max.size else 0.0
            if mx > peak_score[hazard]:
                peak_fh[hazard], peak_score[hazard] = w + 1, mx
            frame[hazard] = {"cells": cells, "max": round(mx, 3)}
            log_bits.append(f"{hazard[0].upper()}={mx:.2f}")
        hours_payload.append(frame)
        print(f"[win {w + 1}/4  {midpoint.strftime('%Y-%m-%d %HZ')}]  "
              + " ".join(log_bits))

    peaks = {hazard: {"fh": peak_fh[hazard],
                      "score": round(peak_score[hazard], 3)}
             for hazard in HAZARDS}
    return hours_payload, peaks


def extract_hour_fields(grid, results, src_idx):
    """Build the per-grid-cell 2-D numpy arrays needed by all three
    hazard blends (tornado / wind / hail) for one forecast hour."""
    ny, nx = grid.shape
    cape  = np.full(grid.shape, np.nan)
    cin   = np.full(grid.shape, np.nan)
    lcl   = np.full(grid.shape, np.nan)
    srh01 = np.full(grid.shape, np.nan)
    srh03 = np.full(grid.shape, np.nan)
    s01   = np.full(grid.shape, np.nan)
    s03   = np.full(grid.shape, np.nan)
    s06   = np.full(grid.shape, np.nan)
    mean6 = np.full(grid.shape, np.nan)
    t700  = np.full(grid.shape, np.nan)
    td700 = np.full(grid.shape, np.nan)
    t500  = np.full(grid.shape, np.nan)
    fzlvl = np.full(grid.shape, 3000.0)
    precip = np.zeros(grid.shape)
    wc = np.zeros(grid.shape, dtype=np.int64)
    lapse = np.full(grid.shape, 6.5)

    for k, point in enumerate(results):
        if point is None:
            continue
        j = k % nx
        i = k // nx
        f = derive_fields(point["hourly"], src_idx)
        cape[i, j]  = f["cape"]
        cin[i, j]   = f["cin"]
        lcl[i, j]   = f["lcl"]
        srh01[i, j] = f["srh01"]
        srh03[i, j] = f["srh03"]
        s01[i, j]   = f["shear_01"]
        s03[i, j]   = f["shear_03"]
        s06[i, j]   = f["shear_06"]
        mean6[i, j] = f["mean6"]
        t700[i, j]  = f["t_700"]
        td700[i, j] = f["td_700"]
        t500[i, j]  = f["t_500"]
        fzlvl[i, j] = f["freezing_level"]
        precip[i, j]= f["precip"]
        wc[i, j]    = f["weather_code"]
        lapse[i, j] = f["low_lapse"]

    return dict(cape=cape, cin=cin, lcl=lcl,
                srh01=srh01, srh03=srh03,
                s01=s01, s03=s03, s06=s06, mean6=mean6,
                t700=t700, td700=td700, t500=t500, fzlvl=fzlvl,
                precip=precip, wc=wc, lapse=lapse)

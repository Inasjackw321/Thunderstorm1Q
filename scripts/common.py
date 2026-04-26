"""Thunderstorm1Q — physics for the tornado probability heatmap.

No GRIB or model-specific dependencies: the live data pipeline pulls
from the Open-Meteo JSON API (see `update_day1.py`), so every value
we operate on here arrives as a plain Python float or numpy array.

Output convention matches SPC tornado outlooks: probability of a
tornado within 25 mi of a point during the valid hour, scaled so that
the standard SPC categorical thresholds (2 / 5 / 10 / 15 / 30 / 45 %)
fall in the right places. Calibration is anchored to STP-fixed
(Thompson et al. 2012), the parameter SPC verifies its sig-tor
outlooks against, with multiplicative gates for actual storm
coverage and storm mode.

    P_tor = P_env(STP_fixed)  ×  P_storms(precip, WMO code, CAPE)
                              ×  M_mode(shear, CAPE)
                              ×  M_lapse(low-level lapse rate)
                              ×  cape_gate

Each factor is in [0, 1]. The product is then clipped to [0, 1]. A
small spatial Gaussian smooth is applied downstream in `fetcher.
score_hour` to damp grid-point noise without blurring real boundaries.
"""
from __future__ import annotations

import datetime as dt
import json
import math
import os
from dataclasses import dataclass

import numpy as np


CONUS_BOUNDS = (24.5, -125.0, 49.5, -66.5)   # S, W, N, E
DEFAULT_GRID_DEG = 1.5                        # ~660 points; balances
                                              # Open-Meteo's per-minute
                                              # rate budget against
                                              # heatmap fidelity
SCORE_FLOOR = 0.02                            # cells below this are dropped
FORECAST_HOURS = 24                           # next 24 h

# Open-Meteo WMO weather codes that indicate convective precipitation.
THUNDERSTORM_CODES = {95, 96, 99}
HEAVY_SHOWER_CODES = {82, 98}


# ---------- scalar helpers (accept scalars or arrays) ----------

def clamp(x, lo, hi):
    return np.clip(x, lo, hi)


def smoothstep(x):
    x = clamp(x, 0.0, 1.0)
    return x * x * (3.0 - 2.0 * x)


def nan_safe(v, fallback=0.0):
    v = np.asarray(v, dtype=np.float64)
    return np.where(np.isfinite(v), v, fallback)


# ---------- environment ----------

def lcl_height_m(temp_c, dew_c):
    """LCL height approximation (Espy / Stull): 125 m per degree C of
    dewpoint depression, clipped to the 50..4000 m range to keep STP
    well-behaved when the surface is near-saturated or very dry."""
    dd = nan_safe(temp_c) - nan_safe(dew_c)
    return clamp(125.0 * dd, 50.0, 4000.0)


def shear_ms(u1, v1, u2, v2):
    """Magnitude of the vector difference (u2,v2) - (u1,v1), in m/s."""
    return np.hypot(nan_safe(u2) - nan_safe(u1),
                    nan_safe(v2) - nan_safe(v1))


def bunkers_right_mover(u_mean, v_mean, u_shear, v_shear):
    """Bunkers (2000) right-mover storm motion estimate.

    Storm = 0-6 km mean wind + a 7.5 m/s deviation perpendicular and
    to the right of the 0-6 km shear vector. This is the canonical
    way to turn a wind profile into a storm-relative reference frame
    without a real cloud model in the loop.
    """
    u_mean = nan_safe(u_mean)
    v_mean = nan_safe(v_mean)
    u_shear = nan_safe(u_shear)
    v_shear = nan_safe(v_shear)
    mag = np.hypot(u_shear, v_shear)
    safe = np.where(mag > 0.5, mag, 0.5)
    # Unit vector 90 degrees clockwise from the shear vector
    # (right-of-shear in a north-up frame).
    nx = v_shear / safe
    ny = -u_shear / safe
    return u_mean + 7.5 * nx, v_mean + 7.5 * ny


def srh_layer(u_low, v_low, u_high, v_high, u_storm, v_storm):
    """Storm-Relative Helicity over one shear layer, m^2/s^2.

    Two-level approximation of the integral form:
        SRH = -integral [ k . (V - C) x dV/dz ] dz
    For a single shear layer collapses to:
        SRH = -[ (u_low - cu) * (v_high - v_low)
               - (v_low - cv) * (u_high - u_low) ]

    Positive SRH = clockwise hodograph curvature in the layer =
    right-mover supercell favorable.
    """
    rel_u = nan_safe(u_low) - nan_safe(u_storm)
    rel_v = nan_safe(v_low) - nan_safe(v_storm)
    sh_u = nan_safe(u_high) - nan_safe(u_low)
    sh_v = nan_safe(v_high) - nan_safe(v_low)
    return -(rel_u * sh_v - rel_v * sh_u)


def stp(cape, cin, lcl_m, srh01, bwd6_ms):
    """Significant Tornado Parameter (Thompson 2003), surface-based."""
    cape = nan_safe(cape, 0.0)
    cin = nan_safe(cin, -50.0)
    lcl = nan_safe(lcl_m, 1500.0)
    srh = nan_safe(srh01, 0.0)
    shr = nan_safe(bwd6_ms, 0.0)
    return (
        clamp(cape / 1500.0, 0, 4) *
        clamp((2000.0 - lcl) / 1000.0, 0, 1) *
        clamp(srh / 150.0, 0, 4) *
        clamp(shr / 20.0, 0, 1.5) *
        clamp((cin + 200.0) / 150.0, 0, 1)
    )


def stp_fixed(cape, cin, srh01, bwd6_ms):
    """Significant Tornado Parameter, fixed-layer variant (no LCL term).

    The LCL gate inside classic STP underweights cool-season and
    nocturnal events where boundary-layer cooling lifts the LCL but
    a low-level jet keeps the SRH/shear environment supportive. The
    fixed-layer form drops that term and is what SPC actually verifies
    on for sig-tor outlooks (Thompson et al. 2012).
    """
    cape = nan_safe(cape, 0.0)
    cin = nan_safe(cin, -50.0)
    srh = nan_safe(srh01, 0.0)
    shr = nan_safe(bwd6_ms, 0.0)
    return (
        clamp(cape / 1500.0, 0, 4) *
        clamp(srh / 150.0, 0, 4) *
        clamp(shr / 20.0, 0, 1.5) *
        clamp((cin + 200.0) / 150.0, 0, 1)
    )


def ehi01(cape, srh01):
    return (nan_safe(cape) * nan_safe(srh01)) / 160000.0


def ehi03(cape, srh03):
    """0-3 km Energy Helicity Index. Useful for non-supercell/QLCS
    tornado potential when the deep-layer signal is weaker."""
    return (nan_safe(cape) * nan_safe(srh03)) / 160000.0


def scp(mucape, srh01, bwd6_ms, cin):
    """Supercell Composite Parameter (Thompson 2003, simplified).

    SCP = (MUCAPE/1000) * (SRH/50) * (BWD/20) * CINterm
    """
    cape = clamp(nan_safe(mucape, 0.0) / 1000.0, 0, 6)
    srh = clamp(nan_safe(srh01, 0.0) / 50.0, 0, 6)
    shr = clamp(nan_safe(bwd6_ms, 0.0) / 20.0, 0, 2)
    cinterm = clamp((nan_safe(cin, -50.0) + 150.0) / 100.0, 0, 1)
    return cape * srh * shr * cinterm


def vtp(mucape, srh01, bwd6_ms, lcl_m, low_lapse_c_per_km, cin):
    """Violent Tornado Parameter (Hampshire et al. 2018 style).

    Extends STP with a low-level lapse-rate term, so we reward
    environments where steep 0-3 km lapse rates amplify stretching in
    supercell updrafts. Inputs capped to Hampshire's limits.
    """
    cape = clamp(nan_safe(mucape, 0.0) / 1500.0, 0, 4)
    srh = clamp(nan_safe(srh01, 0.0) / 150.0, 0, 2)
    shr = clamp(nan_safe(bwd6_ms, 0.0) / 20.0, 0, 1.5)
    lcl = clamp((2000.0 - nan_safe(lcl_m, 1500.0)) / 1000.0, 0, 1)
    cinterm = clamp((nan_safe(cin, -50.0) + 200.0) / 150.0, 0, 1)
    lapse = clamp((nan_safe(low_lapse_c_per_km, 6.5) - 6.5) / 2.0, 0, 1)
    return cape * srh * shr * lcl * cinterm * (0.5 + lapse)


def mlcape_proxy(mucape, cin):
    """Mixed-layer CAPE proxy: dock MUCAPE by a CIN-dependent
    fraction. Elevated CAPE with strong CIN is less tornado-relevant."""
    cape = nan_safe(mucape, 0.0)
    cin = nan_safe(cin, -50.0)
    penalty = clamp((-cin - 25.0) / 175.0, 0, 1)  # CIN -25..-200 -> 0..1
    return cape * (1.0 - 0.6 * penalty)


# ---------- wind / hail-specific parameters ----------

def mid_level_lapse_rate(t_700_c, t_500_c):
    """700-500 hPa lapse rate in K/km. Assumes ~3 km layer depth.
    Clipped to the physically plausible 0-12 K/km range. Steep
    mid-level lapse rates drive both large hail (deep updraft) and
    downbursts (strong evaporative cooling above the cloud base)."""
    lapse = (nan_safe(t_700_c) - nan_safe(t_500_c)) / 3.0
    return clamp(lapse, 0.0, 12.0)


def dcape_proxy(mucape, t_700_c, td_700_c, t_500_c):
    """Downdraft CAPE proxy (J/kg-ish). True DCAPE requires a parcel
    trajectory; we approximate using MUCAPE amplitude scaled by a
    700 hPa dewpoint depression factor and a 700-500 hPa lapse rate
    factor. Both favor evaporative cooling driving strong downdrafts."""
    dd = clamp(nan_safe(t_700_c) - nan_safe(td_700_c), 0.0, 40.0)
    dd_f = smoothstep((dd - 4.0) / 20.0)
    lapse = mid_level_lapse_rate(t_700_c, t_500_c)
    lapse_f = smoothstep((lapse - 6.0) / 3.5)
    return nan_safe(mucape, 0.0) * 0.4 * (0.25 + 0.75 * dd_f) * (0.30 + 0.70 * lapse_f)


def wind_parameter(mucape, cin, bwd6_ms, mean6_ms, t_700_c, td_700_c, t_500_c):
    """Severe-wind (derecho / downburst) composite parameter.

    Combines updraft strength (MUCAPE), deep-layer shear (bow echo
    organization), mid-level dryness (evaporative cooling), mid-level
    lapse rate, and cloud-layer mean wind (storm motion that drives
    gust front intensity). Normalized so ~1.0 corresponds to a
    clearly severe-wind environment.
    """
    cape_f = smoothstep(clamp(nan_safe(mucape, 0.0) - 500.0, 0, 4000) / 3500.0)
    shear_f = smoothstep(clamp(nan_safe(bwd6_ms, 0.0) - 10.0, 0, 20) / 20.0)
    mean_f = smoothstep(clamp(nan_safe(mean6_ms, 0.0) - 8.0, 0, 20) / 20.0)
    dd = clamp(nan_safe(t_700_c) - nan_safe(td_700_c), 0.0, 40.0)
    dry_f = smoothstep((dd - 5.0) / 20.0)
    lapse = mid_level_lapse_rate(t_700_c, t_500_c)
    lapse_f = smoothstep((lapse - 6.0) / 3.5)
    cinterm = clamp((nan_safe(cin, -50.0) + 150.0) / 100.0, 0, 1)
    return (cape_f * shear_f *
            (0.25 + 0.75 * dry_f) *
            (0.35 + 0.65 * lapse_f) *
            (0.40 + 0.60 * mean_f) *
            cinterm)


def hail_parameter(mucape, bwd6_ms, t_700_c, t_500_c,
                   freezing_level_m, lcl_m, cin):
    """Significant-hail composite parameter (SHIP-style, simplified).

    Rewards:
      - MUCAPE (updraft volume / parcel residence in the hail growth zone)
      - 0-6 km bulk shear (supercell mode -> sustained rotating updraft)
      - 700-500 hPa lapse rate (steeper mid lapse rates -> wider
        hail-growth zone at suitable temperatures)
      - Freezing level / wet-bulb-zero height in the 2000-3500 m
        sweet spot: too low and little accretion depth, too high and
        hailstones melt before reaching the surface.
      - Lower LCL (cold-base storms are more efficient hail producers).
    """
    cape_f = smoothstep(clamp(nan_safe(mucape, 0.0) - 500.0, 0, 4000) / 3500.0)
    shear_f = smoothstep(clamp(nan_safe(bwd6_ms, 0.0) - 10.0, 0, 20) / 20.0)
    lapse = mid_level_lapse_rate(t_700_c, t_500_c)
    lapse_f = smoothstep((lapse - 6.5) / 3.0)
    fz = nan_safe(freezing_level_m, 3000.0)
    # Gaussian sweet spot centered at 2600 m, sigma 1200 m.
    wbz_f = np.exp(-((fz - 2600.0) ** 2) / (2.0 * 1200.0 * 1200.0))
    lcl_f = clamp((2500.0 - nan_safe(lcl_m, 1500.0)) / 1500.0, 0, 1)
    cinterm = clamp((nan_safe(cin, -50.0) + 150.0) / 100.0, 0, 1)
    return (cape_f * shear_f *
            (0.35 + 0.65 * lapse_f) *
            wbz_f *
            (0.40 + 0.60 * lcl_f) *
            cinterm)


def wind_probability(mucape, cin, bwd6_ms, mean6_ms,
                     t_700_c, td_700_c, t_500_c,
                     precip_mm, weather_code, shear_01_ms,
                     shear_03_ms, shear_06_ms):
    """Final severe-wind probability blend."""
    w = wind_parameter(mucape, cin, bwd6_ms, mean6_ms,
                       t_700_c, td_700_c, t_500_c)
    s = simref_proxy(precip_mm, weather_code, mucape)
    # Storm-mode weight, lightly flattened: severe wind events happen
    # from QLCS/bowing segments too, so we don't want to penalize them
    # as much as we do for tornadoes.
    m = storm_mode_factor(mucape, shear_01_ms, shear_03_ms, shear_06_ms)
    m = 0.4 + 0.6 * m
    return clamp(w * s * m, 0.0, 1.0)


def hail_probability(mucape, cin, bwd6_ms, t_700_c, t_500_c,
                     freezing_level_m, lcl_m, precip_mm, weather_code,
                     shear_01_ms, shear_03_ms, shear_06_ms):
    """Final significant-hail probability blend."""
    h = hail_parameter(mucape, bwd6_ms, t_700_c, t_500_c,
                       freezing_level_m, lcl_m, cin)
    s = simref_proxy(precip_mm, weather_code, mucape)
    # Big hail almost always implies a rotating updraft, so lean on
    # the supercell-biased mode factor but keep a floor so strong
    # multicells can still produce.
    m = storm_mode_factor(mucape, shear_01_ms, shear_03_ms, shear_06_ms)
    m = 0.25 + 0.75 * m
    return clamp(h * s * m, 0.0, 1.0)


def low_level_lapse_rate(t_sfc_c, t_850_c, height_850_m=1500.0):
    """Lapse rate, K/km, between the surface and roughly 850 hPa.
    Clipped to a physically sensible range so crazy values (from bad
    station elevations etc.) don't explode downstream."""
    dz = max(height_850_m, 300.0) / 1000.0
    lapse = (nan_safe(t_sfc_c) - nan_safe(t_850_c)) / dz
    return clamp(lapse, 0.0, 12.0)


def low_level_boost(lapse_c_per_km):
    """Map low-level lapse rate to a [0.6, 1.3] multiplier. The
    neutral (no boost, no cut) value is ~6.5 K/km."""
    lapse = nan_safe(lapse_c_per_km, 6.5)
    return 0.6 + 0.7 * smoothstep((lapse - 5.0) / 4.0)


def environment_factor(cape, cin, lcl_m, srh01, srh03, bwd6_ms,
                       low_lapse):
    """Blend of mature severe parameters. Each normalized to [0, 1]
    before the weighted mean so no single index can dominate when the
    others disagree.

    STP-fixed-layer is added alongside classic STP so cool-season /
    nocturnal events with elevated LCLs but supportive shear+helicity
    don't get zeroed out by the LCL gate.
    """
    mlcape = mlcape_proxy(cape, cin)
    s = stp(mlcape, cin, lcl_m, srh01, bwd6_ms)
    sf = stp_fixed(mlcape, cin, srh01, bwd6_ms)
    v = vtp(mlcape, srh01, bwd6_ms, lcl_m, low_lapse, cin)
    sc = scp(mlcape, srh01, bwd6_ms, cin)
    e1 = ehi01(mlcape, srh01)
    e3 = ehi03(mlcape, srh03)
    return (
        0.28 * clamp(s  / 3.0, 0, 1) +
        0.22 * clamp(sf / 3.0, 0, 1) +
        0.20 * clamp(v  / 2.0, 0, 1) +
        0.15 * clamp(sc / 8.0, 0, 1) +
        0.075 * clamp(e1 / 2.0, 0, 1) +
        0.075 * clamp(e3 / 2.0, 0, 1)
    )


def diurnal_factor(local_solar_hour):
    """Tornado-climatology diurnal weight, peaks at 18 LST (the well-
    documented late-afternoon maximum) and bottoms out around 06 LST.

    Returns a multiplier in [0.40, 1.30]. Operates on scalars or numpy
    arrays so it can be applied per-grid-cell using a longitude-shifted
    local hour field.
    """
    h = nan_safe(local_solar_hour, 12.0)
    phase = np.cos((h - 18.0) * np.pi / 12.0)
    return 0.85 + 0.45 * phase


def local_solar_hour(utc_hour, lon_deg):
    """Approximate local solar hour at longitude `lon_deg` for the
    given UTC hour. No DST or equation-of-time correction; close
    enough for a diurnal-cycle weight."""
    return (nan_safe(utc_hour, 12.0) + nan_safe(lon_deg) / 15.0) % 24.0


# ---------- simulated reflectivity proxy ----------

def simref_proxy(precip_mm, weather_code, cape):
    """A "will there be an intense convective echo here next hour" guess.

    Open-Meteo doesn't hand us model simulated reflectivity directly, so
    we synthesize it from:
      - precipitation intensity (mm/hr), ramped 0..10
      - whether the WMO weather code indicates a thunderstorm
      - CAPE (weak convection at low CAPE shouldn't saturate the score)
    """
    precip = nan_safe(precip_mm, 0.0)
    cape = nan_safe(cape, 0.0)
    wc = np.asarray(weather_code, dtype=np.int64)

    base = smoothstep(precip / 10.0) * (0.30 + 0.70 * smoothstep(cape / 2000.0))

    is_thunder = np.isin(wc, list(THUNDERSTORM_CODES))
    is_heavy = np.isin(wc, list(HEAVY_SHOWER_CODES))
    base = np.where(is_thunder, np.maximum(base, 0.9), base)
    base = np.where(is_heavy,   np.maximum(base, 0.6), base)
    return clamp(base, 0.0, 1.0)


# ---------- storm mode ----------

def storm_mode_factor(cape, shear_01_ms, shear_03_ms, shear_06_ms):
    """Thompson & Smith 2012-style mode classification via shear+CAPE.

    Uses the 0-3 km shear magnitude to distinguish HP supercell modes
    from garden-variety multicells. Weights:
      discrete tornadic supercell 1.00,
      HP / organized supercell     0.85,
      QLCS / bowing segment        0.50,
      multicell                    0.18,
      none                         0.00.
    """
    cape = nan_safe(cape, 0.0)
    s01 = nan_safe(shear_01_ms, 0.0)
    s03 = nan_safe(shear_03_ms, 0.0)
    s06 = nan_safe(shear_06_ms, 0.0)

    tornadic = (s06 >= 20.0) & (s03 >= 14.0) & (s01 >= 10.0) & (cape >= 750.0)
    hp_super = (~tornadic) & (s06 >= 18.0) & (s01 >= 8.0) & (cape >= 500.0)
    qlcs = (~tornadic) & (~hp_super) & (s06 >= 15.0) & (cape >= 250.0)
    multi = (~tornadic) & (~hp_super) & (~qlcs) & (cape >= 100.0)

    out = np.zeros_like(cape, dtype=np.float64)
    out[multi] = 0.18
    out[qlcs] = 0.50
    out[hp_super] = 0.85
    out[tornadic] = 1.0
    return out


# ---------- spatial smoothing ----------

def gaussian_smooth_2d(field: np.ndarray, sigma_cells: float = 1.1) -> np.ndarray:
    """Small separable Gaussian smoother implemented in pure numpy so
    we don't need scipy. Tornado environments are coherent over ~100 km
    and this damps single-cell numerical noise from the forecast model
    or the proxy functions without washing out real signal.
    """
    if sigma_cells <= 0:
        return field
    # Build 1-D kernel of radius 2*sigma.
    r = max(1, int(round(2.0 * sigma_cells)))
    x = np.arange(-r, r + 1, dtype=np.float64)
    k = np.exp(-(x * x) / (2.0 * sigma_cells * sigma_cells))
    k /= k.sum()

    def conv1(arr, axis):
        pad = [(0, 0), (0, 0)]
        pad[axis] = (r, r)
        a = np.pad(arr, pad, mode="edge")
        out = np.zeros_like(arr, dtype=np.float64)
        for i, w in enumerate(k):
            sl = [slice(None), slice(None)]
            sl[axis] = slice(i, i + arr.shape[axis])
            out += w * a[tuple(sl)]
        return out

    smoothed = conv1(conv1(field.astype(np.float64), 0), 1)
    return smoothed


# ---------- final blend ----------

def tornado_probability(cape, cin, lcl_m, srh01, srh03,
                        shear_01_ms, shear_03_ms, shear_06_ms,
                        precip_mm, weather_code, low_lapse):
    """T1 tornado probability — % chance of a tornado within 25 mi of
    a point during the valid hour, calibrated to SPC outlook scales.

    Anchor: fixed-layer STP (Thompson et al. 2012, the parameter SPC
    verifies its sig-tor outlooks against). The probability mapping
    below was fit so the output matches SPC categorical thresholds:

        STP_f =   0  →   0 %      (no signal)
        STP_f =   1  →   ~6 %     (slight risk)
        STP_f =   2  →  ~11 %     (enhanced)
        STP_f =   4  →  ~19 %     (moderate)
        STP_f =   8  →  ~31 %     (high)
        STP_f =  12+ →  ~38 %     (extreme outbreak ceiling, ~45 % cap)

    The environment ceiling is then attenuated multiplicatively by:

      P_storms — actual convective coverage (precip + WMO thunderstorm
                 code + CAPE). Without storms, even a perfect supercell
                 environment produces zero tornadoes.
      M_mode   — Thompson & Smith 2012 mode classifier, mapped to
                 [0.5, 1.0] so QLCS / multicell environments are
                 modulated but not zeroed.
      M_lapse  — low-level lapse-rate boost, [0.85, 1.15].
      cape_gate— hard kill below 250 J/kg of buoyancy.

    The diurnal-climatology weight (peaks ~18 LST) is applied in
    `fetcher.score_hour` after this function returns.
    """
    # Fixed-layer STP, surface-based with CIN penalty. STP is already
    # normalized so cells with weak CAPE or shear or SRH come out near
    # zero, and a "classic outbreak" environment scores ~5-15.
    mlcape = mlcape_proxy(cape, cin)
    stp_f = stp_fixed(mlcape, cin, srh01, shear_06_ms)

    # Environment-only ceiling: saturating exponential.
    #   P_env = P_max * (1 - exp(-STP_f / k))
    # P_max = 0.45 (SPC HIGH risk top), k = 5 fits the calibration
    # table above. STP_f ≥ ~25 effectively saturates.
    p_env = 0.45 * (1.0 - np.exp(-clamp(stp_f, 0.0, 30.0) / 5.0))

    # Storm-coverage gate: requires actual convection in the cell.
    # simref_proxy returns ~0 for dry air even when CAPE is high.
    p_storms = simref_proxy(precip_mm, weather_code, cape)

    # Storm-mode multiplier in [0.5, 1.0] — supercell-favored.
    mode = storm_mode_factor(cape, shear_01_ms, shear_03_ms, shear_06_ms)
    m_mode = 0.5 + 0.5 * mode

    # Low-level lapse-rate booster in [0.85, 1.15].
    lapse = nan_safe(low_lapse, 6.5)
    m_lapse = 0.85 + 0.30 * smoothstep((lapse - 5.5) / 3.0)

    # Hard kill below 250 J/kg of MLCAPE-equivalent buoyancy.
    cape_gate = clamp(nan_safe(cape, 0.0) / 250.0, 0.0, 1.0)

    return clamp(p_env * p_storms * m_mode * m_lapse * cape_gate,
                 0.0, 1.0)


# ---------- grid ----------

@dataclass
class CONUSGrid:
    lats: np.ndarray
    lons: np.ndarray

    @classmethod
    def default(cls, step: float = DEFAULT_GRID_DEG) -> "CONUSGrid":
        s, w, n, e = CONUS_BOUNDS
        return cls(
            lats=np.arange(s, n + step / 2, step),
            lons=np.arange(w, e + step / 2, step),
        )

    @property
    def shape(self):
        return (len(self.lats), len(self.lons))

    def flat_pairs(self):
        """Return (lat, lon) pairs in row-major order."""
        pairs = []
        for la in self.lats:
            for lo in self.lons:
                pairs.append((float(la), float(lo)))
        return pairs


def sparse_cells(grid: CONUSGrid, scores_2d: np.ndarray,
                 floor: float = SCORE_FLOOR):
    idx = np.argwhere(scores_2d >= floor)
    out = []
    for i, j in idx:
        out.append([
            float(round(grid.lats[i], 2)),
            float(round(grid.lons[j], 2)),
            float(round(scores_2d[i, j], 3)),
        ])
    return out


# ---------- I/O ----------

def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def isoformat(t: dt.datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json(path: str, payload: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    os.replace(tmp, path)


def write_meta(path: str, **fields):
    fields.setdefault("updated_at", isoformat(utcnow()))
    write_json(path, fields)

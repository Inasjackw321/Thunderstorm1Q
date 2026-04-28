"""Thunderstorm1Q — T1 Day-1 (hours 1–24) tornado-probability updater.

Runs on a GitHub Actions cron every 12 hours (00 / 12 UTC). Pulls
CAPE, CIN, surface and 850 hPa temperature/dewpoint, and wind at
10 m / 850 hPa / 700 hPa / 500 hPa from the Open-Meteo forecast API
for a ~1.25 degree CONUS grid, computes the multi-parameter
environment x simref-proxy x storm-mode x lapse-rate blend defined
in `common.py`, lightly smooths the result, and writes
`data/day1.json` + `data/day1.meta.json`.

Open-Meteo is used without an explicit `models=` parameter so the
service picks the best-available member per point (HRRR / RAP where
they're in range, GFS elsewhere) — the same approach that reliably
produced hourly data earlier in this project's history. Explicitly
enumerating all four NCEP models in a single multi-model call started
400-ing in April 2026, so we deliberately don't do that here.

The product is labeled "T1" in the output JSON — a house-branded
ensemble name. The underlying provider is Open-Meteo.

Usage:
  python scripts/update_day1.py                  # normal
  python scripts/update_day1.py --seed           # write a blank
                                                 # "awaiting first run"
                                                 # payload
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
import traceback

import numpy as np

import common
import fetcher
from common import CONUSGrid, sparse_cells


MODEL_LABEL = "T1"
FORECAST_HOURS_AHEAD = 30    # fetch 30 hours so we always cover the
                             # next 24 regardless of the current minute.


def run(out_path: str, meta_path: str):
    grid = CONUSGrid.default()
    now = common.utcnow()

    cfg = fetcher.FetchConfig(
        forecast_hours=FORECAST_HOURS_AHEAD,
        past_hours=1,
        models=None,             # auto-pick HRRR/RAP/GFS per point
    )
    results, failures, n_batches = fetcher.fetch_grid(grid, cfg)

    sample = next((r for r in results if r), None)
    first_valid = (now.replace(minute=0, second=0, microsecond=0)
                   + dt.timedelta(hours=1))
    hourly_idx = fetcher.find_hour_indices(sample["hourly"], first_valid,
                                           common.FORECAST_HOURS)

    hours_payload = []
    peak_fh = {h: 0 for h in fetcher.HAZARDS}
    peak_score = {h: 0.0 for h in fetcher.HAZARDS}

    for fh in range(1, common.FORECAST_HOURS + 1):
        i_src = hourly_idx[fh - 1]
        valid_dt = first_valid + dt.timedelta(hours=fh - 1)
        if i_src is None:
            empty = {hz: {"cells": [], "max": 0.0} for hz in fetcher.HAZARDS}
            hours_payload.append({
                "fh": fh,
                "valid": common.isoformat(valid_dt),
                **empty,
            })
            continue

        fields = fetcher.score_hour(grid, results, i_src, valid_dt=valid_dt)
        frame = {"fh": fh, "valid": common.isoformat(valid_dt)}
        log_bits = []
        for hazard in fetcher.HAZARDS:
            arr = fields[hazard]
            cells = sparse_cells(grid, arr)
            mx = float(np.nanmax(arr)) if arr.size else 0.0
            if mx > peak_score[hazard]:
                peak_fh[hazard], peak_score[hazard] = fh, mx
            frame[hazard] = {"cells": cells, "max": round(mx, 3)}
            log_bits.append(f"{hazard[0].upper()}={mx:.2f}")
        hours_payload.append(frame)
        print(f"[fh={fh:02d}] " + " ".join(log_bits))

    peaks = {hazard: {"fh": peak_fh[hazard],
                      "score": round(peak_score[hazard], 3)}
             for hazard in fetcher.HAZARDS}

    payload = {
        "source": f"Thunderstorm1Q — {MODEL_LABEL}",
        "model": MODEL_LABEL,
        "grid_deg": common.DEFAULT_GRID_DEG,
        "score_floor": common.SCORE_FLOOR,
        "generated_at": common.isoformat(now),
        "forecast_hours": common.FORECAST_HOURS,
        "peaks": peaks,
        "hours": hours_payload,
    }
    common.write_json(out_path, payload)

    next_update = now + dt.timedelta(hours=12)
    common.write_meta(
        meta_path,
        source=payload["source"],
        model=MODEL_LABEL,
        generated_at=payload["generated_at"],
        peaks=peaks,
        next_update=common.isoformat(next_update),
        batch_failures=failures,
        batch_total=n_batches,
        seeded=False,
    )
    print(f"OK: wrote {out_path}  (peak fh{peak_fh}/{peak_score:.2f})")


def seed(out_path: str, meta_path: str):
    now = common.utcnow()
    hours = [{
        "fh": fh,
        "valid": common.isoformat(now + dt.timedelta(hours=fh)),
        **{hz: {"cells": [], "max": 0.0} for hz in fetcher.HAZARDS},
    } for fh in range(1, common.FORECAST_HOURS + 1)]
    peaks = {hz: {"fh": 0, "score": 0.0} for hz in fetcher.HAZARDS}
    payload = {
        "source": "Thunderstorm1Q — seed (awaiting first Actions run)",
        "model": MODEL_LABEL,
        "grid_deg": common.DEFAULT_GRID_DEG,
        "score_floor": common.SCORE_FLOOR,
        "generated_at": common.isoformat(now),
        "forecast_hours": common.FORECAST_HOURS,
        "peaks": peaks,
        "hours": hours,
    }
    common.write_json(out_path, payload)
    common.write_meta(
        meta_path,
        source=payload["source"],
        model=MODEL_LABEL,
        generated_at=payload["generated_at"],
        peaks=peaks,
        next_update=common.isoformat(now + dt.timedelta(minutes=60)),
        seeded=True,
    )
    print(f"Seeded {out_path} (empty placeholder).")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out",  default="data/day1.json")
    ap.add_argument("--meta", default="data/day1.meta.json")
    ap.add_argument("--seed", action="store_true",
                    help="Write an empty placeholder payload.")
    args = ap.parse_args()
    try:
        if args.seed:
            seed(args.out, args.meta)
        else:
            run(args.out, args.meta)
    except Exception:
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

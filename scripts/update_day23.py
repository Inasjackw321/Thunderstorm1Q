"""Thunderstorm1Q — T1 Day-2 and Day-3 (hours 25–72) updater.

One Open-Meteo GFS pull covers the entire 72-hour horizon. We fetch
once with `models=gfs_seamless`, then split the result into two day
payloads:

  * Day 2 — hours 25..48 → 4 × 6h-max windows, ×0.85 skill attenuation
  * Day 3 — hours 49..72 → 4 × 6h-max windows, ×0.70 skill attenuation

Combining the two days into a single grid pull halves the API request
count vs running them as separate scripts and is the main reliever
on Open-Meteo's per-minute free-tier throttle. Each frame's field is
the per-cell MAX across the six contained hours — hazard products
should highlight peaks, not means — multiplied by an attenuation
factor so the colormap doesn't imply more confidence than GFS can
honestly deliver at long lead times.

Writes `data/day2.json` + `data/day2.meta.json` and
`data/day3.json` + `data/day3.meta.json`.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
import traceback

import common
import fetcher
from common import CONUSGrid


MODEL_LABEL = "T1"
FORECAST_FRAMES = 4
FORECAST_HOURS_AHEAD = 78    # hours 1..78 — covers Day 2 + Day 3 with margin
ATTENUATION_DAY2 = 0.85
ATTENUATION_DAY3 = 0.70


def _write_day(out_path, meta_path, now, hours_payload, peaks, attenuation):
    source = (f"Thunderstorm1Q — {MODEL_LABEL} "
              f"(GFS · 6h max windows · ×{attenuation:.2f} skill)")
    payload = {
        "source": source,
        "model": MODEL_LABEL,
        "grid_deg": common.DEFAULT_GRID_DEG,
        "score_floor": common.SCORE_FLOOR,
        "generated_at": common.isoformat(now),
        "forecast_hours": FORECAST_FRAMES,
        "peaks": peaks,
        "hours": hours_payload,
    }
    common.write_json(out_path, payload)
    common.write_meta(
        meta_path,
        source=source,
        model=MODEL_LABEL,
        generated_at=payload["generated_at"],
        peaks=peaks,
        next_update=common.isoformat(now + dt.timedelta(hours=12)),
        seeded=False,
    )
    print(f"OK: wrote {out_path}  (peak win{peaks['tornado']['fh']}"
          f"/{peaks['tornado']['score']:.2f})")


def run(day2_out, day2_meta, day3_out, day3_meta):
    grid = CONUSGrid.default()
    now = common.utcnow()

    cfg = fetcher.FetchConfig(
        forecast_hours=FORECAST_HOURS_AHEAD,
        past_hours=1,
        models="gfs_seamless",
    )
    results, _failures, _n = fetcher.fetch_grid(grid, cfg)

    first_valid = (now.replace(minute=0, second=0, microsecond=0)
                   + dt.timedelta(hours=1))

    print("--- Day 2 (hours 25-48) ---")
    d2_hours, d2_peaks = fetcher.compute_gfs_day(
        grid, results, first_valid,
        day_offset_hours=24,
        attenuation=ATTENUATION_DAY2,
    )
    _write_day(day2_out, day2_meta, now, d2_hours, d2_peaks, ATTENUATION_DAY2)

    print("--- Day 3 (hours 49-72) ---")
    d3_hours, d3_peaks = fetcher.compute_gfs_day(
        grid, results, first_valid,
        day_offset_hours=48,
        attenuation=ATTENUATION_DAY3,
    )
    _write_day(day3_out, day3_meta, now, d3_hours, d3_peaks, ATTENUATION_DAY3)


def seed_one(out_path, meta_path, day_offset_hours):
    now = common.utcnow()
    day_start = (now.replace(minute=0, second=0, microsecond=0)
                 + dt.timedelta(hours=1 + day_offset_hours))
    hours = [{
        "fh": w + 1,
        "valid": common.isoformat(day_start + dt.timedelta(hours=w * 6 + 3)),
        **{hz: {"cells": [], "max": 0.0} for hz in fetcher.HAZARDS},
    } for w in range(FORECAST_FRAMES)]
    peaks = {hz: {"fh": 0, "score": 0.0} for hz in fetcher.HAZARDS}
    source = "Thunderstorm1Q — seed (awaiting first Actions run)"
    payload = {
        "source": source,
        "model": MODEL_LABEL,
        "grid_deg": common.DEFAULT_GRID_DEG,
        "score_floor": common.SCORE_FLOOR,
        "generated_at": common.isoformat(now),
        "forecast_hours": FORECAST_FRAMES,
        "peaks": peaks,
        "hours": hours,
    }
    common.write_json(out_path, payload)
    common.write_meta(
        meta_path,
        source=source,
        model=MODEL_LABEL,
        generated_at=payload["generated_at"],
        peaks=peaks,
        next_update=common.isoformat(now + dt.timedelta(minutes=60)),
        seeded=True,
    )
    print(f"Seeded {out_path} (empty placeholder).")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--day2-out",  default="data/day2.json")
    ap.add_argument("--day2-meta", default="data/day2.meta.json")
    ap.add_argument("--day3-out",  default="data/day3.json")
    ap.add_argument("--day3-meta", default="data/day3.meta.json")
    ap.add_argument("--seed", action="store_true",
                    help="Write empty placeholder payloads for both days.")
    args = ap.parse_args()
    try:
        if args.seed:
            seed_one(args.day2_out, args.day2_meta, day_offset_hours=24)
            seed_one(args.day3_out, args.day3_meta, day_offset_hours=48)
        else:
            run(args.day2_out, args.day2_meta,
                args.day3_out, args.day3_meta)
    except Exception:
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

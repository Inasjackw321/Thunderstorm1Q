"""Thunderstorm1Q — T1 Day-3 (hours 49–72) tornado-probability updater.

Same machinery as update_day2.py — forces `models=gfs_seamless`, builds
4 × 6h-max windows — but offset another 24 hours and with a heavier
skill-attenuation factor, since GFS skill drops meaningfully between
48h and 72h lead.

Writes `data/day3.json` + `data/day3.meta.json`.
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
ATTENUATION = 0.70           # GFS 72-h-lead skill knob
DAY_OFFSET_HOURS = 48        # Day 3 = hours 49..72 from now
FORECAST_HOURS_AHEAD = 78    # need through hour ~72 + a comfort margin
FORECAST_FRAMES = 4


def run(out_path: str, meta_path: str):
    grid = CONUSGrid.default()
    now = common.utcnow()

    cfg = fetcher.FetchConfig(
        forecast_hours=FORECAST_HOURS_AHEAD,
        past_hours=1,
        models="gfs_seamless",
    )
    results, failures, n_batches = fetcher.fetch_grid(grid, cfg)

    first_valid = (now.replace(minute=0, second=0, microsecond=0)
                   + dt.timedelta(hours=1))
    hours_payload, peaks = fetcher.compute_gfs_day(
        grid, results, first_valid,
        day_offset_hours=DAY_OFFSET_HOURS,
        attenuation=ATTENUATION,
    )

    source = (f"Thunderstorm1Q — {MODEL_LABEL} "
              f"(GFS · 6h max windows · ×{ATTENUATION:.2f} skill)")
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

    next_update = now + dt.timedelta(hours=5)
    common.write_meta(
        meta_path,
        source=source,
        model=MODEL_LABEL,
        generated_at=payload["generated_at"],
        peaks=peaks,
        next_update=common.isoformat(next_update),
        batch_failures=failures,
        batch_total=n_batches,
        seeded=False,
    )
    print(f"OK: wrote {out_path}  (peak win{peaks['tornado']['fh']}"
          f"/{peaks['tornado']['score']:.2f})")


def seed(out_path: str, meta_path: str):
    now = common.utcnow()
    day_start = (now.replace(minute=0, second=0, microsecond=0)
                 + dt.timedelta(hours=1 + DAY_OFFSET_HOURS))
    hours = [{
        "fh": w + 1,
        "valid": common.isoformat(day_start + dt.timedelta(hours=w * 6 + 3)),
        "tornado": {"cells": [], "max": 0.0},
    } for w in range(FORECAST_FRAMES)]
    peaks = {"tornado": {"fh": 0, "score": 0.0}}
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
    ap.add_argument("--out",  default="data/day3.json")
    ap.add_argument("--meta", default="data/day3.meta.json")
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

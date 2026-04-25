"""Thunderstorm1Q — T1 Day-2 (hours 25–48) tornado-probability updater.

Runs on the same GitHub Actions cron as update_day1.py, right after it.
Pulls the same CAPE / CIN / T / Td / wind stack from Open-Meteo but
forces `models=gfs_seamless` so hours 25–48 come from the 0.25° GFS
global member (Day 1 uses HRRR via the API's auto-pick; mixing the
two models across the day boundary would put a discontinuity right
at hour 25, so Days 2 and 3 deliberately stay inside the GFS family).

Each day becomes 4 frames at 6-hour windows (midpoints 03 / 09 / 15 /
21 UTC from the day's start). Each frame is the per-cell MAX across
its six contained hours — a hazard product should highlight peaks, not
means — then multiplied by a skill-attenuation factor so the colormap
doesn't imply more confidence than GFS can honestly deliver at this
lead time.

Writes `data/day2.json` + `data/day2.meta.json`.
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
ATTENUATION = 0.85           # GFS 48-h-lead skill knob
DAY_OFFSET_HOURS = 24        # Day 2 = hours 25..48 from now
FORECAST_HOURS_AHEAD = 54    # need through hour ~48 + a comfort margin
FORECAST_FRAMES = 4          # 4 × 6h windows


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
    ap.add_argument("--out",  default="data/day2.json")
    ap.add_argument("--meta", default="data/day2.meta.json")
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

#!/usr/bin/env bash
set -e
export $(grep -v '^#' .env | xargs) || true
python scripts/fetch_evds.py --start 2010-01-01 --end $(date +%F)
python scripts/build_features.py --asof $(date +%F)
python scripts/make_event_windows.py

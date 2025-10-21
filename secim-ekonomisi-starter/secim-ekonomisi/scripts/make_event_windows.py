#!/usr/bin/env python3
import json, pathlib, pandas as pd
from datetime import datetime
EVENTS = [
    "2014-03-30","2014-08-10","2015-06-07","2015-11-01",
    "2017-04-16","2018-06-24","2019-03-31","2019-06-23",
    "2023-05-14","2023-05-28",
    "2028-05-15"  # tahmini referans
]

def main():
    src = pathlib.Path("data/processed/features.parquet")
    if not src.exists():
        raise SystemExit("Önce build_features.py çalıştırın (features.parquet yok).")
    feat = pd.read_parquet(src)
    # Uzun forma dön
    long = feat.melt(id_vars=["date"], var_name="series", value_name="value")
    out_frames = []
    for ev in EVENTS:
        evd = pd.to_datetime(ev)
        tmp = long.copy()
        tmp["event_date"] = evd
        tmp["t"] = (tmp["date"].dt.to_period("M").astype(int) - evd.to_period("M").astype(int))
        tmp = tmp[(tmp["t"]>=-12) & (tmp["t"]<=12)]
        out_frames.append(tmp)
    panel = pd.concat(out_frames, ignore_index=True)
    outp = pathlib.Path("data/processed/event_panel.parquet")
    panel.to_parquet(outp, index=False)
    print(f"Event panel yazıldı: {outp} ({len(panel)} satır)")

if __name__ == "__main__":
    main()

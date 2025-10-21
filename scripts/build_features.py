#!/usr/bin/env python3
import argparse, pathlib, pandas as pd, numpy as np

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    # Basit örnek özellikler: yıllık farklar, log farklar
    df = df.copy()
    # geniş tabloya pivot
    wide = df.pivot_table(index="date", columns="series_code", values="value")
    wide = wide.sort_index()
    # log değişim örneği
    logd = np.log(wide).diff()
    logd.columns = [c + "__logdiff" for c in logd.columns]
    # birleştir
    out = pd.concat([wide, logd], axis=1).reset_index()
    return out

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--asof", default=None, help="YYYY-MM-DD (rapor kesim tarihi)")
    args = parser.parse_args()

    src = pathlib.Path("data/interim/evds_long.csv")
    if not src.exists():
        raise SystemExit("Önce fetch_evds.py çalıştırın (evds_long.csv yok).")

    df = pd.read_csv(src, parse_dates=["date"])
    features = compute_features(df)
    outp = pathlib.Path("data/processed/features.parquet")
    outp.parent.mkdir(parents=True, exist_ok=True)
    features.to_parquet(outp, index=False)
    print(f"Features yazıldı: {outp} ({len(features)} satır)")

if __name__ == "__main__":
    main()

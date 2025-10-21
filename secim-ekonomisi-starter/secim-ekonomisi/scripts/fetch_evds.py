#!/usr/bin/env python3
import os, argparse, json, time, pathlib, sys, csv
from datetime import datetime
import pandas as pd
import requests
import yaml

def load_cfg():
    with open("config/evds.yml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def evds_request(series, start, end, freq, formulas, out_type="json"):
    base = CFG["evds_base"]
    params = {
        "series": series,
        "startDate": datetime.strptime(start,"%Y-%m-%d").strftime("%d-%m-%Y"),
        "endDate": datetime.strptime(end,"%Y-%m-%d").strftime("%d-%m-%Y"),
        "type": out_type,
        "frequency": {"D":"1","W":"2","M":"5","Q":"6","Y":"8"}.get(freq.upper(),"5"),
        "formulas": formulas,
    }
    headers = {"key": os.environ.get("EVDS_API_KEY","")}
    if not headers["key"]:
        raise RuntimeError("EVDS_API_KEY .env içinde tanımlı olmalı.")
    r = requests.get(base, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()

def normalize_items(items: list, series_code: str, freq: str, formulas: str):
    if not items:
        return pd.DataFrame(columns=["date","series_code","value","frequency","formulas"])
    df = pd.DataFrame(items)
    # EVDS genelde tarih kolonu "Tarih" veya "date" olabilir
    date_col = "Tarih" if "Tarih" in df.columns else ( "date" if "date" in df.columns else df.columns[0] )
    # değer kolonu: serinin kodu ya da ilk sayı kolonu
    val_cols = [c for c in df.columns if c != date_col]
    # ilk değer kolonunu al
    val_col = None
    for c in val_cols:
        try:
            pd.to_numeric(df[c].replace(",",".")).astype(float)
            val_col = c
            break
        except Exception:
            continue
    if val_col is None:
        val_col = val_cols[0]
    # parse
    s = pd.Series(df[val_col]).astype(str).str.replace(",",".", regex=False)
    s = pd.to_numeric(s, errors="coerce")
    out = pd.DataFrame({
        "date": pd.to_datetime(df[date_col], dayfirst=True, errors="coerce"),
        "series_code": series_code,
        "value": s,
        "frequency": freq.upper(),
        "formulas": formulas
    }).dropna(subset=["date"])
    return out.sort_values("date")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="config/series_catalog.csv")
    parser.add_argument("--start", default="2010-01-01")
    parser.add_argument("--end", default=datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    cat = pd.read_csv(args.catalog)
    ts = datetime.utcnow().strftime("%Y%m%d")
    raw_dir = pathlib.Path(CFG["io"]["raw_dir"]) / ts
    interim_dir = pathlib.Path(CFG["io"]["interim_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)
    interim_dir.mkdir(parents=True, exist_ok=True)

    frames = []
    for _, row in cat.iterrows():
        series = str(row["series_code"]).strip()
        freq = str(row["frequency"]).strip()
        formulas = str(row["formulas"]).strip()
        start = str(row.get("start_date") or args.start)
        end = str(row.get("end_date") or args.end)

        try:
            js = evds_request(series, start, end, freq, formulas, out_type=CFG["default"]["type"])
            # ham json kaydet
            raw_path = raw_dir / f"{series}_{freq}_{formulas}.json"
            raw_path.write_text(json.dumps(js, ensure_ascii=False, indent=2), encoding="utf-8")
            items = js.get("items", [])
            df = normalize_items(items, series, freq, formulas)
            frames.append(df)
            print(f"OK: {series} {len(df)} satır")
            time.sleep(0.4)  # nazikçe
        except Exception as e:
            print(f"FAIL: {series} -> {e}", file=sys.stderr)

    if frames:
        all_df = pd.concat(frames, ignore_index=True)
        out_csv = interim_dir / "evds_long.csv"
        all_df.to_csv(out_csv, index=False)
        print(f"Interim yazıldı: {out_csv}")
    else:
        print("Uyarı: veri çerçevesi boş.")

if __name__ == "__main__":
    CFG = load_cfg()
    main()

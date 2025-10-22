# scripts/fetch_evds_catalog.py
from dotenv import load_dotenv
import os
from evds import evdsAPI
import pandas as pd
from datetime import datetime

load_dotenv()
KEY = os.getenv("EVDS_API_KEY")
assert KEY, "EVDS_API_KEY yok (.env'i kontrol et)"

evds = evdsAPI(KEY, legacySSL=True)

# 🔢 Şimdilik örnek kısa katalog (istediğin kadar seri ekleyebilirsin)
SERIES = [
    "TP.DK.USD.A.YTL",   # USD (örnek)
    "TP.DK.EUR.A.YTL",   # EUR (örnek)
    "TP.KFE.01",         # Konut Fiyat Endeksi
]

START = "01-01-2019"                      # gg-aa-yyyy
END   = datetime.today().strftime("%d-%m-%Y")

all_rows = []

for code in SERIES:
    try:
        df = evds.get_data(series=[code], startdate=START, enddate=END)
        if df is None or df.empty:
            print(f"FAIL: {code} -> boş döndü")
            continue

        # Tarih kolonunu birleştir (EVDS bazen 'Tarih' verir)
        if "Tarih" in df.columns:
            df = df.rename(columns={"Tarih": "date"})
        elif "date" not in df.columns:
            # son çare: ilk sütunu tarih say
            df = df.rename(columns={df.columns[0]: "date"})

        # Tarihi parse et, NaN’ları at
        df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
        df = df.dropna(subset=["date"])

        # Değer kolonu ismini standardize et
        # (kod kolonunu bul: code ile başlayan sütun olur)
        value_cols = [c for c in df.columns if c != "date"]
        if len(value_cols) == 1:
            df = df.rename(columns={value_cols[0]: "value"})
        df["series"] = code

        # Kaydet: ham (seri bazında) + birleştirilmiş (long)
        out_raw = f"data/raw/{code.replace('.', '_')}.csv"
        df.to_csv(out_raw, index=False, encoding="utf-8")
        all_rows.append(df[["date", "series", "value"]])
        print(f"OK : {code} -> {len(df)} satır, {out_raw}")

    except Exception as e:
        print(f"ERR: {code} -> {e}")

# Tüm serileri tek dosyada uzun format
if all_rows:
    long_df = pd.concat(all_rows, ignore_index=True).sort_values(["series","date"])
    long_df.to_csv("data/interim/evds_long.csv", index=False, encoding="utf-8")
    print("✅ Birleştirildi: data/interim/evds_long.csv")
else:
    print("⚠️ Hiç veri toplanamadı.")

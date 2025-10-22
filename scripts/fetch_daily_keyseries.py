# scripts/fetch_daily_keyseries.py
from dotenv import load_dotenv
import os
from evds import evdsAPI
import pandas as pd
from datetime import datetime
import math

# --- Ayarlar ---
START = "01-01-2013"                                 # gg-aa-yyyy
END   = datetime.today().strftime("%d-%m-%Y")        # bugün
RAW_DIR = "data/raw"
INTERIM_DIR = "data/interim"

# Keşif notu:
# Aşağıda her metrik için BİRDEN FAZLA aday seri kodu var.
# Paket ilk dolu döneni kullanır. (EVDS arayüzünden güncelleyebilirsin.)
CANDIDATES = {
    "USD_TL":   ["TP.DK.USD.A", "TP.DK.USD.SPK", "TP.DK.USD.A.YTL"],
    "EUR_TL":   ["TP.DK.EUR.A", "TP.DK.EUR.SPK", "TP.DK.EUR.A.YTL"],
    "XAU_USD":  ["TP.DK.AUM", "TP.DK.XAU.ONS", "TP.DK.UXAU.ONS"],   # Ons altın USD
    # Gram altın TL için önce doğrudan seri denenir; yoksa USDTRY ve ons USD'den türetilecek
    "GRAM_TL":  ["TP.DK.AU.GRAM.TL", "TP.DK.AULTL", "TP.DK.GRAMALTIN.TL"],
    # Aşağıdakiler aylık ama yine günlük aralığa çağırıyoruz; EVDS aylık döner.
    "TÜFE":     ["TP.TUFE", "TP.TUFE.ANA", "TP.TUFE.ENF"],
    "KFE":      ["TP.KFE.01", "TP.KFE"],   # Konut Fiyat Endeksi
    # Politika faizi / gecelik oranlar (günlük)
    "POLICY":   ["TP.REPO", "TP.GRF", "TP.BIST.1W.REPO", "TP.BIST.REPO"]
}

# --- Hazırlık ---
load_dotenv()
KEY = os.getenv("EVDS_API_KEY")
assert KEY, "EVDS_API_KEY bulunamadı (.env'i kontrol et)"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(INTERIM_DIR, exist_ok=True)

# Bazı Windows ortamlarında legacySSL=True daha stabil
ev = evdsAPI(KEY, legacySSL=True)

def fetch_first_available(series_codes, startdate, enddate, freq="1"):
    """Aday kodlardan ilk dolu DataFrame'i döndürür; yoksa None."""
    for code in series_codes:
        try:
            df = ev.get_data(series=[code], startdate=startdate, enddate=enddate, frequency=freq)
            if df is not None and not df.empty:
                # tarih sütunu ve değer kolonunu normalize et
                if "Tarih" in df.columns:
                    df = df.rename(columns={"Tarih": "date"})
                elif "date" not in df.columns:
                    df = df.rename(columns={df.columns[0]: "date"})
                df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
                df = df.dropna(subset=["date"])
                # değer kolonu: tarih dışındaki tek kolonu 'value' yap
                value_cols = [c for c in df.columns if c != "date"]
                if len(value_cols) == 1:
                    df = df.rename(columns={value_cols[0]: "value"})
                else:
                    # Birden fazla kolon varsa, ilk sayısal kolonu al
                    num_cols = [c for c in value_cols if pd.api.types.is_numeric_dtype(df[c])]
                    take = num_cols[0] if num_cols else value_cols[0]
                    df = df[["date", take]].rename(columns={take: "value"})
                # sort + eşsizleştir
                df = (df.sort_values("date")
                        .drop_duplicates(subset=["date"], keep="last"))
                return code, df[["date","value"]]
        except Exception:
            continue
    return None, None

def safe_append_dedupe_csv(path, new_df, subset_cols):
    """Var olan CSV'ye birleştir, subset'e göre tekrarı at ve yaz."""
    if os.path.exists(path):
        old = pd.read_csv(path, parse_dates=["date"])
        merged = pd.concat([old, new_df], ignore_index=True)
    else:
        merged = new_df.copy()
    merged = (merged
              .sort_values(subset_cols)
              .drop_duplicates(subset=subset_cols, keep="last"))
    merged.to_csv(path, index=False, encoding="utf-8")
    return merged

# --- Çekim ---
results = {}   # metric -> (used_code, df)

# 1) USD, EUR, Ons USD
for metric in ["USD_TL", "EUR_TL", "XAU_USD"]:
    used, df = fetch_first_available(CANDIDATES[metric], START, END, freq="1")
    if df is None:
        print(f"FAIL: {metric} -> aday seriler boş döndü")
    else:
        results[metric] = (used, df)
        out = os.path.join(RAW_DIR, f"{metric}.csv")
        df2 = df.copy()
        df2["series"] = used
        safe_append_dedupe_csv(out, df2[["date","series","value"]], ["date"])

        print(f"OK : {metric} <- {used} ({len(df)} satır) -> {out}")

# 2) Gram Altın TL (doğrudan seri varsa onu kullan, yoksa hesapla)
used, df = fetch_first_available(CANDIDATES["GRAM_TL"], START, END, freq="1")
if df is None:
    # Hesap: GramTL ≈ (OnsUSD / 31.1035) * USDTRY
    if "USD_TL" in results and "XAU_USD" in results:
        usdtry = results["USD_TL"][1].rename(columns={"value":"usdtry"})
        xauusd = results["XAU_USD"][1].rename(columns={"value":"xauusd"})
        gram = pd.merge_asof(
            xauusd.sort_values("date"),
            usdtry.sort_values("date"),
            on="date", direction="nearest", tolerance=pd.Timedelta("1D")
        )
        gram["value"] = (gram["xauusd"] / 31.1035) * gram["usdtry"]
        gram = gram[["date","value"]].dropna()
        used = "DERIVED:GRAM_TL=(XAUUSD/31.1035)*USDTRY"
        df = gram.sort_values("date").drop_duplicates(subset=["date"], keep="last")
        print("OK : GRAM_TL türetildi (XAUUSD & USDTRY ile)")
    else:
        print("FAIL: GRAM_TL -> ne doğrudan seri var ne de türetecek veriler")
        df = None

if df is not None:
    out = os.path.join(RAW_DIR, "GRAM_TL.csv")
    df2 = df.copy(); df2["series"] = "GRAM_TL"
    safe_append_dedupe_csv(out, df2[["date","series","value"]], ["date"])
    results["GRAM_TL"] = ("GRAM_TL", df)

# 3) TÜFE (aylık) ve KFE (aylık) ve Politika (günlük)
for metric, freq in [("TÜFE","8"), ("KFE","8"), ("POLICY","1")]:
    used, df = fetch_first_available(CANDIDATES[metric], START, END, freq=freq)
    if df is None:
        print(f"FAIL: {metric} -> aday seriler boş")
        continue
    out = os.path.join(RAW_DIR, f"{metric}.csv")
    df2 = df.copy(); df2["series"] = used
    safe_append_dedupe_csv(out, df2[["date","series","value"]], ["date"])
    results[metric] = (used, df)
    print(f"OK : {metric} <- {used} ({len(df)} satır) -> {out}")

# 4) Birleştirilmiş 'long' dosya
long_parts = []
for metric, (used_code, dfx) in results.items():
    tmp = dfx.copy()
    tmp["metric"] = metric
    tmp["series"] = used_code
    long_parts.append(tmp[["date","metric","series","value"]])

if long_parts:
    long_df = pd.concat(long_parts, ignore_index=True)
    out_long = os.path.join(INTERIM_DIR, "evds_daily_long.csv")
    # merge + dedupe (metric,date)
    if os.path.exists(out_long):
        old = pd.read_csv(out_long, parse_dates=["date"])
        long_df = pd.concat([old, long_df], ignore_index=True)
    long_df = (long_df
               .sort_values(["metric","date"])
               .drop_duplicates(subset=["metric","date"], keep="last"))
    long_df.to_csv(out_long, index=False, encoding="utf-8")
    print(f"✅ Birleştirildi: {out_long} ({len(long_df)} satır)")
else:
    print("⚠️ Hiç veri toplanamadı.")

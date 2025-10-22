# scripts/discover_evds_codes.py
from dotenv import load_dotenv
import os
from evds import evdsAPI
import pandas as pd

load_dotenv()
KEY = os.getenv("EVDS_API_KEY")
assert KEY, "EVDS_API_KEY yok (.env'i kontrol et)"
ev = evdsAPI(KEY, legacySSL=True)

os.makedirs("data/interim", exist_ok=True)

# İncelemek istediğimiz alt gruplar (senin çıktından):
SUBCODES = {
    "doviz": "bie_dkdovytl",              # Döviz kurları (USD/EUR burada çalışıyor)
    "fiyat_tufe": "bie_tukfiy4",          # TÜFE
    "fiyat_tufe_cekirdek": "bie_feoktg",  # Özel kaps. TÜFE
    "kfe": "bie_konut_fiyat_endeksi",     # KFE
    "faiz": "bie_faiz_oranlari",          # Politika/faiz
    "yurtdisi": "bie_yurtdisi_piyasalar", # Ons altın çoğu hesapta burada
}

def normalize_rows(rows):
    """list[dict] ya da DataFrame -> (df, code_col, name_col, start_col)"""
    if rows is None:
        return None, None, None, None
    if isinstance(rows, list):
        if not rows:
            return pd.DataFrame(), None, None, None
        df = pd.DataFrame(rows)
    elif isinstance(rows, pd.DataFrame):
        df = rows.copy()
    else:
        # bilinmeyen tip -> düz string’e bas
        return pd.DataFrame([{"RAW": str(rows)}]), None, None, None

    # kolonları büyük harfe çevirilmiş kopya ile eşleştir
    upmap = {c: str(c).upper() for c in df.columns}
    inv = {v: k for k, v in upmap.items()}

    # Olası isimler
    code_keys = ["SERIE_CODE","SERIES_CODE","SERIECODE","SER_CODE","CODE","SERIEID","SERIESID"]
    name_keys = ["SERIE_NAME","SERIES_NAME","NAME","DESCRIPTION_TR","DESCRIPTION","TOPIC_TITLE_TR","TITLE_TR"]
    start_keys = ["START_DATE","STARTDATE","START","BASLANGIC_TARIHI"]

    code_col = next((inv[k] for k in code_keys if k in inv), None)
    name_col = next((inv[k] for k in name_keys if k in inv), None)
    start_col = next((inv[k] for k in start_keys if k in inv), None)

    # Tam yakalayamazsak, en makul ilk 5 kolonu bırak
    if code_col is None:
        # 'CODE' geçen ilk kolon
        for c in df.columns:
            if "code" in str(c).lower():
                code_col = c
                break
    if name_col is None:
        for c in df.columns:
            if any(k in str(c).lower() for k in ["name","desc","title"]):
                name_col = c
                break

    return df, code_col, name_col, start_col

all_reports = {}
for nick, subcode in SUBCODES.items():
    try:
        rows = ev.get_series(subcode)
    except Exception as e:
        print(f"[{nick}] get_series hata: {e}")
        continue

    df, code_col, name_col, start_col = normalize_rows(rows)
    if df is None or df.empty:
        print(f"[{nick}] boş döndü")
        continue

    # Raporu sadeleştir
    cols = []
    if code_col: cols.append(code_col)
    if name_col and name_col != code_col: cols.append(name_col)
    if start_col and start_col not in cols: cols.append(start_col)
    # yedek: ilk 5 kolon
    if not cols:
        cols = list(df.columns[:5])

    out = df[cols].copy()
    # kullanıcıya kolaylık: kolon adlarını sabitle
    rename = {}
    if code_col: rename[code_col] = "SERIE_CODE"
    if name_col: rename[name_col] = "SERIE_NAME"
    if start_col: rename[start_col] = "START_DATE"
    if rename:
        out = out.rename(columns=rename)

    out_path = f"data/interim/series_{nick}.csv"
    out.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[{nick}] kaydedildi -> {out_path} | {len(out)} satır | kolonlar: {list(out.columns)}")

print("✅ Bitti. 'data/interim/series_*.csv' dosyalarına bak.")

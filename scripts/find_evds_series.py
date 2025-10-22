# scripts/find_evds_series.py
from dotenv import load_dotenv
import os
from evds import evdsAPI
import pandas as pd

load_dotenv()
KEY = os.getenv("EVDS_API_KEY")
assert KEY, "EVDS_API_KEY yok"
evds = evdsAPI(KEY, legacySSL=True)

def show(df, title, take=30):
    print("\n" + "="*8, title, "="*8)
    if isinstance(df, list):
        # evds bazılarını list döndürüyor -> DataFrame'e çevir
        df = pd.DataFrame(df)
    if isinstance(df, pd.DataFrame):
        # Kullanışlı sütunları göster
        cols = [c for c in df.columns if c.lower() in
                {"serie_code","serie_name","topic_title_tr","name","code","start_date"}]
        if not cols: cols = df.columns[:5]
        print(df[cols].head(take).to_string(index=False))
    else:
        print(df)

# 1) Ana kategoriler
show(evds.main_categories, "Ana Kategoriler", 100)

# 2) Alt kategoriler içinde "KURLAR", "FİYAT", "ENDEKS", "FAİZ" gibi başlıkları bul
show(evds.get_sub_categories("KURLAR"), "Alt Kategoriler - KURLAR", 100)
show(evds.get_sub_categories("FİYAT"),  "Alt Kategoriler - FİYAT", 100)
show(evds.get_sub_categories("ENDEKS"), "Alt Kategoriler - ENDEKS", 100)
show(evds.get_sub_categories("FAİZ"),   "Alt Kategoriler - FAİZ", 100)

# 3) Belirli alt kategori kodunda SERİLERİ getir (örnekler):
# Not: get_series parametresi EVDS tarafında topic/subtopic kodudur.
# Aşağıdaki örnekler her hesapta değişebilir; dolu olanları deneyerek ilerle.
for sub in [
    "bie_dkdovizkurlari",     # Döviz Kurları (ör: USD/EUR zaten çalıştı)
    "bie_yurtdisi_piyasalar", # Ons altın burada olabilir
    "bie_konut_fiyat_endeksi",
    "bie_tufe",               # Tüketici Fiyat Endeksi
    "bie_faiz_oranlari"       # Politika/Gecelik repo vb.
]:
    try:
        ser = evds.get_series(sub)
        show(ser, f"Seriler @ {sub}", 50)
    except Exception as e:
        print(f"\n*** {sub} alınamadı: {e}")

# 4) Anahtar kelimeyle kaba filtre (isimde geçenleri listele)
def search_keyword(subcode, keyword):
    try:
        ser = evds.get_series(subcode)
        df = pd.DataFrame(ser)
        mask = df.apply(lambda col: col.astype(str).str.contains(keyword, case=False, na=False)).any(axis=1)
        show(df[mask], f'"{keyword}" @ {subcode}', 50)
    except Exception as e:
        print(f'*** Arama hata: {subcode} / {keyword} -> {e}')

for kw in ["altın", "ons", "xau", "gram", "kfe", "konut", "tufe", "politika", "repo"]:
    for sub in ["bie_yurtdisi_piyasalar","bie_konut_fiyat_endeksi","bie_tufe","bie_faiz_oranlari","bie_dkdovizkurlari"]:
        search_keyword(sub, kw)

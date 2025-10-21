# Seçim Ekonomisi – Otomatik Veri & Analiz Starter

Bu depo, Türkiye için seçim öncesi/sonrası makro ve piyasa göstergelerini **otomatik** çekmek,
işlemek ve tek komutla **rapor** üretmek için bir başlangıç şablonudur.

## Hızlı Başlangıç
```bash
# 1) Ortam
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2) API anahtarı
cp .env.example .env   # .env içine EVDS_API_KEY'i yaz

# 3) Veriyi çek + özellikleri üret
python scripts/fetch_evds.py --start 2010-01-01 --end $(date +%F)
python scripts/build_features.py --asof $(date +%F)
python scripts/make_event_windows.py

# 4) Rapor
quarto render report/secm-ekonomisi.qmd
```

## Dizin Yapısı
```
secim-ekonomisi/
├─ config/
│  ├─ evds.yml                # EVDS taban ayarları
│  └─ series_catalog.csv      # çekilecek seriler (tek kaynak gerçek)
├─ scripts/
│  ├─ fetch_evds.py           # EVDS'ten toplu çekim
│  ├─ fetch_extra.py          # (ops.) HMB/TÜİK/CDS/BIST çekimleri
│  ├─ build_features.py       # reel dönüşümler, kredi/fiskal impulse
│  └─ make_event_windows.py   # seçim olay pencereleri [-12, +12]
├─ data/{raw,interim,processed}/
├─ report/secm-ekonomisi.qmd  # tek komut rapor (Quarto)
└─ .github/workflows/refresh.yml  # GitHub Actions ile otomatik güncelleme
```

## Parametrik Çalıştırma
Tüm script'ler tarih parametreleri alır (örn. `--end 2025-10-22`). Böylece her çalıştırmada **güncel** veri gelir.

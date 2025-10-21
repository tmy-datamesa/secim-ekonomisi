#!/usr/bin/env python3
"""
Ek kaynaklar (HMB bütçe/nakit, TÜİK CSV, CDS, BIST) için şablon.
Gerektiğinde fonksiyonları doldurup data/interim'e yazın.
"""
import argparse, pathlib, pandas as pd

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--end", default=None, help="YYYY-MM-DD")
    args = parser.parse_args()
    pathlib.Path("data/interim").mkdir(parents=True, exist_ok=True)
    # TODO: Uygun kaynaklardan çekim yap ve evds_long ile birleştirmek üzere kaydet.
    print("fetch_extra: yer tutucu (TODO).")

if __name__ == "__main__":
    main()

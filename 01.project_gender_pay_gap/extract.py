"""
Extract: unduh 4 dataset mentah Eurostat ke RAW_DIR tanpa diubah.

Jalankan manual:  python extract.py
Dari Airflow:     from extract import extract
"""
import os
import time
import urllib.request
from pathlib import Path

RAW_DIR = Path(os.getenv("RAW_DIR", "data/raw"))

DATASETS = {
    "wage_gap": (
        "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/"
        "earn_gr_gpgr2/1.0/*.*.*.*?c[freq]=A&c[unit]=PC"
        "&c[nace_r2]=B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R"
        "&c[geo]=BE,BG,CZ,DK,DE,EE,IE,EL,ES,FR,HR,IT,CY,LV,LT,LU,HU,MT,NL,AT,PL,PT,RO,SI,SK,FI,SE,IS,NO,CH,UK,ME,MK,AL,RS,TR"
        "&c[TIME_PERIOD]=2024,2023,2022,2021,2020,2019,2018,2017,2016,2015"
        "&compress=true&format=csvdata&formatVersion=2.0&lang=en&labels=name"
    ),
    "emp_gap": (
        "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/"
        "sdg_05_30/1.0/*.*.*.*.*?c[freq]=A&c[wstatus]=EMP,EMP_PT,EMP_TEMP,UEMP_PT"
        "&c[age]=Y20-64&c[unit]=PC_PNT"
        "&c[geo]=BE,BG,CZ,DK,DE,EE,IE,EL,ES,FR,HR,IT,CY,LV,LT,LU,HU,MT,NL,AT,PL,PT,RO,SI,SK,FI,SE,IS,NO,CH,BA,ME,MK,RS,TR"
        "&c[TIME_PERIOD]=2024,2023,2022,2021,2020,2019,2018,2017,2016,2015"
        "&compress=true&format=csvdata&formatVersion=2.0&lang=en&labels=name"
    ),
    "women_mang": (
        "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/"
        "sdg_05_61/1.0/*.*.*.*.*?c[freq]=A&c[prof_pos]=NEXEC,NEXEC_EXEC&c[sex]=F&c[unit]=PC"
        "&c[geo]=BE,BG,CZ,DK,DE,EE,IE,EL,ES,FR,HR,IT,CY,LV,LT,LU,HU,MT,NL,AT,PL,PT,RO,SI,SK,FI,SE,IS,NO,BA,ME,MK,RS,TR"
        "&c[TIME_PERIOD]=2024,2023,2022,2021,2020,2019,2018,2017,2016,2015"
        "&compress=true&format=csvdata&formatVersion=2.0&lang=en&labels=name"
    ),
    "women_college": (
        "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/"
        "sdg_04_20/1.0/*.*.*.*.*.*?c[freq]=A&c[sex]=F&c[age]=Y25-34&c[unit]=PC&c[isced11]=ED5-8"
        "&c[geo]=BE,BG,CZ,DK,DE,EE,IE,EL,ES,FR,HR,IT,CY,LV,LT,LU,HU,MT,NL,AT,PL,PT,RO,SI,SK,FI,SE,IS,NO,CH,BA,ME,MK,RS,TR,UK"
        "&c[TIME_PERIOD]=2024,2023,2022,2021,2020,2019,2018,2017,2016,2015"
        "&compress=true&format=csvdata&formatVersion=2.0&lang=en&labels=name"
    ),
}


def download(name, url, out_dir, retries=3, timeout=120):
    """Unduh satu dataset ke <out_dir>/<name>.csv.gz, ulangi jika gagal."""
    out_path = out_dir / f"{name}.csv.gz"
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                data = resp.read()
            if not data:
                raise ValueError("respons kosong")
            out_path.write_bytes(data)
            print(f"[extract] {name}: OK ({len(data):,} bytes) -> {out_path}")
            return out_path
        except Exception as exc:
            print(f"[extract] {name}: percobaan {attempt}/{retries} gagal: {exc}")
            if attempt == retries:
                raise
            time.sleep(5 * attempt)


def extract(out_dir=None):
    """Unduh semua dataset. Mengembalikan dict {nama: path file}."""
    out_dir = Path(out_dir) if out_dir else RAW_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    return {name: str(download(name, url, out_dir)) for name, url in DATASETS.items()}


if __name__ == "__main__":
    print(extract())
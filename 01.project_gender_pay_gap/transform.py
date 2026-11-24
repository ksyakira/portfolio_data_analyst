"""
Transform: data/raw/*.csv.gz -> data/clean/*_clean.csv

Meniru logika cleaning di notebook analis (dropDuplicates, trim + initcap nama negara,
pembulatan 2 desimal) dan memakai NAMA KOLOM yang sama dengan schema.sql dari Role 2,
supaya load ke database tinggal memasukkan data.

Keputusan tim:
  - kolom kode (geo, nace_r2, wstatus, prof_pos) dipertahankan untuk primary key
  - kolom nilai dan kolom teks tidak boleh kosong: angka -> 0, teks -> 'Unknown'
  - kolom flag (observation_flag, confidentiality_status) dibiarkan kosong kalau memang
    tidak ada flag (kosong di sini artinya "tidak ada flag", bukan data hilang)
  - kolom is_imputed menandai baris yang nilai aslinya kosong

Jalankan manual:  python transform.py
Dari Airflow:     from transform import transform
"""
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_DIR = Path(os.getenv("RAW_DIR", "data/raw"))
CLEAN_DIR = Path(os.getenv("CLEAN_DIR", "data/clean"))

NACE_NAME_COL = (
    "statistical_classification_of_economic_activities_in_the_european_community_nace_rev_2"
)

# keys  : (kolom sumber, nama kolom output, gaya)
#         gaya "initcap" = huruf kapital di awal kata, gaya "trim" = hanya buang spasi
# value : nama kolom nilai (dari obs_value), sama dengan nama kolom di schema.sql
# flags : (kolom sumber, nama kolom output); kosong = tidak ada flag
SPECS = {
    "wage_gap": {
        "keys": [
            ("nace_r2", "industry_code", "trim"),
            (NACE_NAME_COL, "industry_name", "trim"),
        ],
        "value": "pay_gap_percentage",
        "flags": [
            ("obs_flag", "observation_flag"),
            ("conf_status", "confidentiality_status"),
        ],
        "primary_key": ["country_code", "year", "industry_code"],
    },
    "emp_gap": {
        "keys": [
            ("wstatus", "employment_status_code", "trim"),
            ("labour_force_and_employment_status", "employment_status_name", "initcap"),
        ],
        "value": "employment_gap_percentage_points",
        "flags": [("obs_flag", "observation_flag")],
        "primary_key": ["country_code", "year", "employment_status_code"],
    },
    "women_mang": {
        "keys": [
            ("prof_pos", "management_position_code", "trim"),
            ("professional_position", "management_position_name", "initcap"),
        ],
        "value": "women_management_percentage",
        "flags": [],
        "primary_key": ["country_code", "year", "management_position_code"],
    },
    "women_college": {
        "keys": [],
        "value": "female_tertiary_education_percentage",
        "flags": [("obs_flag", "observation_flag")],
        "primary_key": ["country_code", "year"],
    },
}


def get_spark():
    return (
        SparkSession.builder.appName("gender_pay_gap_transform")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )


def to_snake_case(df):
    """Sama dengan fungsi di notebook analis."""
    new_names = [
        c.strip().lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace(".", "")
        .replace("(", "")
        .replace(")", "")
        for c in df.columns
    ]
    return df.toDF(*new_names)


def read_raw(spark, path):
    df = spark.read.csv(str(path), header=True, inferSchema=True, escape='"')
    return to_snake_case(df)


def transform_one(spark, name, spec):
    raw = read_raw(spark, RAW_DIR / f"{name}.csv.gz")

    cols = [
        F.trim(F.col("geo")).alias("country_code"),
        F.initcap(F.trim(F.col("geopolitical_entity_reporting"))).alias("country_name"),
        F.col("time_period").alias("year"),
    ]

    # kolom teks (kode dan nama) yang tidak boleh kosong
    text_cols = ["country_code", "country_name"]
    for src, alias, style in spec["keys"]:
        if style == "initcap":
            cols.append(F.initcap(F.trim(F.col(src))).alias(alias))
        else:
            cols.append(F.trim(F.col(src)).alias(alias))
        text_cols.append(alias)

    cols.append(F.round(F.col("obs_value"), 2).alias(spec["value"]))

    # kolom flag: dibiarkan kosong kalau tidak ada flag
    for src, alias in spec["flags"]:
        cols.append(F.trim(F.col(src)).alias(alias))

    # dihitung SEBELUM fillna, supaya tahu baris mana yang nilainya asli kosong
    cols.append(F.col("obs_value").isNull().alias("is_imputed"))

    clean = (
        raw.dropDuplicates()
        .select(*cols)
        .fillna(0)                           # kolom angka
        .fillna("Unknown", subset=text_cols)  # kolom teks (flag tidak ikut diisi)
        .orderBy(*spec["primary_key"])
    )
    return clean


def transform(raw_dir=None, clean_dir=None):
    """Jalankan transform semua dataset. Mengembalikan dict {nama: path file clean}."""
    global RAW_DIR, CLEAN_DIR
    if raw_dir:
        RAW_DIR = Path(raw_dir)
    if clean_dir:
        CLEAN_DIR = Path(clean_dir)
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    spark = get_spark()
    outputs = {}
    try:
        for name, spec in SPECS.items():
            clean = transform_one(spark, name, spec)
            pdf = clean.toPandas()
            out_path = CLEAN_DIR / f"{name}_clean.csv"
            pdf.to_csv(out_path, index=False)
            imputed = int(pdf["is_imputed"].sum())
            print(f"[transform] {name}: {len(pdf)} baris, {imputed} imputed -> {out_path}")
            outputs[name] = str(out_path)
    finally:
        spark.stop()
    return outputs


if __name__ == "__main__":
    print(transform())
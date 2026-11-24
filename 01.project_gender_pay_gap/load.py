"""
Load: data/clean/*_clean.csv -> PostgreSQL (star schema dari Role 2) dan MongoDB (koleksi).

Cara menjalankan:
  python load.py            -> PostgreSQL, lalu MongoDB
  python load.py postgres   -> hanya PostgreSQL
  python load.py mongo      -> hanya MongoDB

PostgreSQL (satu transaksi: kalau satu langkah gagal, semuanya dibatalkan):
  1. kosongkan 4 tabel fakta dan 4 tabel dimensi (dim_year tidak disentuh)
  2. isi tabel dimensi
  3. isi tabel fakta
  4. cek jumlah baris tiap tabel

MongoDB (4 koleksi berisi data clean apa adanya, termasuk kolom is_imputed):
  1. kosongkan koleksi
  2. pasang unique index pada primary key (supaya tidak mungkin ada data ganda)
  3. isi koleksi
  4. cek jumlah dokumen

Keduanya aman dijalankan ulang: data lama diganti, tidak ada duplikat.
Kredensial dibaca dari environment / file .env, JANGAN ditulis di kode.

Env yang dibutuhkan (nama PG_* sama dengan .env di airflow_repo; nama POSTGRES_* juga diterima):
  PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
  MONGO_URI   (alamat MongoDB Atlas)
  MONGO_DB    (opsional, default gender_pay_gap)
  CLEAN_DIR   (opsional, default data/clean)
"""
import json
import os
import sys
from pathlib import Path

import pandas as pd

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # python-dotenv opsional
    pass

CLEAN_DIR = Path(os.getenv("CLEAN_DIR", "data/clean"))

# True  = baris yang nilai aslinya kosong (is_imputed) dimasukkan ke PostgreSQL sebagai NULL
#         (AVG di SQL otomatis mengabaikan NULL)
# False = dimasukkan apa adanya (0)
IMPUTED_AS_NULL = True

# Nama file clean (tanpa _clean.csv). Nama ini juga dipakai sebagai nama koleksi MongoDB.
SOURCE_TABLES = ["wage_gap", "emp_gap", "women_mang", "women_college"]

# Tabel fakta: sumber CSV, kolom yang dimasukkan, kolom nilai, kolom flag
FACTS = {
    "fact_gender_pay_gap": {
        "source": "wage_gap",
        "columns": ["country_code", "year", "industry_code", "pay_gap_percentage",
                    "observation_flag", "confidentiality_status"],
        "value_column": "pay_gap_percentage",
        "flag_columns": ["observation_flag", "confidentiality_status"],
    },
    "fact_employment_gap": {
        "source": "emp_gap",
        "columns": ["country_code", "year", "employment_status_code",
                    "employment_gap_percentage_points", "observation_flag"],
        "value_column": "employment_gap_percentage_points",
        "flag_columns": ["observation_flag"],
    },
    "fact_female_education": {
        "source": "women_college",
        "columns": ["country_code", "year", "female_tertiary_education_percentage",
                    "observation_flag"],
        "value_column": "female_tertiary_education_percentage",
        "flag_columns": ["observation_flag"],
    },
    "fact_women_management": {
        "source": "women_mang",
        "columns": ["country_code", "year", "management_position_code",
                    "women_management_percentage"],
        "value_column": "women_management_percentage",
        "flag_columns": [],
    },
}

# Tabel dimensi (selain dim_country dan dim_year): sumber CSV, kolom kode, kolom nama
DIMENSIONS = {
    "dim_industry": {
        "source": "wage_gap",
        "code": "industry_code",
        "name": "industry_name",
    },
    "dim_employment_status": {
        "source": "emp_gap",
        "code": "employment_status_code",
        "name": "employment_status_name",
    },
    "dim_management_position": {
        "source": "women_mang",
        "code": "management_position_code",
        "name": "management_position_name",
    },
}

# Kolom yang harus unik di tiap koleksi MongoDB (sama dengan primary key di PostgreSQL)
MONGO_KEYS = {
    "wage_gap": ["country_code", "year", "industry_code"],
    "emp_gap": ["country_code", "year", "employment_status_code"],
    "women_mang": ["country_code", "year", "management_position_code"],
    "women_college": ["country_code", "year"],
}


def read_clean(name):
    # keep_default_na=False: teks kosong tetap "" (bukan NaN), teks seperti "NA" tidak berubah
    df = pd.read_csv(CLEAN_DIR / f"{name}_clean.csv", keep_default_na=False)
    df["is_imputed"] = df["is_imputed"].astype(str).str.lower() == "true"
    return df


def make_dimension(df, code_col, name_col, dim_name):
    """Ambil pasangan kode-nama yang unik. Berhenti dengan pesan jelas kalau tidak konsisten."""
    dim = df[[code_col, name_col]].drop_duplicates().sort_values(code_col)
    if dim[code_col].duplicated().any():
        raise ValueError(dim_name + ": ada kode yang punya lebih dari satu nama")
    if dim[name_col].duplicated().any():
        raise ValueError(dim_name + ": ada nama yang dipakai lebih dari satu kode")
    return dim


def build_dimensions(tables):
    """Bangun dataframe untuk tabel dimensi dari CSV clean."""
    dimensions = {}

    # dim_country: gabungan negara dari keempat tabel
    all_countries = []
    for name in SOURCE_TABLES:
        all_countries.append(tables[name][["country_code", "country_name"]])
    all_countries = pd.concat(all_countries)
    dimensions["dim_country"] = make_dimension(
        all_countries, "country_code", "country_name", "dim_country"
    )

    for dim_name, cfg in DIMENSIONS.items():
        source_df = tables[cfg["source"]]
        dimensions[dim_name] = make_dimension(source_df, cfg["code"], cfg["name"], dim_name)

    return dimensions


def build_facts(tables):
    """Bangun dataframe untuk tabel fakta dari CSV clean."""
    facts = {}
    for fact_name, cfg in FACTS.items():
        source_df = tables[cfg["source"]]
        fact = source_df[cfg["columns"]].copy()

        # baris yang nilai aslinya kosong -> NULL
        value_col = cfg["value_column"]
        fact[value_col] = fact[value_col].astype(float)
        if IMPUTED_AS_NULL:
            fact.loc[source_df["is_imputed"], value_col] = None

        # flag kosong ("") -> NULL
        for flag_col in cfg["flag_columns"]:
            fact.loc[fact[flag_col] == "", flag_col] = None

        facts[fact_name] = fact
    return facts


def get_env(names, default=None):
    """Ambil nilai environment dari nama pertama yang terisi.
    Contoh: get_env(["PG_HOST", "POSTGRES_HOST"]) mencoba PG_HOST dulu, lalu POSTGRES_HOST."""
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    if default is not None:
        return default
    raise KeyError("Environment variable belum diisi: " + " atau ".join(names))


def pg_engine():
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    url = URL.create(
        "postgresql+psycopg2",
        username=get_env(["PG_USER", "POSTGRES_USER"]),
        password=get_env(["PG_PASSWORD", "POSTGRES_PASSWORD"]),
        host=get_env(["PG_HOST", "POSTGRES_HOST"]),
        port=int(get_env(["PG_PORT", "POSTGRES_PORT"], default="5432")),
        database=get_env(["PG_DB", "POSTGRES_DB"]),
    )
    return create_engine(url)


def load_postgres(dimensions, facts):
    from sqlalchemy import text

    # urutan isi: dimensi dulu, baru fakta (karena fakta punya foreign key ke dimensi)
    insert_order = list(dimensions.keys()) + list(facts.keys())
    all_data = {}
    all_data.update(dimensions)
    all_data.update(facts)

    engine = pg_engine()
    # satu transaksi: kalau ada yang gagal, semuanya dibatalkan
    with engine.begin() as conn:
        # 1. kosongkan semua tabel (kecuali dim_year) dengan SATU perintah,
        #    karena tabel fakta dan dimensi saling berhubungan lewat foreign key
        table_list = ", ".join("public." + name for name in insert_order)
        conn.execute(text("TRUNCATE TABLE " + table_list))

        # dim_year sudah diisi oleh schema.sql; baris ini hanya jaga-jaga
        conn.execute(text(
            "INSERT INTO public.dim_year (year) "
            "SELECT generate_series(2015, 2024)::SMALLINT ON CONFLICT (year) DO NOTHING"
        ))

        # 2 dan 3. isi tabel dimensi, lalu tabel fakta
        for name in insert_order:
            df = all_data[name]
            df.to_sql(name, conn, schema="public", if_exists="append",
                      index=False, method="multi", chunksize=1000)

        # 4. cek jumlah baris sama dengan CSV
        for name in insert_order:
            count = conn.execute(text("SELECT COUNT(*) FROM public." + name)).scalar()
            if count != len(all_data[name]):
                raise ValueError(
                    name + ": di database " + str(count) + " baris, di CSV "
                    + str(len(all_data[name])) + " baris"
                )
            print("[load] postgres." + name + ": " + str(count) + " baris")


def make_mongo_docs(df):
    """Ubah dataframe menjadi daftar dokumen MongoDB. Teks kosong ("") menjadi null."""
    data = df.astype(object)
    data = data.where(data != "", None)
    # lewat JSON supaya tipe numpy berubah jadi tipe Python biasa
    return json.loads(data.to_json(orient="records"))


def load_mongo(tables, required=False):
    uri = os.getenv("MONGO_URI")
    if not uri:
        if required:
            raise ValueError("MONGO_URI kosong. Isi dulu di file .env")
        print("[load] MONGO_URI kosong -> MongoDB dilewati")
        return

    from pymongo import MongoClient

    db_name = os.getenv("MONGO_DB", "gender_pay_gap")
    # timeout 15 detik: kalau Atlas tidak bisa dihubungi, langsung gagal dengan pesan jelas
    client = MongoClient(uri, serverSelectionTimeoutMS=15000)
    try:
        db = client[db_name]
        for name in SOURCE_TABLES:
            docs = make_mongo_docs(tables[name])
            coll = db[name]

            # 1. kosongkan koleksi lama
            coll.delete_many({})
            # 2. pasang unique index pada primary key: data ganda akan ditolak
            index_keys = []
            for key in MONGO_KEYS[name]:
                index_keys.append((key, 1))
            coll.create_index(index_keys, unique=True)
            # 3. isi koleksi
            coll.insert_many(docs)
            # 4. cek jumlah dokumen sama dengan CSV
            count = coll.count_documents({})
            if count != len(docs):
                raise ValueError(
                    name + ": di MongoDB " + str(count) + " dokumen, di CSV "
                    + str(len(docs)) + " baris"
                )
            print("[load] mongo." + db_name + "." + name + ": " + str(count) + " dokumen")
    finally:
        client.close()


def load_all(target="all"):
    if target not in ("all", "postgres", "mongo"):
        raise ValueError("target harus all, postgres, atau mongo (bukan '" + target + "')")

    tables = {}
    for name in SOURCE_TABLES:
        tables[name] = read_clean(name)

    if target in ("all", "postgres"):
        dimensions = build_dimensions(tables)
        facts = build_facts(tables)
        load_postgres(dimensions, facts)

    if target in ("all", "mongo"):
        # kalau diminta khusus "mongo", MONGO_URI wajib ada
        load_mongo(tables, required=(target == "mongo"))


if __name__ == "__main__":
    load_target = "all"
    if len(sys.argv) > 1:
        load_target = sys.argv[1]
    load_all(load_target)
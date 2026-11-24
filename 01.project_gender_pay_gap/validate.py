"""
Validate: cek kualitas data clean dengan Great Expectations (GX) SEBELUM load.

Jalankan manual : python validate.py
Dari Airflow    : task validate_gx (BashOperator menjalankan file ini)

Hasil:
  - Semua cek lolos  -> exit code 0 -> task hijau -> task load boleh jalan
  - Ada cek gagal    -> exit code 1 -> task merah  -> task load TIDAK jalan
  - Laporan PASS/FAIL disimpan di REPORT_DIR/quality_report.csv

Standar kualitas dibuat per tabel di dictionary TABLES di bawah, dan mengikuti
schema.sql dari Role 2 (primary key, batas panjang kolom, batas 0-100, UNIQUE nama dimensi).
"""

import os
import sys

import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe


# ---------- Lokasi folder ----------
CLEAN_DIR = os.getenv("CLEAN_DIR", "data/clean")
REPORT_DIR = os.getenv("REPORT_DIR", "data/reports")

# Mode UJI GAGAL: kalau VALIDATE_FORCE_FAIL=true, satu cek yang SENGAJA salah ditambahkan.
# Gunanya untuk membuktikan bahwa kalau validasi gagal, task load tidak jalan.
# Biarkan kosong/false untuk pemakaian normal.
FORCE_FAIL = os.getenv("VALIDATE_FORCE_FAIL", "false").lower() == "true"

# Semua dataset mencakup tahun 2015 - 2024
YEARS = list(range(2015, 2025))


# ---------- Standar kualitas per tabel ----------
# rows            : jumlah baris yang harus sama (angka acuan dari notebook)
# imputed         : jumlah baris yang diisi otomatis (missing -> 0), kolom is_imputed
# rate_col        : kolom nilai utama (persen), sama dengan nama kolom di schema.sql
# rate_min/max    : batas wajar nilai
# key             : primary key (kombinasi kolom harus unik dan tidak null)
# category_col    : kolom kode kategori (boleh None)
# category_values : nilai kode yang diperbolehkan (boleh None)
# text_cols       : kolom teks wajib terisi -> {nama kolom: panjang maksimum di schema.sql}
# flag_cols       : kolom flag (boleh kosong) -> {nama kolom: panjang maksimum di schema.sql}
TABLES = {
    "wage_gap": {
        "file": "wage_gap_clean.csv",
        "rows": 4829,
        "imputed": 33,
        "rate_col": "pay_gap_percentage",
        "rate_min": -100,
        "rate_max": 100,
        "key": ["country_code", "year", "industry_code"],
        "category_col": "industry_code",
        "category_values": ["B", "C", "D", "E", "F", "G", "H", "I", "J",
                            "K", "L", "M", "N", "O", "P", "Q", "R"],
        "text_cols": {"country_name": 100, "industry_name": 200},
        "flag_cols": {"observation_flag": 10, "confidentiality_status": 10},
    },
    "emp_gap": {
        "file": "emp_gap_clean.csv",
        "rows": 1360,
        "imputed": 7,
        "rate_col": "employment_gap_percentage_points",
        "rate_min": -100,
        "rate_max": 100,
        "key": ["country_code", "year", "employment_status_code"],
        "category_col": "employment_status_code",
        "category_values": ["EMP", "EMP_PT", "EMP_TEMP", "UEMP_PT"],
        "text_cols": {"country_name": 100, "employment_status_name": 100},
        "flag_cols": {"observation_flag": 10},
    },
    "women_mang": {
        "file": "women_mang_clean.csv",
        "rows": 664,
        "imputed": 0,
        "rate_col": "women_management_percentage",
        "rate_min": 0,
        "rate_max": 100,
        "key": ["country_code", "year", "management_position_code"],
        "category_col": "management_position_code",
        "category_values": ["NEXEC", "NEXEC_EXEC"],
        "text_cols": {"country_name": 100, "management_position_name": 150},
        "flag_cols": {},
    },
    "women_college": {
        "file": "women_college_clean.csv",
        "rows": 345,
        "imputed": 0,
        "rate_col": "female_tertiary_education_percentage",
        "rate_min": 0,
        "rate_max": 100,
        "key": ["country_code", "year"],
        "category_col": None,
        "category_values": None,
        "text_cols": {"country_name": 100},
        "flag_cols": {"observation_flag": 10},
    },
}

# ---------- Standar tabel dimensi (mengikuti PRIMARY KEY dan UNIQUE di schema.sql) ----------
# Satu kode harus punya satu nama, dan satu nama hanya untuk satu kode,
# kalau tidak, load ke tabel dim_* akan gagal.
DIMENSIONS = [
    {"name": "dim_country", "code": "country_code", "label": "country_name",
     "sources": ["wage_gap", "emp_gap", "women_mang", "women_college"]},
    {"name": "dim_industry", "code": "industry_code", "label": "industry_name",
     "sources": ["wage_gap"]},
    {"name": "dim_employment_status", "code": "employment_status_code",
     "label": "employment_status_name", "sources": ["emp_gap"]},
    {"name": "dim_management_position", "code": "management_position_code",
     "label": "management_position_name", "sources": ["women_mang"]},
]


# ---------- Fungsi bantu ----------
def run_check(batch, results, table_name, label, expectation):
    """Jalankan satu expectation, simpan hasilnya (True = lolos)."""
    result = batch.validate(expectation)
    passed = bool(result.success)
    results.append([table_name, label, passed])

    if passed:
        print("  PASS  " + label)
    else:
        print("  FAIL  " + label)


def get_required_columns(cfg):
    """Daftar kolom yang wajib ada di CSV clean."""
    columns = ["country_code", "year", cfg["rate_col"], "is_imputed"]
    for k in cfg["key"]:
        if k not in columns:
            columns.append(k)
    for c in cfg["text_cols"]:
        if c not in columns:
            columns.append(c)
    for c in cfg["flag_cols"]:
        if c not in columns:
            columns.append(c)
    return columns


# ---------- Program utama ----------
def main():
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("clean_data")

    results = []
    loaded = {}   # dataframe tiap tabel yang berhasil dibaca (dipakai untuk cek dimensi)

    for table_name, cfg in TABLES.items():
        print("")
        print("=== " + table_name + " ===")

        # Baca file clean
        path = CLEAN_DIR + "/" + cfg["file"]
        if not os.path.exists(path):
            print("  FAIL  file tidak ditemukan: " + path)
            results.append([table_name, "file clean ada", False])
            continue
        df = pd.read_csv(path)

        # Cek 1: kolom wajib ada. Kalau ada yang hilang, cek lain dilewati.
        missing = []
        for col in get_required_columns(cfg):
            if col not in df.columns:
                missing.append(col)
        if len(missing) > 0:
            print("  FAIL  kolom wajib tidak ada: " + ", ".join(missing))
            results.append([table_name, "kolom wajib ada", False])
            continue
        results.append([table_name, "kolom wajib ada", True])
        print("  PASS  kolom wajib ada")

        # is_imputed dijadikan angka 0/1 supaya bisa dijumlahkan
        df["is_imputed"] = (df["is_imputed"].astype(str).str.lower() == "true").astype(int)
        loaded[table_name] = df

        # Siapkan batch GX dari dataframe
        asset = data_source.add_dataframe_asset(name=table_name)
        batch_definition = asset.add_batch_definition_whole_dataframe("batch_" + table_name)
        batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

        # Cek 2: jumlah baris sesuai angka acuan
        run_check(batch, results, table_name,
                  "jumlah baris = " + str(cfg["rows"]),
                  gxe.ExpectTableRowCountToEqual(value=cfg["rows"]))

        # Cek 3: country_code harus 2 huruf kapital (contoh: BE, DE, EL)
        run_check(batch, results, table_name,
                  "country_code 2 huruf kapital",
                  gxe.ExpectColumnValuesToMatchRegex(column="country_code", regex=r"^[A-Z]{2}$"))

        # Cek 4: kolom teks wajib terisi, bukan 'Unknown', tanpa spasi di tepi,
        #        dan panjangnya muat di kolom database
        for text_col, max_len in cfg["text_cols"].items():
            run_check(batch, results, table_name,
                      text_col + " tidak null",
                      gxe.ExpectColumnValuesToNotBeNull(column=text_col))
            run_check(batch, results, table_name,
                      text_col + " bukan 'Unknown'",
                      gxe.ExpectColumnValuesToNotBeInSet(column=text_col, value_set=["Unknown"]))
            run_check(batch, results, table_name,
                      text_col + " tanpa spasi di tepi",
                      gxe.ExpectColumnValuesToMatchRegex(column=text_col, regex=r"^\S(.*\S)?$"))
            run_check(batch, results, table_name,
                      text_col + " maksimal " + str(max_len) + " karakter",
                      gxe.ExpectColumnValueLengthsToBeBetween(column=text_col,
                                                              min_value=1, max_value=max_len))

        # Cek 5: kolom flag (boleh kosong) panjangnya muat di kolom database
        for flag_col, max_len in cfg["flag_cols"].items():
            if df[flag_col].notna().any():
                run_check(batch, results, table_name,
                          flag_col + " maksimal " + str(max_len) + " karakter",
                          gxe.ExpectColumnValueLengthsToBeBetween(column=flag_col,
                                                                  min_value=1, max_value=max_len))

        # Cek 6: year bertipe integer, 2015-2024, dan semua tahun ada
        run_check(batch, results, table_name,
                  "year bertipe integer",
                  gxe.ExpectColumnTypeToBe(column="year", type_="int64"))
        run_check(batch, results, table_name,
                  "year di antara 2015 dan 2024",
                  gxe.ExpectColumnValuesToBeBetween(column="year", min_value=2015, max_value=2024))
        run_check(batch, results, table_name,
                  "semua tahun 2015-2024 ada",
                  gxe.ExpectColumnDistinctValuesToEqualSet(column="year", value_set=YEARS))

        # Cek 7: nilai rate tidak kosong dan dalam rentang wajar
        run_check(batch, results, table_name,
                  cfg["rate_col"] + " tidak null",
                  gxe.ExpectColumnValuesToNotBeNull(column=cfg["rate_col"]))
        run_check(batch, results, table_name,
                  cfg["rate_col"] + " antara " + str(cfg["rate_min"]) + " dan " + str(cfg["rate_max"]),
                  gxe.ExpectColumnValuesToBeBetween(column=cfg["rate_col"],
                                                    min_value=cfg["rate_min"],
                                                    max_value=cfg["rate_max"]))

        # Cek 8: primary key tidak null dan unik
        for key_col in cfg["key"]:
            run_check(batch, results, table_name,
                      "PK kolom " + key_col + " tidak null",
                      gxe.ExpectColumnValuesToNotBeNull(column=key_col))
        run_check(batch, results, table_name,
                  "PK unik: " + "+".join(cfg["key"]),
                  gxe.ExpectCompoundColumnsToBeUnique(column_list=cfg["key"]))

        # Cek 9: kode kategori hanya nilai yang diperbolehkan
        if cfg["category_col"] is not None:
            run_check(batch, results, table_name,
                      cfg["category_col"] + " hanya berisi kode yang valid",
                      gxe.ExpectColumnValuesToBeInSet(column=cfg["category_col"],
                                                      value_set=cfg["category_values"]))

        # Cek 10: jumlah baris hasil imputasi sesuai
        run_check(batch, results, table_name,
                  "jumlah is_imputed = " + str(cfg["imputed"]),
                  gxe.ExpectColumnSumToBeBetween(column="is_imputed",
                                                 min_value=cfg["imputed"],
                                                 max_value=cfg["imputed"]))

    # ---------- Cek tabel dimensi ----------
    for dim in DIMENSIONS:
        print("")
        print("=== " + dim["name"] + " ===")

        # semua tabel sumber harus berhasil dibaca
        parts = []
        for source in dim["sources"]:
            if source in loaded:
                parts.append(loaded[source][[dim["code"], dim["label"]]])
        if len(parts) != len(dim["sources"]):
            print("  FAIL  tabel sumber belum lengkap, cek dimensi dilewati")
            results.append([dim["name"], "tabel sumber lengkap", False])
            continue

        dim_df = pd.concat(parts).drop_duplicates()

        asset = data_source.add_dataframe_asset(name=dim["name"])
        batch_definition = asset.add_batch_definition_whole_dataframe("batch_" + dim["name"])
        batch = batch_definition.get_batch(batch_parameters={"dataframe": dim_df})

        run_check(batch, results, dim["name"],
                  dim["code"] + " unik (satu kode = satu nama)",
                  gxe.ExpectColumnValuesToBeUnique(column=dim["code"]))
        run_check(batch, results, dim["name"],
                  dim["label"] + " unik (satu nama = satu kode)",
                  gxe.ExpectColumnValuesToBeUnique(column=dim["label"]))

    # ---------- Uji gagal (hanya aktif kalau VALIDATE_FORCE_FAIL=true) ----------
    if FORCE_FAIL:
        print("")
        print("=== UJI GAGAL (sengaja) ===")
        print("  Mode uji gagal AKTIF: satu cek di bawah ini sengaja dibuat salah.")
        test_df = pd.DataFrame({"x": [1]})
        asset = data_source.add_dataframe_asset(name="uji_gagal")
        batch_definition = asset.add_batch_definition_whole_dataframe("batch_uji_gagal")
        batch = batch_definition.get_batch(batch_parameters={"dataframe": test_df})
        # Tabel ini hanya punya 1 baris, jadi cek "harus 2 baris" PASTI salah
        run_check(batch, results, "uji_gagal",
                  "[SENGAJA SALAH] tabel 1 baris dicek harus 2 baris",
                  gxe.ExpectTableRowCountToEqual(value=2))

    # ---------- Simpan laporan ----------
    report = pd.DataFrame(results, columns=["table", "check", "passed"])
    os.makedirs(REPORT_DIR, exist_ok=True)
    report.to_csv(REPORT_DIR + "/quality_report.csv", index=False)

    total = len(report)
    failed = report[report["passed"] == False]

    print("")
    print("==============================")
    print("Total cek : " + str(total))
    print("Lolos     : " + str(total - len(failed)))
    print("Gagal     : " + str(len(failed)))
    print("==============================")

    if len(failed) > 0:
        print("Daftar yang gagal:")
        for i in range(len(failed)):
            row = failed.iloc[i]
            print("  - [" + row["table"] + "] " + row["check"])
        print("Data TIDAK boleh di-load.")
        sys.exit(1)

    print("Semua cek lolos. Data boleh di-load.")


if __name__ == "__main__":
    main()
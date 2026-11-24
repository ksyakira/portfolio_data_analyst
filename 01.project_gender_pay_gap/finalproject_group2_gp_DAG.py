"""
DAG pipeline Gender Pay Gap Eropa.

Alur:
  extract -> transform -> validate_gx -> load_postgres
                                      -> load_mongo

Task load hanya jalan kalau validate_gx lolos semua cek.
DAG berjalan otomatis sesuai jadwal SCHEDULE_CRON di bawah.
"""

from datetime import timedelta

import pendulum

# Import ini punya cadangan supaya jalan di Airflow 3 maupun Airflow 2
try:
    from airflow.sdk import DAG
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow import DAG
    from airflow.operators.bash import BashOperator


# ---------- Jadwal ----------
# Format cron: menit  jam  tanggal  bulan  hari-dalam-minggu
# Hari dalam minggu: 1 = Senin, 2 = Selasa, ... 6 = Sabtu, 0 = Minggu
#
# Contoh:
#   "0 6 * * 1"     -> setiap Senin pukul 06.00
#   "0 9 * * *"     -> setiap hari pukul 09.00
#   "0 6 1 * *"     -> setiap tanggal 1 pukul 06.00
#   "*/5 * * * *"   -> setiap 5 menit (HANYA untuk demo/tes, jangan dibiarkan)
SCHEDULE_CRON = "0 6 * * 1"

# Zona waktu jadwal: jam di atas dihitung dalam WIB
TIMEZONE = "Asia/Jakarta"


# ---------- Pengaturan lokasi (path di DALAM container) ----------
SCRIPT_DIR = "/workspace/scripts/gp"
RAW_DIR = "/workspace/data/gp/raw"
CLEAN_DIR = "/workspace/data/gp/clean"
REPORT_DIR = "/workspace/data/gp/reports"

# Variabel ini dibaca oleh extract.py, transform.py, validate.py, dan load.py
EXTRA_ENV = {
    "RAW_DIR": RAW_DIR,
    "CLEAN_DIR": CLEAN_DIR,
    "REPORT_DIR": REPORT_DIR,
}


# ---------- Pengaturan dasar DAG ----------
default_args = {
    "owner": "role1_data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="gender_pay_gap_pipeline",
    description="Extract, transform, validate (GX), load data gender pay gap Eropa",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 11, 1, tz=TIMEZONE),
    schedule=SCHEDULE_CRON,   # jalan otomatis sesuai jadwal (DAG harus dalam keadaan tidak di-pause)
    catchup=False,            # jangan menjalankan jadwal yang terlewat
    max_active_runs=1,        # satu run saja pada satu waktu, supaya tidak tabrakan
    tags=["gender-pay-gap", "final-project"],
) as dag:

    # Task 1: download data mentah
    extract = BashOperator(
        task_id="extract",
        bash_command="mkdir -p " + RAW_DIR + " && cd " + SCRIPT_DIR + " && python extract.py",
        env=EXTRA_ENV,
        append_env=True,   # tetap membawa variabel dari .env (PG_*, MONGO_URI)
    )

    # Task 2: cleaning dengan PySpark
    transform = BashOperator(
        task_id="transform",
        bash_command="mkdir -p " + CLEAN_DIR + " && cd " + SCRIPT_DIR + " && python transform.py",
        env=EXTRA_ENV,
        append_env=True,
    )

    # Task 3: cek kualitas data dengan Great Expectations
    # Kalau ada cek yang gagal, validate.py keluar dengan kode 1 -> task merah ->
    # kedua task load di bawah TIDAK jalan.
    validate = BashOperator(
        task_id="validate_gx",
        bash_command="mkdir -p " + REPORT_DIR + " && cd " + SCRIPT_DIR + " && python validate.py",
        env=EXTRA_ENV,
        append_env=True,
    )

    # Task 4a: masukkan ke PostgreSQL (star schema)
    # trigger_rule all_success = hanya jalan kalau task sebelumnya (validate_gx) berhasil
    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command="cd " + SCRIPT_DIR + " && python load.py postgres",
        env=EXTRA_ENV,
        append_env=True,
        trigger_rule="all_success",
    )

    # Task 4b: masukkan ke MongoDB Atlas (4 koleksi)
    load_mongo = BashOperator(
        task_id="load_mongo",
        bash_command="cd " + SCRIPT_DIR + " && python load.py mongo",
        env=EXTRA_ENV,
        append_env=True,
        trigger_rule="all_success",
    )

    # Urutan task: kedua load berjalan paralel setelah validate lolos
    extract >> transform >> validate >> [load_postgres, load_mongo]

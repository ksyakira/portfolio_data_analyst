# Data Analyst Portfolio — Khoizuron Syakira

Selamat datang di repository portofolio Data Analyst saya. Repository ini mendokumentasikan kumpulan proyek analisis data dan *data engineering* *end-to-end* yang mencakup pengambilan data (*web scraping* & SQL), pembangunan *ETL pipeline* otomatis dan *data warehouse*, analisis statistik deskriptif & inferensial, hingga pembuatan *dashboard* visualisasi interaktif untuk mendukung pengambilan keputusan berbasis data.

---

## 🛠️ Tech Stack & Tools

- **Programming & Data Processing:** Python (Pandas, NumPy, SciPy, BeautifulSoup, PySpark), Great Expectations
- **Database & Data Engineering:** PostgreSQL, Google BigQuery, MongoDB Atlas, SQL (DDL/DML/DQL), Relational Data Modeling (ERD & Star/Snowflake Schema), Apache Airflow (DAG), Shell Scripting, Docker
- **Visualization & BI:** Tableau Public, Google Sheets, Matplotlib, Seaborn
- **Version Control:** Git & GitHub

---

## 📂 Featured & Milestone Projects

### 1. Gender Pay Gap Trend in Europe — End-to-End ETL Pipeline & Analysis (Final Project)
Proyek kolaborasi pembangunan *data pipeline* otomatis dan analisis tren kesenjangan upah berdasarkan gender di Eropa.
- **Scope & Workflow:** Perancangan skema basis data (*ERD*), *data cleaning & validation*, pembangunan skrip ETL modular (`extract.py`, `transform.py`, `validate.py`, `load.py`), penjadwalan alur kerja otomatis dengan Apache Airflow DAG, serta perancangan *dashboard* interaktif di Tableau.
- **Tools:** Python, SQL, Apache Airflow, Tableau, Shell Script
- **Repository:** [GenderProject](https://github.com/kirizqy/GenderProject)

---

### 2. Automated Travel & Tourism Booking ETL Pipeline (Milestone 3)
Proyek perancangan *end-to-end* ETL *pipeline* otomatis untuk agen perjalanan (*travel agency*) yang memuat data pemesanan perjalanan ke **MongoDB Atlas** guna menganalisis destinasi populer, tren pembatalan (*cancellation*), dan biaya perjalanan per individu di dalam lingkungan kontainer **Docker** dengan **JupyterLab**.
- **Sumber Data & Tantangan Awal:** Menggunakan dataset mentah `P2M3_khoizuron_syakira_data_raw.csv` (1.000 transaksi, 29 kolom: 18 teks, 9 *integer*, 2 desimal) dengan tantangan berupa data PII (`Customer_Name`), string tanggal yang belum diparse, penamaan kolom *mixed-case*, serta anomali nilai rating `0` (*"Not Rated Yet"*).
- **Temuan Utama EDA:** Tidak ditemukan *missing values*, duplikat, atau spasi berlebih pada kolom teks, serta relasi tanggal dan jumlah pelancong konsisten. Nilai rating `0.0` (228 baris) hanya muncul pada status `Cancelled`, `Confirmed`, dan `Pending`; pada booking `Completed` (772 baris), rata-rata rating sebenarnya sekitar **3,0** (~2,9987), bukan **~2,3**. Sementara itu, booking `Cancelled` tetap memiliki biaya perjalanan tinggi (rata-rata ~245.279; median 171.700 dibanding `Completed` dengan rata-rata ~220.196; median 150.950) dan *outlier* biaya tinggi menarik rata-rata ke atas.
- **Alur Pipeline & Orkestrasi:**
  - **Validasi (`Great Expectations`):** Menjalankan 7 aturan validasi pada data mentah sebelum transformasi (keunikan `Booking_ID`, rentang `Hotel_Rating` 1–5, nilai `Booking_Status`, tipe numerik `Total_Trip_Cost`, rata-rata `Customer_Rating` 2,5–3,5 dan median `Total_Trip_Cost` 120.000–200.000 pada `Completed`, serta `Total_Trip_Cost > Discount_Amount`).
  - **Ekstraksi (`extract.py`):** Membaca CSV mentah via `SparkSession` (`"TravelExtract"`) dan menyimpannya ke `/workspace/data/travel/extract_result.csv`.
  - **Transformasi (`transform.py`):** Menghapus kolom PII `Customer_Name`, melakukan `trim`/`dropDuplicates()`/`dropna()`, mengubah `Customer_Rating == 0` menjadi `NULL`, mengonversi kolom tanggal ke tipe `DATE`, menambahkan `cost_per_traveller`, dan menyeragamkan nama kolom ke *lowercase snake_case* pada `/workspace/data/travel/transform_result.csv`.
  - **Pemuatan (`load.py`):** Memuat dokumen ke MongoDB Atlas (`milestone3.travel_bookings`) via `MONGO_URI` dengan konversi `ISODate` dan pola idempoten (`delete_many({})` sebelum `insert_many`) agar jumlah dokumen tetap konsisten 1.000.
  - **Orkestrasi (`P2M3_khoizuron_syakira_DAG.py`):** Menjadwalkan `extract >> transform >> load` lewat `BashOperator` di **Apache Airflow** dengan jadwal cron `"10,20,30 9 * * 6"` (Sabtu pukul 09:10, 09:20, 09:30), `start_date=datetime(2024, 11, 1)`, `catchup=False`, dan `max_active_runs=1`.
- **Tools:** Python, PySpark, Great Expectations, MongoDB Atlas, Apache Airflow, Docker, JupyterLab

---

### 3. Chicago Theft Crime Pattern Analysis & Interactive Dashboard (Milestone 2)
Proyek analisis data menyeluruh untuk **Kepolisian Chicago (*Chicago Police Department*)** dalam menghadapi keterbatasan personel, menggunakan kerangka kerja **SMART**, penjabaran masalah **5W+1H**, pengujian statistik, dan pelaporan visual di **Tableau Public**.
- **Scope & Workflow:** Menarik data pencurian (`THEFT`) 1 tahun penuh (**Oktober 2025 – September 2026**) dari `bigquery-public-data.chicago_crime.crime` (`RAND()` limit 10.000 baris, 10 kolom utama), membersihkannya di `pandas` menjadi **9.974 baris** (menghapus baris kosong pada `ward`, `community_area`, dan `location_description`, mengubah `ward` & `community_area` menjadi string kategorikal, mengekstrak `month`, `day_name`, dan `hour`), lalu menganalisis tren bulanan, *heatmap* hari & jam rawan, 5 *community area* teratas, perbandingan kuartal, fluktuasi/*outlier* pencurian di `Street`, serta uji hipotesis perbedaan *arrest rate* antara lokasi publik (`Street`/`Sidewalk`) dan privat (`Residence`/`Apartment`) dengan target penurunan kasus **10%** pada bulan berikutnya.
- **Temuan Utama:** Kasus tertinggi terjadi pada **Oktober 2025 (937 kasus)**, **Mei 2026 (931 kasus)**, dan **Agustus 2026 (907 kasus)**, sedangkan terendah pada **Februari 2026 (667 kasus)** saat musim dingin; rata-rata bulanan historis (~831 kasus) menjadi acuan proyeksi November 2026 dari September 2026 (819 kasus).
- **Tools:** Python (Pandas, SciPy, Matplotlib/Seaborn), Google BigQuery / SQL, Tableau Public
- **Interactive Dashboard:** [Lihat Dashboard di Tableau Public](https://public.tableau.com/views/M2_17926637818820/Visualisasi?:language=en-GB&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)

---

### 4. E-Commerce Retail ETL Pipeline: Web Scraping to PostgreSQL (Milestones 1)
Pembangunan alur *Extract, Transform, Load* (ETL) dari data retail e-commerce hingga tersimpan dalam basis data relasional PostgreSQL.
- **Scope & Workflow:** *Web scraping* katalog produk retail menggunakan Python, pembersihan dan standardisasi tipe data numerik dengan Pandas, perancangan skema tabel relasional (DDL), serta pemuatan data ke PostgreSQL (DML).
- **Tools:** Python (BeautifulSoup, Pandas), Jupyter Notebook, PostgreSQL

---

## 🧪 Technical Challenges & Case Studies

| Project | Fokus & Studi Kasus | Tools & Metode Utama |
| :--- | :--- | :--- |
| **Graded Challenge 6 — E-Commerce Data Warehouse (`thelook_ecommerce`)** | Perancangan *Data Warehouse* (Fact & Dimension tables) untuk proses penjualan e-commerce serta pembangunan *pipeline* ETL berskala besar. | Google BigQuery, PySpark, PostgreSQL (DDL/DML), Data Modeling |
| **Graded Challenge 5 — Practical Statistics & Market Feasibility (Seblak Dropship)** | Studi kelayakan bisnis dropship menggunakan kerangka SMART, statistik deskriptif (skewness, kurtosis), estimasi pendapatan dengan *95% Confidence Interval*, uji hipotesis antarwilayah, dan uji korelasi harga vs rating. | Python (Pandas, SciPy), Hypothesis Testing, Correlation Analysis |
| **Graded Challenge 4 — Portugal Real Estate Market Analysis** | Analisis faktor penentu harga properti dan peluang peningkatan penjualan sebesar 10% dalam 6 bulan melalui penjabaran *key questions*, *data cleaning*, dan penyusunan *actionable recommendations*. | Google Sheets (Pivot Table, Formulas), Data Cleaning, Business Reporting |
| **Graded Challenge 3 — E-Commerce Web Scraping & Data Preparation** | Ekstraksi data produk (harga, lokasi toko, jumlah terjual, rating) dari 10+ halaman pencarian Tokopedia dengan penanganan struktur HTML dinamis dan *missing values*. | Python (BeautifulSoup, Pandas), Regex / String Cleaning |
| **Graded Challenge 2 — Interactive Retail Shopping

# Đồ án Nhập môn Dữ liệu lớn – Storage → Processing (NYC TLC, PySpark)

Pipeline phụ trách: **Storage → Processing**. Dữ liệu: NYC TLC Trip Record Data (Parquet).
Tiến độ và quyết định: xem `NOTES.md`. Kế hoạch: `docs/KE_HOACH_CA_NHAN.md`.

## Yêu cầu môi trường
- Java JDK 8/11/17 đều chạy được với PySpark 3.5 (khuyến nghị 17; kiểm tra: `java -version`)
- Python 3.9–3.12 (khuyến nghị 3.11)
- ~8 GB RAM; ~5 GB đĩa trống cho 12 tháng Yellow

## Cài đặt
```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Windows: bước bắt buộc trước Bước 0
Spark ghi file cục bộ trên Windows cần `winutils.exe` (nếu thiếu sẽ báo lỗi `HADOOP_HOME and hadoop.home.dir are unset`).
```powershell
python scripts/setup_windows_hadoop.py   # tải winutils.exe + hadoop.dll vào tools/hadoop/bin (không cần admin)
```
`jobs/common.py` tự đặt `HADOOP_HOME` trỏ tới `tools/hadoop`, không cần chỉnh biến môi trường.
Đây là bản build cộng đồng (cdarlint/winutils, Hadoop 3.3.6), không phải của Apache; script in SHA-256 để ghi vào phụ lục.
Cảnh báo `NativeCodeLoader: Unable to load native-hadoop library` là bình thường, có thể bỏ qua.
Thay thế không dùng binary bên thứ ba: chạy dự án trong WSL2 (Ubuntu) hoặc Docker.

## Bước 0 – kiểm tra môi trường
```bash
python jobs/00_check_env.py      # kỳ vọng: "OK – môi trường sẵn sàng." và file results/00_check_env/env_check.json
```

## Bước 1 – tải dữ liệu
```bash
python scripts/download_data.py --type yellow --start 2024-01 --end 2024-12   # 12 tháng + bảng taxi zone
python scripts/download_data.py --type yellow --start 2024-01 --end 2024-03 --dry-run   # chỉ in URL
```
Mẹo: chạy lại lệnh sẽ bỏ qua file đã tải đủ. Log tải: `results/download_data/download_log.csv`.
Nếu 404, kiểm tra tháng đó đã được TLC công bố chưa (thường trễ ~2 tháng).

## Bước 2 – kiểm tra dữ liệu thô (không cần Spark, vài giây)
```bash
python scripts/inspect_raw.py          # in số dòng từng tháng, cột khác kiểu/thiếu giữa các tháng
```
Ghi `results/inspect_raw/schema_inspection.csv`, `results/inspect_raw/raw_files_summary.csv`.

## Bước 3 – nạp vào Data Lake (Parquet phân vùng year/month)
```bash
python jobs/01_ingest_to_lake.py                              # tất cả tháng trong data/raw
python jobs/01_ingest_to_lake.py --start 2024-01 --end 2024-03   # chỉ vài tháng
```
Kết quả: `data/lake/yellow/year=YYYY/month=M/` và `data/lake/dim_zone/`; log kiểm chứng số dòng: `results/01_ingest_to_lake/ingest_log.csv`.
Chạy lại một tháng sẽ chỉ ghi đè đúng phân vùng đó. Nếu thấy trace lỗi "TIMESTAMP(NANOS)" kèm dòng
"dùng pyarrow chuyển timestamp ns -> us" thì đó là đường dự phòng đang hoạt động, không phải lỗi.

## Bước 4 – đo chất lượng dữ liệu trước khi làm sạch (profiling)
```bash
python jobs/02a_profile_raw.py                  # 1 lượt quét, ~35 luật kiểm tra + null + phân vị + tần suất cột phân loại
python jobs/02a_profile_raw.py --duplicates     # thêm: đếm dòng trùng theo từng tháng (chậm hơn, tốn RAM hơn)
```
Ghi vào `results/02a_profile_raw/`: `dq_profile.csv`, `dq_profile_by_month.csv`, `dq_nulls.csv`, `dq_quantiles.csv`, `dq_categorical.csv` (và `dq_duplicates.csv`).
Một dòng có thể vi phạm nhiều luật nên các số không cộng được với nhau.

## Bước 5 – làm sạch và chuẩn hoá (Processing)
```bash
python jobs/02_clean.py                                   # cả 12 tháng, xử lý từng tháng
python jobs/02_clean.py --start 2024-01 --end 2024-01     # thử trước 1 tháng
python jobs/02_clean.py --max-distance 150 --max-speed 70  # đổi ngưỡng (mặc định: 6 giờ, 100 dặm, 80 mph, fare tối thiểu 3 USD; ghi vào clean_params.json)
```
Kết quả: `data/clean/yellow/` (đạt), `data/clean/yellow_rejected/` (bị loại, cột `reject_reasons`),
và `results/02_clean/`: `clean_summary_by_month.csv`, `rule_counts.csv`, `after_quantiles.csv`, `clean_params.json`.
Luật và lý do chọn: xem docstring đầu file `jobs/02_clean.py` và `NOTES.md`.

### Bước 2 (bản 2) – Làm sạch với luật đã kiểm chứng
```
python jobs/02_clean_v2.py          # ghi data/clean_v2 và results/02_clean_v2/ (không đè bản 1)
```
Khác bản 1: quãng đường 0 được giữ + cờ, thời lượng ≤ 6 giờ, tốc độ ≤ 80 mph, thêm luật fare < 3 USD. Lý do: NOTES.md (2026-10-20).
Có thể xoá `data/clean` của bản 1 sau khi xong để tiết kiệm đĩa.

### Bước 3 – Tổng hợp KPI bằng Spark SQL
```
python jobs/03_aggregate.py                      # đọc data/clean_v2, chạy mọi sql/kpi_*.sql
python jobs/03_aggregate.py --only kpi_daily     # chạy một KPI
```
Ghi `data/processed/<kpi>/` (Parquet) và `results/03_aggregate/<kpi>.csv` + `aggregate_log.csv` (thời gian, kiểm tổng chuyến).
KPI: theo ngày, giờ x thứ, khu vực đón, 100 cặp đón-trả đông nhất, cơ cấu thanh toán theo tháng, chất lượng theo nhà cung cấp.

### Bước 4 – Benchmark (thực nghiệm hiệu năng)
```
python jobs/04_benchmark.py --quick                  # thử nhanh (1 lượt đo, cỡ nhỏ)
python jobs/04_benchmark.py                          # đủ: scale, shuffle, format, cores, pruning (mỗi cấu hình 1 khởi động + 3 lượt đo)
python jobs/04_benchmark.py --experiments scale format
python jobs/04_benchmark.py --capture-ui             # TỰ ĐỘNG lưu Spark UI sau mỗi thí nghiệm (khuyên dùng)
python jobs/04_benchmark.py --pause                  # (cách cũ) dừng để chụp tay http://localhost:4040
```
Chụp ảnh tự động cần (một lần, ~150 MB): `pip install playwright` rồi `python -m playwright install chromium`; không cài thì vẫn lưu đủ JSON/CSV.
Ghi `results/04_benchmark/`: `bench_runs.csv` (từng lượt), `bench_summary.csv` (median/min/max, dòng/giây), `env.json`, và `spark_ui/<thí nghiệm>/` (jobs.png, stages.png, sql.png, sql_query_NNN.png = sơ đồ từng truy vấn, stage_N.png, *.json, *_summary.csv).
Thí nghiệm `format` tạo bản CSV 3 tháng ở `data/bench/` (cần ~1–2 GB đĩa; xoá được sau khi đo).

### Gom kết quả để gửi soát
```
python scripts/collect_results.py                    # -> handoff/ + handoff_YYYYmmdd_HHMM.zip (csv/json nhỏ + NOTES.md)
python scripts/collect_results.py --png-main         # kèm ảnh tab tổng của Spark UI
python scripts/collect_results.py --steps 04_benchmark
```
Chỉ sao chép, không sửa/xoá file trong `results/`. `handoff/` có `MANIFEST.txt` liệt kê file đã gom và file bị bỏ vì quá lớn.

### Bước 2b – Kiểm chứng luật làm sạch (chạy SAU 02_clean)
```
python jobs/02b_verify_rules.py                          # cả 12 tháng (đọc nhiều lượt, có thể vài phút đến chục phút)
python jobs/02b_verify_rules.py --start 2024-01 --end 2024-03   # thử nhanh
```
Kết quả trong `results/02b_verify_rules/` (v0…v7, mỗi file trả lời một câu hỏi, xem docstring đầu file).
Thử trên dữ liệu giả: `python jobs/02b_verify_rules.py --lake data/fake_lake --clean data/fake_clean --results data/fake_results_v`

### Bước 2c – Chẩn đoán 2 điểm còn mở (chạy SAU 02b)
```
python jobs/02c_diagnose_open_items.py
```
Kết quả: `results/02c_diagnose_open_items/` (d1: vì sao total_amount lệch tổng các khoản ở ~28,6% dòng; d2: nhóm fare âm nhưng total dương).

## Kiểm thử bằng dữ liệu giả (không dùng cho báo cáo)
```bash
python tests/make_fake_tlc.py --out data/fake_raw --months 2024-01 2024-02 2025-01 --rows 200000
python jobs/01_ingest_to_lake.py --raw data/fake_raw --lake data/fake_lake --results data/fake_results
python jobs/02_clean.py --lake data/fake_lake --clean data/fake_clean --results data/fake_results
```

## Cấu trúc
```
scripts/download_data.py      tải dữ liệu theo loại xe và khoảng tháng
scripts/setup_windows_hadoop.py  (Windows) tải winutils.exe + hadoop.dll
jobs/common.py                cấu hình Spark dùng chung (local[2], 3g, 16 shuffle partitions)
jobs/00_check_env.py          kiểm tra môi trường
scripts/inspect_raw.py        kiểm tra schema/số dòng file thô
jobs/01_ingest_to_lake.py     ghi Parquet phân vùng + kiểm chứng số dòng
tests/make_fake_tlc.py        sinh dữ liệu giả để thử mã
jobs/02a_profile_raw.py       đo lỗi dữ liệu thật -> chọn luật làm sạch
jobs/02_clean.py              làm sạch bản 1: data/clean (đạt) + yellow_rejected (loại, có lý do)
jobs/02_clean_v2.py           làm sạch bản 2 (luật mới) -> data/clean_v2, results/02_clean_v2/
scripts/collect_results.py    gom kết quả vào handoff/ + zip để gửi soát
jobs/ui_capture.py            chụp Spark UI tự động (REST API + ảnh Playwright)
jobs/04_benchmark.py          thực nghiệm hiệu năng (results/04_benchmark/)
jobs/03_aggregate.py          KPI bằng Spark SQL (sql/kpi_*.sql) -> data/processed, results/03_aggregate/
jobs/02b_verify_rules.py      kiểm chứng luật làm sạch bằng số liệu thật (results/02b_verify_rules/)
jobs/02c_diagnose_open_items.py  chẩn đoán total_amount lệch + nhóm fare<0 total>0 (results/02c_diagnose_open_items/)
jobs/03_aggregate.py          (kế tiếp) KPI
benchmarks/run_benchmark.py   (kế tiếp) thực nghiệm
data/{raw,lake,clean,processed}  dữ liệu (không đưa lên Git)
results/<tên-script>/          kết quả của từng bước (xem mục dưới)
scripts/reorganize_results.py chuyển file kết quả cũ (nằm phẳng) vào đúng thư mục bước
docs/  report/
```

## Quy ước thư mục kết quả
Mỗi script ghi kết quả vào thư mục riêng tên trùng tên script (bỏ đuôi `.py`):
```
results/
├── download_data/       download_log.csv
├── 00_check_env/        env_check.json
├── inspect_raw/         schema_inspection.csv, raw_files_summary.csv
├── 01_ingest_to_lake/   ingest_log.csv
├── 02a_profile_raw/     dq_*.csv
└── (các bước sau: 02_clean/, 03_aggregate/, run_benchmark/ ...)
```
Nếu đã có file kết quả cũ nằm phẳng trong `results/`, chạy `python scripts/reorganize_results.py` một lần để chuyển vào đúng chỗ.

## Nguồn dữ liệu
NYC Taxi & Limousine Commission – [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
TLC nêu rõ không đảm bảo độ chính xác của dữ liệu; báo cáo cần ghi lại tuyên bố này.


## Bước 5 – Vẽ biểu đồ cho báo cáo

```powershell
python jobs/05_make_figures.py            # đọc results/, ghi results/05_make_figures/
python jobs/05_make_figures.py --only fig03 --dpi 300
```
Sinh 11 hình PNG (fig01…fig11: benchmark scale/shuffle/format/pruning/cores, KPI ngày/giờ-thứ/top khu/thanh toán, chất lượng theo nhà cung cấp, luật làm sạch v1 vs v2) và `figures_index.csv`. Mọi con số trong tiêu đề đều tính từ CSV kết quả.

## Bước 4b – Đo RAM đỉnh: Pandas 12 tháng so với Spark

```powershell
pip install psutil                                   # (đã có trong requirements.txt)
python jobs/04b_pandas_memory.py                     # 1/3/6/12 tháng × pandas_narrow / pandas_wide / spark × 3 lượt
python jobs/04b_pandas_memory.py --sizes 12 --limit-gb 6    # giả lập máy chỉ còn 6 GB trống
```
Mỗi lượt chạy trong một tiến trình con riêng; tiến trình cha lấy mẫu RSS của cả cây tiến trình (kể cả JVM) mỗi 50 ms. Kết quả: `results/04b_pandas_memory/mem_summary.csv`. **Đóng ứng dụng nặng khi đo; hết RAM có thể làm máy chậm/đơ vài phút.**

## Bước 6 – Bộ dữ liệu thứ hai: HVFHV (Uber/Lyft) 1–2 tháng

Mục đích: chứng minh pipeline chạy được trên bộ lớn hơn (~20 triệu dòng/tháng, gấp khoảng 6 lần Yellow một tháng) và so RAM Pandas với Spark.
```powershell
python scripts/download_data.py --type fhvhv --start 2024-01 --end 2024-02 --no-zones   # mỗi file vài trăm MB
python jobs/01_ingest_to_lake.py --type fhvhv                  # -> data/lake/fhvhv (phân vùng year/month)
python jobs/06_profile_hvfhv.py                                # hồ sơ chất lượng -> results/06_profile_hvfhv/
python jobs/04b_pandas_memory.py --dataset fhvhv --sizes 1 2   # RAM/thời gian -> results/04b_pandas_memory/fhvhv/
```
Thử code trước bằng dữ liệu giả: `python tests/make_fake_fhvhv.py --out data/fake_raw` rồi thêm `--raw data/fake_raw --lake data/fake_lake` cho bước ingest. HVFHV được đọc thẳng từ lake (chưa làm sạch); 06 chỉ đo và mô tả bất thường.

**Chẩn đoán HVFHV (06b):** `python jobs/06b_diagnose_hvfhv.py` → `results/06b_diagnose_hvfhv/` (null / "đón trước giờ gọi" / "driver_pay > 1,5× giá cước" chia theo hãng × tháng × ngày × nhóm giá × tip/toll/phí sân bay).


## Docker (môi trường tái lập)

Cần Docker Desktop (Windows: bật WSL2). Image gồm Python 3.11 + Java 17 + PySpark 3.5.3 + Pandas/PyArrow/Matplotlib; **dữ liệu và kết quả không nằm trong image** mà gắn volume từ thư mục dự án.
```powershell
docker compose build
docker compose run --rm app          # demo trọn pipeline trên dữ liệu GIẢ (~600 nghìn dòng) -> demo_out/ (log, CSV, PNG); không đụng data/ và results/ thật
# chạy từng bước trên dữ liệu thật (data/, results/ trên máy bạn):
docker compose run --rm app python scripts/download_data.py --start 2024-01 --end 2024-12
docker compose run --rm app python jobs/01_ingest_to_lake.py
docker compose run --rm app python jobs/02_clean_v2.py
docker compose run --rm app python jobs/03_aggregate.py
docker compose run --rm app python jobs/05_make_figures.py
# giới hạn RAM container (mặc định 6g):  $env:APP_MEM="4g"; docker compose run --rm app
```
Docker Desktop mặc định cấp cho WSL2 khoảng một nửa RAM máy; nếu Spark bị `Killed`, tăng trong `%UserProfile%\.wslconfig` (`memory=10GB`). Số đo benchmark trong container **khác** chạy trực tiếp trên Windows (không so trực tiếp). Chạy không cần Docker: xem các bước phía trên.
Thử code ngoài Docker: `bash scripts/docker_demo.sh` (cần `PYTHON` trỏ tới Python có pyspark, vd `PYTHON=python bash ...` trong Git Bash/WSL).

## Sơ đồ cho báo cáo

```powershell
python scripts/make_diagrams.py        # -> docs/diagrams/diagram01_data_flow.(png|svg), diagram02_clean_policy.(png|svg)
```
Sơ đồ luồng dữ liệu (nguồn → raw → lake → clean_v2 → processed, kèm script từng bước và nhánh đo đạc) và sơ đồ chính sách làm sạch (loại / sửa / giữ). Số dòng lấy từ `results/` nếu có; file SVG chèn được vào Word/LaTeX mà không vỡ nét.

## Chụp ảnh Spark UI (jobs/04c_capture_spark_ui.py)
Spark UI chỉ tồn tại khi Spark còn chạy, nên ảnh phải chụp lúc chạy. Script này chạy lại MỘT lượt các truy vấn đại diện (E2 pruning, E3 shuffle, E1 format; tuỳ chọn E5 scale, E4 cores) rồi chụp ngay, không cần chạy lại cả benchmark.
```
pip install playwright
python -m playwright install chromium          # hoặc đặt CHROMIUM_PATH tới chrome.exe có sẵn
python jobs/04c_capture_spark_ui.py            # pruning + shuffle + format
python jobs/04c_capture_spark_ui.py --only all # cả 5 thí nghiệm
python jobs/04c_capture_spark_ui.py --no-screenshots   # chỉ JSON/CSV
```
Cần `data/clean_v2`; thí nghiệm format cần `data/bench/yellow_csv_3m` (tạo bởi `python jobs/04_benchmark.py --experiments format`).
Kết quả: `results/04_benchmark/spark_ui_png/<thí nghiệm>/` (ảnh các tab, ảnh từng truy vấn SQL có số file/partition/byte đọc, 5 stage lâu nhất, JSON/CSV). Ảnh đáng đưa vào báo cáo: `pruning/sql_query_*.png` (số `number of partitions read`, `size of files read`), `format/sql_query_*.png`, `shuffle_*/stages.png`.

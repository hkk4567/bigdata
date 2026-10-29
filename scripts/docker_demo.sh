#!/usr/bin/env bash
# Demo tái lập trọn pipeline trên dữ liệu GIẢ (~600 nghìn dòng): sinh dữ liệu -> ingest -> làm sạch v2 -> KPI -> benchmark nhanh -> biểu đồ.
# Chạy trong một BẢN SAO tạm của mã nguồn nên KHÔNG ghi đè data/ và results/ thật của bạn.
# Kết quả demo: demo_out/ (log, CSV, PNG).   Dùng:  docker compose run --rm app      hoặc:  bash scripts/docker_demo.sh
# Biến: PYTHON (mặc định python), ROWS (dòng/tháng, mặc định 200000), OUT (mặc định demo_out)
set -euo pipefail
PY="${PYTHON:-python}"
ROWS="${ROWS:-200000}"
SRC="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${OUT:-$SRC/demo_out}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

mkdir -p "$OUT"
for d in jobs sql scripts tests; do cp -r "$SRC/$d" "$WORK/$d"; done
cp "$SRC/requirements.txt" "$WORK/"
cd "$WORK"
mkdir -p data/raw
LOG="$OUT/demo_log.txt"
: > "$LOG"
step() { echo; echo "=== $* ===" | tee -a "$LOG"; }

step "0. Kiểm tra môi trường"
$PY jobs/00_check_env.py 2>&1 | tee -a "$LOG"
step "1. Sinh dữ liệu giả (3 tháng x $ROWS dòng) + bảng zone"
$PY tests/make_fake_tlc.py --out data/raw --months 2024-01 2024-02 2024-03 --rows "$ROWS" 2>&1 | tee -a "$LOG"
step "2. Ingest vào Data Lake (Parquet phân vùng year/month)"
$PY jobs/01_ingest_to_lake.py 2>&1 | grep -v WARN | tee -a "$LOG"
step "3. Làm sạch v2 (reject / repair / keep)"
$PY jobs/02_clean_v2.py 2>&1 | grep -v WARN | tee -a "$LOG"
step "4. Tổng hợp KPI bằng Spark SQL"
$PY jobs/03_aggregate.py 2>&1 | grep -v WARN | tee -a "$LOG"
step "5. Benchmark nhanh (1 lượt, cỡ nhỏ)"
$PY jobs/04_benchmark.py --quick --experiments scale format 2>&1 | grep -v WARN | tee -a "$LOG"
step "6. Đo RAM Pandas so với Spark (1 và 2 tháng)"
$PY jobs/04b_pandas_memory.py --sizes 1 2 --runs 1 2>&1 | grep -v WARN | tee -a "$LOG"
step "7. Vẽ biểu đồ (chỉ các hình có đủ dữ liệu trong demo; đủ 15 hình cần kết quả thật)"
$PY jobs/05_make_figures.py --only fig01 fig03 fig06 fig07 fig10 fig12 2>&1 | tee -a "$LOG"

rm -rf "$OUT/results" "$OUT/processed"
cp -r results "$OUT/results"
[ -d data/processed ] && cp -r data/processed "$OUT/processed" || true
echo; echo "XONG. Kết quả demo: $OUT  (log: demo_log.txt)" | tee -a "$LOG"

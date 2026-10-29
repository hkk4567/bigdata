#!/usr/bin/env python3
"""Storage: nạp file Parquet thô (data/raw) vào Data Lake (data/lake) dạng Parquet phân vùng.

Thiết kế:
  - Đọc TỪNG file tháng riêng (tránh lỗi gộp schema), chuẩn hoá tên cột về chữ thường,
    ép về schema thống nhất (cột thiếu -> null, kiểu khác -> cast). Xem YELLOW_SCHEMA.
  - Phân vùng theo year/month lấy từ TÊN FILE (không lấy từ giờ đón khách vì dữ liệu thật có
    ngày sai, ví dụ năm 2002). Bước này KHÔNG làm sạch dữ liệu – việc đó thuộc 02_clean.py.
  - Idempotent: chạy lại một tháng chỉ ghi đè đúng phân vùng đó (dynamic partition overwrite).
  - Kiểm chứng: so số dòng đầu vào (metadata pyarrow) với số dòng đọc lại từ lake -> results/01_ingest_to_lake/ingest_log.csv.
  - Bảng taxi_zone_lookup.csv -> data/lake/dim_zone (Parquet).

Chạy:
  python jobs/01_ingest_to_lake.py                       # mọi file yellow trong data/raw
  python jobs/01_ingest_to_lake.py --start 2024-01 --end 2024-03
  python jobs/01_ingest_to_lake.py --type fhvhv          # loại khác: giữ nguyên cột, chỉ đổi tên về chữ thường
"""
import argparse
import csv
import datetime as dt
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, get_spark, results_dir  # noqa: E402

# (tên cột chữ thường, kiểu Spark SQL) – schema đích của lớp lake cho Yellow Taxi
YELLOW_SCHEMA = [
    ("vendorid", "int"), ("tpep_pickup_datetime", "timestamp"), ("tpep_dropoff_datetime", "timestamp"),
    ("passenger_count", "bigint"), ("trip_distance", "double"), ("ratecodeid", "bigint"),
    ("store_and_fwd_flag", "string"), ("pulocationid", "int"), ("dolocationid", "int"),
    ("payment_type", "bigint"), ("fare_amount", "double"), ("extra", "double"), ("mta_tax", "double"),
    ("tip_amount", "double"), ("tolls_amount", "double"), ("improvement_surcharge", "double"),
    ("total_amount", "double"), ("congestion_surcharge", "double"), ("airport_fee", "double"),
    ("cbd_congestion_fee", "double"),
]
SCHEMAS = {"yellow": YELLOW_SCHEMA}


def ym_from_name(p: Path):
    m = re.search(r"_(\d{4})-(\d{2})\.parquet$", p.name)
    return (int(m.group(1)), int(m.group(2))) if m else None


def dir_bytes(d: Path) -> int:
    return sum(f.stat().st_size for f in d.rglob("*") if f.is_file())


def read_month(spark, path: Path):
    """Đọc 1 file; nếu Spark không đọc được (vd. timestamp nanosecond) thì chuyển qua pyarrow rồi đọc lại."""
    try:
        df = spark.read.parquet(path.as_uri())
        df.schema  # ép đọc schema ngay để lỗi nổi lên ở đây
        return df, False, None
    except Exception as e:  # noqa: BLE001
        print(f"  Spark không đọc trực tiếp được ({type(e).__name__}); dùng pyarrow chuyển timestamp ns -> us ...")
        import pyarrow as pa
        import pyarrow.parquet as pq

        tbl = pq.read_table(path)
        fixed = [pa.field(f.name, pa.timestamp("us")) if pa.types.is_timestamp(f.type) else f for f in tbl.schema]
        tbl = tbl.cast(pa.schema(fixed))
        tmp = Path(tempfile.mkdtemp(prefix="bd_fix_"))
        pq.write_table(tbl, tmp / path.name)
        return spark.read.parquet((tmp / path.name).as_uri()), True, tmp


def conform(df, ttype: str):
    """Chuẩn hoá tên cột (chữ thường) và ép về schema đích nếu có."""
    from pyspark.sql import functions as F

    df = df.toDF(*[c.lower() for c in df.columns])
    schema = SCHEMAS.get(ttype)
    if not schema:
        return df
    have = set(df.columns)
    cols = [(F.col(n).cast(t) if n in have else F.lit(None).cast(t)).alias(n) for n, t in schema]
    extra = sorted(have - {n for n, _ in schema})
    if extra:
        print(f"  cột ngoài schema đích (bỏ): {extra}")
    return df.select(*cols)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--type", default="yellow")
    ap.add_argument("--raw", default=str(ROOT / "data" / "raw"))
    ap.add_argument("--lake", default=str(ROOT / "data" / "lake"))
    ap.add_argument("--results", default=None, help="mặc định: results/01_ingest_to_lake")
    ap.add_argument("--start", help="YYYY-MM (tuỳ chọn)")
    ap.add_argument("--end", help="YYYY-MM (tuỳ chọn)")
    ap.add_argument("--cores", type=int, default=2)
    ap.add_argument("--driver-mem", default="3g")
    a = ap.parse_args()

    raw, lake, res = Path(a.raw), Path(a.lake), results_dir(__file__, a.results)
    files = []
    for p in sorted(raw.glob(f"{a.type}_tripdata_*.parquet")):
        ym = ym_from_name(p)
        if not ym:
            continue
        key = f"{ym[0]:04d}-{ym[1]:02d}"
        if (a.start and key < a.start) or (a.end and key > a.end):
            continue
        files.append((p, ym))
    if not files:
        print(f"Không có file {a.type}_tripdata_*.parquet phù hợp trong {raw}. Chạy scripts/download_data.py trước.")
        return 1

    import pyarrow.parquet as pq
    from pyspark.sql import functions as F

    spark = get_spark("ingest_to_lake", cores=a.cores, driver_mem=a.driver_mem)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    target = lake / a.type
    log_path = res / "ingest_log.csv"
    new_log = not log_path.exists()
    bad = 0

    with open(log_path, "a", newline="", encoding="utf-8") as lf:
        w = csv.writer(lf)
        if new_log:
            w.writerow(["timestamp", "type", "year", "month", "rows_in", "rows_out", "status",
                        "raw_bytes", "lake_bytes", "seconds", "pyarrow_fallback"])
        for p, (y, m) in files:
            print(f"[{p.name}] -> {target}/year={y}/month={m}")
            t0 = time.time()
            rows_in = pq.ParquetFile(p).metadata.num_rows
            df, fallback, tmp = read_month(spark, p)
            out = conform(df, a.type).withColumn("source_file", F.lit(p.name)) \
                .withColumn("year", F.lit(y).cast("int")).withColumn("month", F.lit(m).cast("int"))
            out.write.mode("overwrite").partitionBy("year", "month").parquet(target.as_uri())
            part_dir = target / f"year={y}" / f"month={m}"  # month là int nên thư mục là month=1, month=12 ...
            # Đọc lại qua bộ lọc phân vùng (không ghép "year=..." vào URI vì dấu "=" bị mã hoá thành %3D)
            rows_out = (spark.read.parquet(target.as_uri())
                        .where((F.col("year") == y) & (F.col("month") == m)).count())
            if tmp:
                shutil.rmtree(tmp, ignore_errors=True)
            secs = time.time() - t0
            status = "OK" if rows_in == rows_out else "MISMATCH"
            bad += status != "OK"
            lake_b = dir_bytes(part_dir)
            print(f"  rows_in={rows_in:,} rows_out={rows_out:,} {status}  {p.stat().st_size/1e6:.1f}MB -> {lake_b/1e6:.1f}MB  {secs:.1f}s")
            w.writerow([dt.datetime.now().isoformat(timespec="seconds"), a.type, y, m, rows_in, rows_out, status,
                        p.stat().st_size, lake_b, f"{secs:.2f}", fallback])
            lf.flush()

    zones = raw / "taxi_zone_lookup.csv"
    if zones.exists():
        z = spark.read.option("header", True).option("inferSchema", True).csv(zones.as_uri())
        z = z.toDF(*[c.lower() for c in z.columns])
        z.coalesce(1).write.mode("overwrite").parquet((lake / "dim_zone").as_uri())
        print(f"[dim_zone] {z.count()} dòng -> {lake / 'dim_zone'}")
    else:
        print("(không có taxi_zone_lookup.csv – bỏ qua dim_zone)")

    spark.stop()
    print(f"Xong: {len(files)} tháng, {bad} lỗi. Log: {log_path}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())

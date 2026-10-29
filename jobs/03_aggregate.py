#!/usr/bin/env python3
"""Processing: tính KPI bằng Spark SQL trên dữ liệu đã làm sạch (data/clean_v2) -> data/processed/<kpi>/ (Parquet) + CSV nhỏ.

Mỗi truy vấn nằm ở một file sql/kpi_*.sql (view `trips` = dữ liệu sạch, view `zones` = dim_zone).
Thêm KPI mới = thêm một file sql/kpi_xxx.sql. Ghi: data/processed/<tên>/ và results/03_aggregate/<tên>.csv,
nhật ký thời gian từng truy vấn: results/03_aggregate/aggregate_log.csv.
Kiểm chứng: với các KPI phân hoạch toàn bộ chuyến, tổng cột trips phải bằng số dòng dữ liệu sạch.

Chạy:  python jobs/03_aggregate.py
       python jobs/03_aggregate.py --only kpi_daily kpi_zone_pickup
       python jobs/03_aggregate.py --start 2024-01 --end 2024-03
"""
import argparse
import csv
import datetime as dt
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, get_spark, results_dir  # noqa: E402

PARTITIONS_ALL_TRIPS = {"kpi_daily", "kpi_hourly_dow", "kpi_zone_pickup", "kpi_payment_month", "kpi_vendor_dq"}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--type", default="yellow")
    ap.add_argument("--lake", default=str(ROOT / "data" / "lake"))
    ap.add_argument("--clean", default=str(ROOT / "data" / "clean_v2"))
    ap.add_argument("--processed", default=str(ROOT / "data" / "processed"))
    ap.add_argument("--sql-dir", default=str(ROOT / "sql"))
    ap.add_argument("--results", default=None, help="mặc định: results/03_aggregate")
    ap.add_argument("--only", nargs="*", help="chỉ chạy các KPI này (tên file không có .sql)")
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--cores", type=int, default=2)
    ap.add_argument("--driver-mem", default="3g")
    a = ap.parse_args()

    import pandas as pd
    from pyspark.sql import functions as F

    res = results_dir(__file__, a.results)
    spark = get_spark("aggregate", cores=a.cores, driver_mem=a.driver_mem)
    trips = spark.read.parquet((Path(a.clean) / a.type).as_uri())
    ym = F.col("year") * 100 + F.col("month")
    if a.start:
        trips = trips.where(ym >= int(a.start.replace("-", "")))
    if a.end:
        trips = trips.where(ym <= int(a.end.replace("-", "")))
    trips.createOrReplaceTempView("trips")
    spark.read.parquet((Path(a.lake) / "dim_zone").as_uri()).createOrReplaceTempView("zones")
    n_clean = trips.count()
    print(f"Dữ liệu sạch: {n_clean:,} dòng")

    files = sorted(Path(a.sql_dir).glob("kpi_*.sql"))
    if a.only:
        files = [f for f in files if f.stem in a.only]
    if not files:
        print("Không tìm thấy file sql/kpi_*.sql phù hợp.")
        return 1
    out_root = Path(a.processed)
    bad = 0
    with open(res / "aggregate_log.csv", "w", newline="", encoding="utf-8") as lf:
        w = csv.writer(lf)
        w.writerow(["timestamp", "kpi", "rows_out", "total_trips", "check", "seconds"])
        for f in files:
            t0 = time.time()
            q = f.read_text(encoding="utf-8").strip().rstrip(";")
            df = spark.sql(q)
            df.write.mode("overwrite").parquet((out_root / f.stem).as_uri())
            out = spark.read.parquet((out_root / f.stem).as_uri())
            pdf = pd.DataFrame([r.asDict() for r in out.collect()])
            pdf.to_csv(res / f"{f.stem}.csv", index=False, encoding="utf-8")
            total, check = "", "n/a"
            if f.stem in PARTITIONS_ALL_TRIPS and "trips" in pdf:
                total = int(pdf["trips"].sum())
                check = "OK" if total == n_clean else "MISMATCH"
                bad += check != "OK"
            secs = time.time() - t0
            print(f"[{f.stem}] {len(pdf):,} dòng | tổng chuyến {total if total != '' else '-'} | kiểm {check} | {secs:.1f}s")
            w.writerow([dt.datetime.now().isoformat(timespec="seconds"), f.stem, len(pdf), total, check, f"{secs:.2f}"])
            lf.flush()
    spark.stop()
    print(f"\nKết quả: {res}\nParquet: {out_root}\n{'OK – mọi KPI phân hoạch khớp số dòng dữ liệu sạch.' if not bad else f'CẢNH BÁO: {bad} KPI lệch tổng.'}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())

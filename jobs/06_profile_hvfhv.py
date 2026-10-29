#!/usr/bin/env python3
"""Hồ sơ dữ liệu HVFHV (High Volume For-Hire Vehicle: Uber/Lyft) trong data/lake/fhvhv, KHÔNG làm sạch.

Mục đích (mục 3 + 6 báo cáo): cho thấy pipeline xử lý được bộ dữ liệu thứ hai, lớn hơn (~19-20 triệu dòng/tháng, nhiều gấp ~6 lần Yellow)
và mô tả chất lượng dữ liệu để biết cần luật làm sạch nào (nếu muốn làm tiếp). Chỉ ĐO và MÔ TẢ.

Đầu ra (results/06_profile_hvfhv/):
  hv_profile_by_month.csv    số dòng, thời gian đón sớm/muộn nhất, số dòng ngoài tháng, trung bình các đại lượng chính
  hv_anomalies_by_month.csv  số dòng: miles<=0, time<=0, fare<=0, driver_pay<0, tips<0, dropoff<pickup, pickup ngoài tháng, thời gian chờ > 1 giờ ...
  hv_license_mix.csv         tỉ lệ chuyến theo hãng (HV0002 Juno, HV0003 Uber, HV0004 Via, HV0005 Lyft; tên theo data dictionary TLC)
  hv_nulls.csv               số null theo cột
  hv_quantiles.csv           phân vị (p1, p50, p99, max) của trip_miles, trip_time, base_passenger_fare, driver_pay, tips, thời gian chờ
Chạy:  python jobs/06_profile_hvfhv.py            (cần đã chạy: scripts/download_data.py --type fhvhv ... và 01_ingest_to_lake.py --type fhvhv)
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, get_spark, results_dir  # noqa: E402

LICENSES = {"HV0002": "Juno", "HV0003": "Uber", "HV0004": "Via", "HV0005": "Lyft"}  # theo data dictionary HVFHV của TLC


def save_csv(df, path):
    """Ghi DataFrame Spark nhỏ ra CSV bằng module csv (không phụ thuộc pandas/distutils của pyspark)."""
    import csv
    rows = df.collect()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(df.columns)
        for r in rows:
            w.writerow(list(r))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lake", default=str(ROOT / "data" / "lake"))
    ap.add_argument("--results", default=None)
    ap.add_argument("--cores", type=int, default=2)
    ap.add_argument("--driver-mem", default="3g")
    a = ap.parse_args()

    from pyspark.sql import functions as F
    res = results_dir(__file__, a.results)
    spark = get_spark("profile_hvfhv", cores=a.cores, driver_mem=a.driver_mem)
    src = Path(a.lake) / "fhvhv"
    df = spark.read.parquet(src.as_uri())
    def epoch(c):  # lake có thể lưu TIMESTAMP_NTZ: ép về timestamp trước khi đổi sang giây (hiệu hai mốc không phụ thuộc múi giờ)
        return F.col(c).cast("timestamp").cast("long")
    # các đại lượng dẫn xuất (đơn vị: trip_time là giây, trip_miles là dặm)
    df = (df.withColumn("wait_s", epoch("pickup_datetime") - epoch("request_datetime"))
            .withColumn("dur_s_ts", epoch("dropoff_datetime") - epoch("pickup_datetime"))
            .withColumn("pickup_month", F.month("pickup_datetime")).withColumn("pickup_year", F.year("pickup_datetime")))
    df.cache()
    total = df.count()
    print(f"Tổng {total:,} dòng, {len(df.columns)} cột")

    out = (df.groupBy("year", "month").agg(
        F.count(F.lit(1)).alias("rows"), F.min("pickup_datetime").alias("min_pickup"), F.max("pickup_datetime").alias("max_pickup"),
        F.sum(F.when((F.col("pickup_month") != F.col("month")) | (F.col("pickup_year") != F.col("year")), 1).otherwise(0)).alias("pickup_outside_month"),
        F.round(F.avg("trip_miles"), 3).alias("avg_miles"), F.round(F.avg("trip_time") / 60, 3).alias("avg_trip_min"),
        F.round(F.avg("base_passenger_fare"), 3).alias("avg_base_fare"), F.round(F.avg("driver_pay"), 3).alias("avg_driver_pay"),
        F.round(F.sum("base_passenger_fare"), 2).alias("sum_base_fare")).orderBy("year", "month"))
    save_csv(out, res / "hv_profile_by_month.csv")

    def cnt(cond, name):
        return F.sum(F.when(cond, 1).otherwise(0)).alias(name)
    an = (df.groupBy("year", "month").agg(
        F.count(F.lit(1)).alias("rows"), cnt(F.col("trip_miles") <= 0, "miles_le_0"), cnt(F.col("trip_time") <= 0, "time_le_0"),
        cnt(F.col("base_passenger_fare") <= 0, "fare_le_0"), cnt(F.col("driver_pay") < 0, "driver_pay_lt_0"), cnt(F.col("tips") < 0, "tips_lt_0"),
        cnt(F.col("dropoff_datetime") < F.col("pickup_datetime"), "dropoff_before_pickup"),
        cnt((F.col("pickup_month") != F.col("month")) | (F.col("pickup_year") != F.col("year")), "pickup_outside_month"),
        cnt(F.col("wait_s") < 0, "pickup_before_request"), cnt(F.col("wait_s") > 3600, "wait_gt_1h"),
        cnt(F.col("trip_time") > 6 * 3600, "time_gt_6h"), cnt(F.col("trip_miles") > 100, "miles_gt_100"),
        cnt((F.col("trip_time") > 0) & (F.col("trip_miles") / (F.col("trip_time") / 3600) > 80), "speed_gt_80mph"),
        cnt(F.abs(F.col("trip_time") - F.col("dur_s_ts")) > 60, "trip_time_vs_timestamps_gt_60s"),
        cnt(F.col("driver_pay") > F.col("base_passenger_fare") * 1.5, "driver_pay_gt_1_5x_fare")).orderBy("year", "month"))
    save_csv(an, res / "hv_anomalies_by_month.csv")

    mix = df.groupBy("hvfhs_license_num").agg(F.count(F.lit(1)).alias("rows")).orderBy(F.desc("rows")).collect()
    import pandas as pd
    pd.DataFrame([{"hvfhs_license_num": r[0], "company": LICENSES.get(r[0], "khác/không rõ"), "rows": r[1], "share_pct": round(r[1] / total * 100, 3)}
                  for r in mix]).to_csv(res / "hv_license_mix.csv", index=False, encoding="utf-8")

    nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns if c not in ("wait_s", "dur_s_ts", "pickup_month", "pickup_year")]).first().asDict()
    pd.DataFrame([{"column": k, "nulls": int(v or 0), "null_pct": round((v or 0) / total * 100, 4)} for k, v in nulls.items()]).to_csv(
        res / "hv_nulls.csv", index=False, encoding="utf-8")

    rows = []
    for c in ("trip_miles", "trip_time", "base_passenger_fare", "driver_pay", "tips", "wait_s"):
        q = df.approxQuantile(c, [0.01, 0.5, 0.99], 0.001)
        mx = df.agg(F.max(c)).first()[0]
        mn = df.agg(F.min(c)).first()[0]
        rows.append({"column": c, "min": mn, "p1": q[0], "p50": q[1], "p99": q[2], "max": mx})
    pd.DataFrame(rows).to_csv(res / "hv_quantiles.csv", index=False, encoding="utf-8")
    print(f"Xong -> {res}")


if __name__ == "__main__":
    main()

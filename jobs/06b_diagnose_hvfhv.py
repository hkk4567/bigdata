#!/usr/bin/env python3
"""Chẩn đoán 3 điều 06_profile_hvfhv.py để ngỏ (chỉ đọc data/lake/fhvhv, không ghi lake):

  H1  null của originating_base_num / on_scene_datetime (26,02%) nằm ở hãng nào?
  H2  "đón trước giờ gọi xe" (pickup < request, ~0,99%): theo hãng × tháng; mức lệch (giây); có đi kèm on_scene null không?
  H3  "tiền tài xế > 1,5 × giá cước" nhảy từ 0,40% (T1) lên 1,64% (T2): theo hãng × tháng, theo NGÀY (tìm ngày đổi),
      theo nhóm giá cước, và có phải do tip/toll/phí sân bay (driver_pay gồm cả các khoản này) hay không.

Đầu ra (results/06b_diagnose_hvfhv/):
  h1_nulls_by_license_month.csv     số dòng, null originating_base_num, null on_scene theo hãng × tháng
  h2_pickup_before_request.csv      theo hãng × tháng: dòng, số dòng đón trước gọi, %, trung vị / p99 độ lệch (giây), số dòng kèm on_scene null
  h3_ratio_by_license_month.csv     driver_pay/base_passenger_fare: trung bình và phân vị p50/p90/p99; % dòng > 1,5
  h3_ratio_daily.csv                % dòng > 1,5 theo NGÀY × hãng (để thấy ngày nhảy)
  h3_ratio_by_fare_bucket.csv       % dòng > 1,5 theo nhóm giá cước × hãng × tháng
  h3_ratio_by_extras.csv            % dòng > 1,5 theo (có tip? có toll? có phí sân bay?) × hãng × tháng
  h3_pay_minus_extras.csv           sau khi trừ tip + toll + phí sân bay khỏi driver_pay: % dòng > 1,5 (nếu giảm mạnh = nguyên nhân là khoản cộng thêm)
Chạy:  python jobs/06b_diagnose_hvfhv.py
"""
import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, get_spark, results_dir  # noqa: E402

LICENSES = {"HV0002": "Juno", "HV0003": "Uber", "HV0004": "Via", "HV0005": "Lyft"}


def save_csv(df, path):
    rows = df.collect()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(df.columns)
        for r in rows:
            w.writerow(list(r))
    print(f"  ghi {Path(path).name} ({len(rows)} dòng)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lake", default=str(ROOT / "data" / "lake"))
    ap.add_argument("--results", default=None)
    ap.add_argument("--cores", type=int, default=2)
    ap.add_argument("--driver-mem", default="3g")
    a = ap.parse_args()

    from pyspark.sql import functions as F
    res = results_dir(__file__, a.results)
    spark = get_spark("diagnose_hvfhv", cores=a.cores, driver_mem=a.driver_mem)
    df = spark.read.parquet((Path(a.lake) / "fhvhv").as_uri())
    ep = lambda c: F.col(c).cast("timestamp").cast("long")  # noqa: E731
    lic = F.col("hvfhs_license_num")
    company = F.when(lic == "HV0003", "Uber").when(lic == "HV0005", "Lyft").when(lic == "HV0002", "Juno").when(lic == "HV0004", "Via").otherwise(lic).alias("company")
    base = df.select(
        company, "month", F.to_date("pickup_datetime").alias("day"), "tips", "tolls", "airport_fee", "congestion_surcharge", "base_passenger_fare", "driver_pay",
        F.col("originating_base_num").isNull().alias("orig_null"), F.col("on_scene_datetime").isNull().alias("onscene_null"),
        (ep("pickup_datetime") - ep("request_datetime")).alias("wait_s"),
        F.when(F.col("base_passenger_fare") > 0, F.col("driver_pay") / F.col("base_passenger_fare")).alias("ratio"))
    base = base.withColumn("hi", F.col("ratio") > 1.5).cache()
    n = base.count()
    print(f"{n:,} dòng")

    i = lambda c: F.sum(F.col(c).cast("int"))  # noqa: E731
    pct = lambda c, d="rows": F.round(F.col(c) / F.col(d) * 100, 3)  # noqa: E731

    # H1
    h1 = (base.groupBy("company", "month").agg(F.count(F.lit(1)).alias("rows"), i("orig_null").alias("originating_base_num_null"), i("onscene_null").alias("on_scene_null"))
          .withColumn("orig_null_pct", pct("originating_base_num_null")).withColumn("on_scene_null_pct", pct("on_scene_null")).orderBy("company", "month"))
    save_csv(h1, res / "h1_nulls_by_license_month.csv")

    # H2
    neg = base.withColumn("neg", F.col("wait_s") < 0)
    h2 = (neg.groupBy("company", "month").agg(
        F.count(F.lit(1)).alias("rows"), i("neg").alias("pickup_before_request"),
        F.sum(F.when(F.col("neg") & F.col("onscene_null"), 1).otherwise(0)).alias("neg_and_on_scene_null"),
        F.percentile_approx(F.when(F.col("neg"), F.col("wait_s")), 0.5).alias("neg_wait_s_median"),
        F.percentile_approx(F.when(F.col("neg"), F.col("wait_s")), 0.01).alias("neg_wait_s_p1"),
        F.min("wait_s").alias("wait_s_min"), F.percentile_approx("wait_s", 0.5).alias("wait_s_median_all"))
        .withColumn("neg_pct", pct("pickup_before_request")).orderBy("company", "month"))
    save_csv(h2, res / "h2_pickup_before_request.csv")

    # H3
    pos = base.where(F.col("ratio").isNotNull())
    h3 = (pos.groupBy("company", "month").agg(
        F.count(F.lit(1)).alias("rows_fare_gt_0"), F.round(F.avg("ratio"), 4).alias("ratio_mean"),
        F.round(F.percentile_approx("ratio", 0.5), 4).alias("ratio_p50"), F.round(F.percentile_approx("ratio", 0.9), 4).alias("ratio_p90"),
        F.round(F.percentile_approx("ratio", 0.99), 4).alias("ratio_p99"), i("hi").alias("ratio_gt_1_5"))
        .withColumn("ratio_gt_1_5_pct", pct("ratio_gt_1_5", "rows_fare_gt_0")).orderBy("company", "month"))
    save_csv(h3, res / "h3_ratio_by_license_month.csv")

    daily = (pos.groupBy("day", "company").agg(F.count(F.lit(1)).alias("rows"), i("hi").alias("ratio_gt_1_5"), F.round(F.avg("ratio"), 4).alias("ratio_mean"))
             .withColumn("ratio_gt_1_5_pct", pct("ratio_gt_1_5")).orderBy("day", "company"))
    save_csv(daily, res / "h3_ratio_daily.csv")

    bucket = (F.when(F.col("base_passenger_fare") < 5, "1) <5").when(F.col("base_passenger_fare") < 10, "2) 5-10")
              .when(F.col("base_passenger_fare") < 20, "3) 10-20").when(F.col("base_passenger_fare") < 50, "4) 20-50").otherwise("5) >=50"))
    hb = (pos.withColumn("fare_bucket", bucket).groupBy("company", "month", "fare_bucket").agg(F.count(F.lit(1)).alias("rows"), i("hi").alias("ratio_gt_1_5"))
          .withColumn("ratio_gt_1_5_pct", pct("ratio_gt_1_5")).orderBy("company", "month", "fare_bucket"))
    save_csv(hb, res / "h3_ratio_by_fare_bucket.csv")

    ex = (pos.withColumn("has_tip", F.col("tips") > 0).withColumn("has_toll", F.col("tolls") > 0).withColumn("has_airport_fee", F.col("airport_fee") > 0)
          .groupBy("company", "month", "has_tip", "has_toll", "has_airport_fee").agg(F.count(F.lit(1)).alias("rows"), i("hi").alias("ratio_gt_1_5"))
          .withColumn("ratio_gt_1_5_pct", pct("ratio_gt_1_5")).orderBy("company", "month", "has_tip", "has_toll", "has_airport_fee"))
    save_csv(ex, res / "h3_ratio_by_extras.csv")

    adj = pos.withColumn("pay_core", F.col("driver_pay") - F.col("tips") - F.col("tolls") - F.col("airport_fee"))
    ha = (adj.withColumn("hi_core", F.col("pay_core") / F.col("base_passenger_fare") > 1.5).groupBy("company", "month")
          .agg(F.count(F.lit(1)).alias("rows"), i("hi").alias("ratio_gt_1_5"), i("hi_core").alias("ratio_core_gt_1_5"),
               F.round(F.avg(F.col("pay_core") / F.col("base_passenger_fare")), 4).alias("ratio_core_mean"))
          .withColumn("ratio_gt_1_5_pct", pct("ratio_gt_1_5")).withColumn("ratio_core_gt_1_5_pct", pct("ratio_core_gt_1_5")).orderBy("company", "month"))
    save_csv(ha, res / "h3_pay_minus_extras.csv")
    print(f"Xong -> {res}")


if __name__ == "__main__":
    main()

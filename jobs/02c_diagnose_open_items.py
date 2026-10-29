#!/usr/bin/env python3
"""Chẩn đoán 2 điểm còn mở sau 02b (chạy SAU 02_clean.py, trước khi sửa luật):

 D1  total_amount khác tổng các khoản ở ~28,6% dòng ĐẠT (v7)  -> chênh lệch là bao nhiêu, rơi vào nhóm nào?
     results/02c_diagnose_open_items/d1_total_diff_values.csv      các giá trị chênh lệch (làm tròn 0,01) phổ biến nhất
     results/02c_diagnose_open_items/d1_total_diff_by_group.csv    tỉ lệ lệch theo payment_type / vendor / ratecode / tháng / Flex
 D2  ~125,8 nghìn dòng chỉ bị luật FARE_NONPOSITIVE (fare < 0 nhưng total > 0, chủ yếu Flex Fare): có phải hoàn tiền không?
     results/02c_diagnose_open_items/d2_flex_negative_fare_profile.csv   thống kê các khoản tiền của nhóm này
     results/02c_diagnose_open_items/d2_flex_negative_fare_by_month.csv  số dòng theo tháng và tỉ lệ trên Flex Fare cùng tháng

Chạy:  python jobs/02c_diagnose_open_items.py   (thêm --start/--end để thử nhanh)
"""
import argparse
import sys
import time
from functools import reduce
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, get_spark, results_dir  # noqa: E402

PARTS = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
         "congestion_surcharge", "airport_fee", "cbd_congestion_fee"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--type", default="yellow")
    ap.add_argument("--lake", default=str(ROOT / "data" / "lake"))
    ap.add_argument("--clean", default=str(ROOT / "data" / "clean"))
    ap.add_argument("--results", default=None)
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--cores", type=int, default=2)
    ap.add_argument("--driver-mem", default="3g")
    a = ap.parse_args()

    import pandas as pd
    from pyspark.sql import functions as F

    res = results_dir(__file__, a.results)
    spark = get_spark("diagnose_open_items", cores=a.cores, driver_mem=a.driver_mem)
    ymc = F.col("year") * 100 + F.col("month")

    def load(path):
        d = spark.read.parquet(Path(path).as_uri())
        if a.start:
            d = d.where(ymc >= int(a.start.replace("-", "")))
        if a.end:
            d = d.where(ymc <= int(a.end.replace("-", "")))
        return d

    clean = load(Path(a.clean) / a.type)
    rej = load(Path(a.clean) / f"{a.type}_rejected")
    t0 = time.time()
    pdf = lambda rows: pd.DataFrame([r.asDict() for r in rows])  # noqa: E731

    # ---------- D1 ----------
    parts_sum = reduce(lambda x, y: x + y, [F.coalesce(F.col(c), F.lit(0.0)) for c in PARTS])
    diff = F.round(F.col("total_amount") - parts_sum, 2)
    c = clean.withColumn("diff", diff).withColumn("mismatch", F.abs(F.col("diff")) > 0.011)
    top = pdf(c.where("mismatch").groupBy("diff").count().orderBy(F.desc("count")).limit(30).collect())
    n_mis = c.where("mismatch").count()
    top["pct_of_mismatch"] = (top["count"] / n_mis * 100).round(2)
    top.to_csv(res / "d1_total_diff_values.csv", index=False)
    print(f"\n=== D1 – {n_mis:,} dòng lệch. Giá trị chênh (total − tổng các khoản) phổ biến nhất:")
    print(top.head(15).to_string(index=False))

    groups = {"payment_type": F.col("payment_type").cast("string"), "vendorid": F.col("vendorid").cast("string"),
              "ratecodeid": F.coalesce(F.col("ratecodeid").cast("string"), F.lit("null")), "month": F.col("month").cast("string"),
              "congestion_null": F.col("congestion_surcharge").isNull().cast("string"),
              "congestion_gt0": (F.coalesce(F.col("congestion_surcharge"), F.lit(0)) > 0).cast("string"),
              "airport_fee_null": F.col("airport_fee").isNull().cast("string")}
    frames = []
    for g, e in groups.items():
        d = pdf(c.groupBy(e.alias("value")).agg(F.count(F.lit(1)).alias("rows"),
                                                F.sum(F.col("mismatch").cast("int")).alias("mismatch_rows")).collect())
        d.insert(0, "group", g)
        frames.append(d)
    gdf = pd.concat(frames, ignore_index=True)
    gdf["mismatch_pct"] = (gdf["mismatch_rows"] / gdf["rows"] * 100).round(2)
    gdf.to_csv(res / "d1_total_diff_by_group.csv", index=False)
    print("\n=== D1 – tỉ lệ lệch theo nhóm")
    print(gdf.sort_values(["group", "value"]).to_string(index=False))

    # ---------- D2 ----------
    only = rej.where(F.col("reject_reasons") == "FARE_NONPOSITIVE")
    cols = PARTS + ["total_amount", "trip_distance", "trip_duration_s"]
    agg = [F.count(F.lit(1)).alias("rows"), F.avg(F.when(F.col("payment_type") == 0, 1).otherwise(0)).alias("share_flex"),
           F.avg(F.when(F.col("total_amount") > 0, 1).otherwise(0)).alias("share_total_gt0"),
           F.avg(F.when(F.col("trip_distance") > 0, 1).otherwise(0)).alias("share_distance_gt0")]
    for k in cols:
        agg += [F.avg(k).alias(f"avg_{k}"), F.expr(f"percentile_approx({k}, 0.5)").alias(f"p50_{k}")]
    prof = pdf(only.agg(*agg).collect()).T.reset_index()
    prof.columns = ["metric", "value"]
    prof["value"] = prof["value"].astype(float).round(3)
    prof.to_csv(res / "d2_flex_negative_fare_profile.csv", index=False)
    print("\n=== D2 – nhóm chỉ bị luật FARE_NONPOSITIVE (fare<=0 nhưng không bị luật khác bắt)")
    print(prof.to_string(index=False))

    m1 = pdf(only.groupBy("month").agg(F.count(F.lit(1)).alias("rows_only_fare_rule")).collect())
    flex_rows = pdf(load(Path(a.lake) / a.type).where(F.col("payment_type") == 0).groupBy("month").count().collect()).rename(columns={"count": "flex_rows_in_month"})
    if flex_rows.empty:
        flex_rows = pd.DataFrame({"month": [], "flex_rows_in_month": []})
    m1 = m1.merge(flex_rows, on="month", how="left").sort_values("month")
    m1["pct_of_flex_month"] = (m1["rows_only_fare_rule"] / m1["flex_rows_in_month"] * 100).round(2)
    m1.to_csv(res / "d2_flex_negative_fare_by_month.csv", index=False)
    print("\n=== D2 – theo tháng")
    print(m1.to_string(index=False))

    spark.stop()
    print(f"\nXong sau {(time.time() - t0) / 60:.1f} phút. Kết quả: {res}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Đo chất lượng dữ liệu trong lake TRƯỚC khi làm sạch (profiling) – căn cứ để chọn luật cho 02_clean.py.

Một lượt quét duy nhất (groupBy year, month) tính ~45 chỉ số cùng lúc, nên chạy nhanh trên 41 triệu dòng.
Đầu ra (results/02a_profile_raw/):
  dq_profile.csv           mã luật, mô tả, số dòng vi phạm, % trên tổng  (tổng toàn bộ dữ liệu)
  dq_profile_by_month.csv  cùng chỉ số theo từng tháng (dạng dài)
  dq_nulls.csv             số null từng cột
  dq_quantiles.csv         phân vị của giá, quãng đường, thời lượng, tổng tiền
  dq_categorical.csv       tần suất các cột phân loại (vendor, ratecode, payment_type, passenger_count, store_and_fwd_flag)
  dq_duplicates.csv        (chỉ khi --duplicates) số dòng trùng theo tháng

Chạy:
  python jobs/02a_profile_raw.py
  python jobs/02a_profile_raw.py --start 2024-01 --end 2024-03
  python jobs/02a_profile_raw.py --duplicates       # chậm hơn: đếm dòng trùng từng tháng
Một "dòng vi phạm" có thể thuộc nhiều luật, nên các số KHÔNG cộng được với nhau.
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, get_spark, results_dir  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--type", default="yellow")
    ap.add_argument("--lake", default=str(ROOT / "data" / "lake"))
    ap.add_argument("--results", default=None, help="mặc định: results/02a_profile_raw")
    ap.add_argument("--start", help="YYYY-MM")
    ap.add_argument("--end", help="YYYY-MM")
    ap.add_argument("--duplicates", action="store_true")
    ap.add_argument("--cores", type=int, default=2)
    ap.add_argument("--driver-mem", default="3g")
    a = ap.parse_args()

    import pandas as pd
    from pyspark.sql import functions as F

    lake, res = Path(a.lake), results_dir(__file__, a.results)
    spark = get_spark("profile_raw", cores=a.cores, driver_mem=a.driver_mem)
    df = spark.read.parquet((lake / a.type).as_uri())
    ym = F.col("year") * 100 + F.col("month")
    if a.start:
        df = df.where(ym >= int(a.start.replace("-", "")))
    if a.end:
        df = df.where(ym <= int(a.end.replace("-", "")))

    zone_ids = [r[0] for r in spark.read.parquet((lake / "dim_zone").as_uri()).select("locationid").collect()]
    print(f"dim_zone: {len(zone_ids)} mã khu vực hợp lệ (min={min(zone_ids)}, max={max(zone_ids)})")

    pu, do_ = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    dur = F.unix_timestamp(do_) - F.unix_timestamp(pu)           # giây
    dist, fare, tot = F.col("trip_distance"), F.col("fare_amount"), F.col("total_amount")
    pax, pul, dol = F.col("passenger_count"), F.col("pulocationid"), F.col("dolocationid")
    pay, rate = F.col("payment_type"), F.col("ratecodeid")
    pick_ym = F.year(pu) * 100 + F.month(pu)

    rules = [  # (mã, mô tả, điều kiện)
        ("TIME_NULL", "giờ đón hoặc giờ trả bị null", pu.isNull() | do_.isNull()),
        ("DUR_NEG", "giờ trả < giờ đón (thời lượng âm)", dur < 0),
        ("DUR_ZERO", "thời lượng = 0 giây", dur == 0),
        ("DUR_LT_1MIN", "thời lượng 1–59 giây", (dur > 0) & (dur < 60)),
        ("DUR_GT_3H", "thời lượng > 3 giờ", dur > 3 * 3600),
        ("DUR_GT_24H", "thời lượng > 24 giờ", dur > 24 * 3600),
        ("PICKUP_OUT_OF_MONTH", "ngày đón nằm ngoài tháng của file", pick_ym != ym),
        ("PICKUP_OUT_OF_YEAR", "ngày đón nằm ngoài năm của file", F.year(pu) != F.col("year")),
        ("DIST_NEG", "trip_distance < 0", dist < 0),
        ("DIST_ZERO", "trip_distance = 0", dist == 0),
        ("DIST_GT_100", "trip_distance > 100 dặm", dist > 100),
        ("SPEED_GT_100MPH", "tốc độ trung bình > 100 mph", (dur > 0) & (dist > 0) & (dist / (dur / 3600.0) > 100)),
        ("FARE_NEG", "fare_amount < 0", fare < 0),
        ("FARE_ZERO", "fare_amount = 0", fare == 0),
        ("FARE_GT_500", "fare_amount > 500", fare > 500),
        ("TOTAL_NEG", "total_amount < 0", tot < 0),
        ("TOTAL_ZERO", "total_amount = 0", tot == 0),
        ("TOTAL_GT_1000", "total_amount > 1000", tot > 1000),
        ("TOTAL_LT_FARE", "total_amount < fare_amount", tot < fare),
        ("TIP_NEG", "tip_amount < 0", F.col("tip_amount") < 0),
        ("TOLLS_NEG", "tolls_amount < 0", F.col("tolls_amount") < 0),
        ("EXTRA_NEG", "extra < 0", F.col("extra") < 0),
        ("MTA_TAX_NEG", "mta_tax < 0", F.col("mta_tax") < 0),
        ("PAX_NULL", "passenger_count null", pax.isNull()),
        ("PAX_ZERO", "passenger_count = 0", pax == 0),
        ("PAX_GT_6", "passenger_count > 6", pax > 6),
        ("PU_NULL", "pulocationid null", pul.isNull()),
        ("PU_NOT_IN_ZONES", "pulocationid không có trong bảng khu vực", pul.isNotNull() & ~pul.isin(zone_ids)),
        ("PU_UNKNOWN_264_265", "pulocationid thuộc nhóm Unknown (264, 265)", pul.isin(264, 265)),
        ("DO_NULL", "dolocationid null", dol.isNull()),
        ("DO_NOT_IN_ZONES", "dolocationid không có trong bảng khu vực", dol.isNotNull() & ~dol.isin(zone_ids)),
        ("DO_UNKNOWN_264_265", "dolocationid thuộc nhóm Unknown (264, 265)", dol.isin(264, 265)),
        ("PAY_ZERO", "payment_type = 0", pay == 0),
        ("PAY_OUT_OF_RANGE", "payment_type ngoài 0–6", pay.isNotNull() & ~pay.isin(0, 1, 2, 3, 4, 5, 6)),
        ("RATE_NULL", "ratecodeid null", rate.isNull()),
        ("RATE_INVALID", "ratecodeid ngoài 1–6 và 99", rate.isNotNull() & ~rate.isin(1, 2, 3, 4, 5, 6, 99)),
    ]
    cols = [c for c in df.columns if c not in ("year", "month")]

    aggs = [F.count(F.lit(1)).alias("total_rows")]
    aggs += [F.sum(F.when(cond, 1).otherwise(0)).alias(code) for code, _, cond in rules]
    aggs += [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"NULL__{c}") for c in cols]

    t0 = time.time()
    print("Quét dữ liệu (1 lượt, nhóm theo year/month) ...")
    rows = df.groupBy("year", "month").agg(*aggs).orderBy("year", "month").collect()
    by_month = pd.DataFrame([r.asDict() for r in rows])  # không dùng toPandas(): lỗi trên Python 3.13 với PySpark 3.5
    print(f"  xong sau {time.time() - t0:.1f}s")

    total_rows = int(by_month["total_rows"].sum())
    desc = {c: d for c, d, _ in rules}
    prof = [(c, desc[c], int(by_month[c].sum())) for c, _, _ in rules]
    prof_df = pd.DataFrame(prof, columns=["rule", "description", "rows"])
    prof_df["pct"] = (prof_df["rows"] / total_rows * 100).round(4)
    prof_df.to_csv(res / "dq_profile.csv", index=False, encoding="utf-8")

    long_df = by_month.melt(id_vars=["year", "month", "total_rows"], value_vars=[r[0] for r in rules],
                            var_name="rule", value_name="rows")
    long_df.to_csv(res / "dq_profile_by_month.csv", index=False)

    nulls = pd.DataFrame({"column": cols, "nulls": [int(by_month[f"NULL__{c}"].sum()) for c in cols]})
    nulls["pct"] = (nulls["nulls"] / total_rows * 100).round(4)
    nulls.to_csv(res / "dq_nulls.csv", index=False)

    qcols = ["fare_amount", "trip_distance", "total_amount"]
    probs = [0.0, 0.001, 0.01, 0.25, 0.5, 0.75, 0.99, 0.999, 1.0]
    qdf = df.select(*qcols, dur.alias("duration_s"))
    qs = qdf.approxQuantile(qcols + ["duration_s"], probs, 0.001)
    qtab = pd.DataFrame(qs, index=qcols + ["duration_s"], columns=[f"p{p*100:g}" for p in probs])
    qtab.index.name = "column"
    qtab.to_csv(res / "dq_quantiles.csv")

    cat_rows = []
    for c in ["vendorid", "payment_type", "ratecodeid", "passenger_count", "store_and_fwd_flag"]:
        for r in df.groupBy(c).count().orderBy(c).collect():
            cat_rows.append((c, r[0], int(r[1]), round(r[1] / total_rows * 100, 4)))
    cat = pd.DataFrame(cat_rows, columns=["column", "value", "rows", "pct"])
    cat.to_csv(res / "dq_categorical.csv", index=False)

    if a.duplicates:
        keys = ["vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "pulocationid", "dolocationid",
                "trip_distance", "total_amount", "passenger_count"]
        drows = []
        for _, r in by_month[["year", "month"]].iterrows():
            part = df.where((F.col("year") == int(r.year)) & (F.col("month") == int(r.month))).select(*keys)
            n, d = part.count(), part.distinct().count()
            drows.append((int(r.year), int(r.month), n, n - d))
            print(f"  trùng {int(r.year)}-{int(r.month):02d}: {n - d:,} / {n:,}")
        pd.DataFrame(drows, columns=["year", "month", "rows", "duplicate_rows"]).to_csv(res / "dq_duplicates.csv", index=False)

    spark.stop()

    print(f"\nTổng số dòng: {total_rows:,}\n")
    print(f"{'rule':<22}{'rows':>14}{'%':>10}  mô tả")
    for c, d, n in prof:
        print(f"{c:<22}{n:>14,}{n / total_rows * 100:>9.3f}%  {d}")
    print("\nNull theo cột (chỉ cột có null):")
    for _, r in nulls[nulls["nulls"] > 0].iterrows():
        print(f"  {r['column']:<24}{int(r['nulls']):>12,}  {r['pct']:.3f}%")
    print("\nPhân vị:\n" + qtab.to_string())
    print(f"\nĐã ghi các file dq_*.csv vào {res}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""BẢN 2 của 02_clean.py (bản 1 giữ nguyên để so sánh trước/sau). Khác bản 1: quãng đường 0 -> SỬA thay vì loại; ngưỡng
thời lượng 6 giờ, tốc độ 80 mph; thêm luật FARE_BELOW_MIN (0 < fare < 3 USD khi quãng đường > 0). Lý do: results/02b, 02c.
Ghi ra data/clean_v2 (không đè data/clean của bản 1) và results/02_clean_v2/.

Processing: làm sạch + chuẩn hoá dữ liệu lake -> data/clean_v2/yellow (đạt) và data/clean_v2/yellow_rejected (bị loại, kèm lý do).

Các luật dựa trên số đếm thật ở results/02a_profile_raw/ và results/02b_verify_rules/ (xem NOTES.md). Hai nhóm:
  LOẠI (reject)  – dòng không dùng được cho phân tích chuyến đi; KHÔNG xoá hẳn mà đưa vào yellow_rejected
                   với cột reject_reasons (nhiều mã nối bằng "|") để kiểm toán và báo cáo "trước/sau".
  SỬA (repair)   – lỗi nhỏ, phần còn lại của dòng vẫn tốt: passenger_count = 0 hoặc > 6 -> null (+ cột cờ);
                   trip_distance = 0 -> null (+ cờ is_zero_distance, avg_speed_mph cũng null): sau 02b/02c, 90% dòng
                   này chỉ lỗi quãng đường (giá bình thường, nhiều dòng là Flex/giá thương lượng/lỗi thiết bị Q1), loại
                   sẽ làm doanh thu thấp 2,1%. KPI theo quãng đường tự bỏ qua null; KPI đếm chuyến/doanh thu vẫn tính.
  GIỮ NGUYÊN     – dòng "Flex Fare" (payment_type = 0): passenger_count/ratecodeid/store_and_fwd_flag/
                   congestion_surcharge/airport_fee cùng null trên ~9,9% dòng; là chuyến thật nên không loại.
Ngoài ra: loại dòng trùng hệt (mọi cột nghiệp vụ giống nhau) trong cùng tháng.
Cột thêm: trip_duration_s, avg_speed_mph, pickup_hour, pickup_dow (1=Chủ nhật…7=Thứ bảy), is_flex_fare,
          passenger_count_repaired.

Xử lý TỪNG THÁNG (vừa RAM 8 GB); chạy lại một tháng chỉ ghi đè đúng phân vùng đó.
Kiểm chứng bảo toàn: rows_in = trùng + loại + đạt, ghi vào results/02_clean_v2/clean_summary_by_month.csv.

Chạy:
  python jobs/02_clean_v2.py
  python jobs/02_clean_v2.py --start 2024-01 --end 2024-03
  python jobs/02_clean_v2.py --max-distance 150 --max-speed 80      # đổi ngưỡng (được lưu vào clean_params.json)
"""
import argparse
import csv
import datetime as dt
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, get_spark, results_dir  # noqa: E402

BUSINESS_COLS = [
    "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "ratecodeid",
    "store_and_fwd_flag", "pulocationid", "dolocationid", "payment_type", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee",
    "cbd_congestion_fee",
]
MONEY_PARTS = ["extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "congestion_surcharge",
               "airport_fee", "cbd_congestion_fee"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--type", default="yellow")
    ap.add_argument("--lake", default=str(ROOT / "data" / "lake"))
    ap.add_argument("--clean", default=str(ROOT / "data" / "clean_v2"))
    ap.add_argument("--results", default=None, help="mặc định: results/02_clean_v2")
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--max-duration-h", type=float, default=6.0, help="loại chuyến dài hơn (giờ); 02b: >6h TB 21 giờ, 0,3 mph = quên tắt đồng hồ")
    ap.add_argument("--max-distance", type=float, default=100.0, help="loại quãng đường lớn hơn (dặm)")
    ap.add_argument("--max-speed", type=float, default=80.0, help="loại tốc độ trung bình lớn hơn (mph)")
    ap.add_argument("--min-fare", type=float, default=3.0, help="loại fare dương nhưng nhỏ hơn mức này khi quãng đường > 0 (phí khởi điểm TLC 3 USD)")
    ap.add_argument("--max-fare", type=float, default=500.0)
    ap.add_argument("--max-total", type=float, default=1000.0)
    ap.add_argument("--max-passengers", type=int, default=6)
    ap.add_argument("--cores", type=int, default=2)
    ap.add_argument("--driver-mem", default="3g")
    a = ap.parse_args()

    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    lake, res = Path(a.lake), results_dir(__file__, a.results)
    out_ok = Path(a.clean) / a.type
    out_bad = Path(a.clean) / f"{a.type}_rejected"
    spark = get_spark("clean", cores=a.cores, driver_mem=a.driver_mem)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    src = spark.read.parquet((lake / a.type).as_uri())
    ym_col = F.col("year") * 100 + F.col("month")
    if a.start:
        src = src.where(ym_col >= int(a.start.replace("-", "")))
    if a.end:
        src = src.where(ym_col <= int(a.end.replace("-", "")))
    months = sorted((r.year, r.month) for r in src.select("year", "month").distinct().collect())
    if not months:
        print("Không có dữ liệu trong lake cho khoảng đã chọn. Chạy 01_ingest_to_lake.py trước.")
        return 1
    zone_ids = [r[0] for r in spark.read.parquet((lake / "dim_zone").as_uri()).select("locationid").collect()]
    (res / "clean_params.json").write_text(json.dumps({**vars(a), "zone_ids": len(zone_ids)}, indent=2, ensure_ascii=False))

    pu, do_ = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    dur = F.unix_timestamp(do_) - F.unix_timestamp(pu)
    dist, fare, tot = F.col("trip_distance"), F.col("fare_amount"), F.col("total_amount")
    pax, pul, dol = F.col("passenger_count"), F.col("pulocationid"), F.col("dolocationid")
    neg_part = F.lit(False)
    for c in MONEY_PARTS:
        neg_part = neg_part | (F.coalesce(F.col(c), F.lit(0.0)) < 0)

    rules = [  # (mã, mô tả, điều kiện LOẠI)
        ("TIME_NULL", "giờ đón/trả null", pu.isNull() | do_.isNull()),
        ("DUR_NONPOSITIVE", "thời lượng <= 0 giây", dur <= 0),
        ("DUR_GT_MAX", f"thời lượng > {a.max_duration_h:g} giờ", dur > a.max_duration_h * 3600),
        ("PICKUP_OUT_OF_MONTH", "ngày đón ngoài tháng của file", (F.year(pu) * 100 + F.month(pu)) != ym_col),
        ("DIST_NEGATIVE", "trip_distance < 0", dist < 0),
        ("DIST_GT_MAX", f"trip_distance > {a.max_distance:g} dặm", dist > a.max_distance),
        ("SPEED_GT_MAX", f"tốc độ TB > {a.max_speed:g} mph", (dur > 0) & (dist / (dur / 3600.0) > a.max_speed)),
        ("FARE_NONPOSITIVE", "fare_amount <= 0", fare <= 0),
        ("FARE_BELOW_MIN", f"0 < fare_amount < {a.min_fare:g} khi quãng đường > 0", (fare > 0) & (fare < a.min_fare) & (dist > 0)),
        ("FARE_GT_MAX", f"fare_amount > {a.max_fare:g}", fare > a.max_fare),
        ("TOTAL_NONPOSITIVE", "total_amount <= 0", tot <= 0),
        ("TOTAL_GT_MAX", f"total_amount > {a.max_total:g}", tot > a.max_total),
        ("NEG_COMPONENT", "có khoản phụ phí/tip/thuế < 0", neg_part),
        ("ZONE_INVALID", "mã khu vực null hoặc không có trong dim_zone",
         pul.isNull() | dol.isNull() | ~pul.isin(zone_ids) | ~dol.isin(zone_ids)),
    ]
    rules = [(c, d, F.coalesce(cond, F.lit(False))) for c, d, cond in rules]
    pax_bad = F.coalesce((pax == 0) | (pax > a.max_passengers), F.lit(False))

    log_path = res / "clean_summary_by_month.csv"
    totals = {"rows_in": 0, "dups": 0, "rejected": 0, "clean": 0, "pax_repaired": 0, "zero_dist": 0}
    rule_totals = {c: 0 for c, _, _ in rules}
    bad = 0
    with open(log_path, "w", newline="", encoding="utf-8") as lf:
        w = csv.writer(lf)
        w.writerow(["timestamp", "year", "month", "rows_in", "duplicates_removed", "rows_rejected", "rows_clean",
                    "passenger_repaired", "pct_clean", "status", "seconds", "zero_distance_flagged"])
        for y, m in months:
            t0 = time.time()
            df = src.where((F.col("year") == y) & (F.col("month") == m))
            rows_in = df.count()
            dd = df.dropDuplicates(BUSINESS_COLS)
            tagged = (
                dd.withColumn("reject_reasons", F.concat_ws("|", *[F.when(c, F.lit(code)) for code, _, c in rules]))
                .withColumn("passenger_count_repaired", pax_bad)
                .withColumn("is_zero_distance", F.coalesce(dist == 0, F.lit(False)))
                .withColumn("trip_duration_s", dur.cast("int"))
                .withColumn("avg_speed_mph", F.when((dur > 0) & (dist > 0), dist / (dur / 3600.0)))
                .withColumn("passenger_count", F.when(pax_bad, F.lit(None).cast("bigint")).otherwise(pax))
                .withColumn("trip_distance", F.when(F.col("is_zero_distance"), F.lit(None).cast("double")).otherwise(dist))
                .withColumn("pickup_hour", F.hour(pu))
                .withColumn("pickup_dow", F.dayofweek(pu))
                .withColumn("is_flex_fare", F.col("payment_type") == 0)
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
            agg_exprs = [F.count(F.lit(1)).alias("n"),
                         F.sum(F.when(F.col("reject_reasons") == "", 1).otherwise(0)).alias("n_clean"),
                         F.sum(F.when(F.col("passenger_count_repaired"), 1).otherwise(0)).alias("n_pax"),
                         F.sum(F.when(F.col("is_zero_distance"), 1).otherwise(0)).alias("n_zero_dist")]
            reasons = F.split("reject_reasons", r"\|")  # đếm theo cột đã gắn nhãn (cột gốc đã được sửa nên không tính lại điều kiện)
            agg_exprs += [F.sum(F.when(F.array_contains(reasons, code), 1).otherwise(0)).alias(code) for code, _, _ in rules]
            r = tagged.agg(*agg_exprs).first().asDict()
            n, n_clean, n_pax, n_zd = int(r["n"]), int(r["n_clean"]), int(r["n_pax"]), int(r["n_zero_dist"])
            dups, rejected = rows_in - n, n - n_clean

            (tagged.where(F.col("reject_reasons") == "").drop("reject_reasons")
             .write.mode("overwrite").partitionBy("year", "month").parquet(out_ok.as_uri()))
            (tagged.where(F.col("reject_reasons") != "").drop("passenger_count_repaired")
             .write.mode("overwrite").partitionBy("year", "month").parquet(out_bad.as_uri()))
            tagged.unpersist()

            back = (spark.read.parquet(out_ok.as_uri()).where((F.col("year") == y) & (F.col("month") == m)).count()
                    + spark.read.parquet(out_bad.as_uri()).where((F.col("year") == y) & (F.col("month") == m)).count())
            ok = (rows_in == dups + rejected + n_clean) and (back == n)
            bad += not ok
            secs = time.time() - t0
            for c, _, _ in rules:
                rule_totals[c] += int(r[c])
            for k, v in (("rows_in", rows_in), ("dups", dups), ("rejected", rejected), ("clean", n_clean), ("pax_repaired", n_pax), ("zero_dist", n_zd)):
                totals[k] += v
            print(f"[{y}-{m:02d}] in={rows_in:,} trùng={dups:,} loại={rejected:,} đạt={n_clean:,} "
                  f"({n_clean / rows_in * 100:.2f}%) sửa_pax={n_pax:,} {'OK' if ok else 'LỆCH'} {secs:.1f}s")
            w.writerow([dt.datetime.now().isoformat(timespec="seconds"), y, m, rows_in, dups, rejected, n_clean, n_pax,
                        f"{n_clean / rows_in * 100:.4f}", "OK" if ok else "MISMATCH", f"{secs:.2f}", n_zd])
            lf.flush()

    with open(res / "rule_counts.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["rule", "description", "rows", "pct_of_input"])
        for c, d, _ in rules:
            w.writerow([c, d, rule_totals[c], f"{rule_totals[c] / totals['rows_in'] * 100:.4f}"])
        w.writerow(["PAX_REPAIRED_TO_NULL", f"passenger_count = 0 hoặc > {a.max_passengers} -> null (sửa, không loại)",
                    totals["pax_repaired"], f"{totals['pax_repaired'] / totals['rows_in'] * 100:.4f}"])
        w.writerow(["ZERO_DISTANCE_FLAGGED", "trip_distance = 0 -> null + is_zero_distance (sửa, không loại; tính cả dòng đã bị luật khác loại)",
                    totals["zero_dist"], f"{totals['zero_dist'] / totals['rows_in'] * 100:.4f}"])
        w.writerow(["DUPLICATES_REMOVED", "dòng trùng hệt trong tháng", totals["dups"],
                    f"{totals['dups'] / totals['rows_in'] * 100:.4f}"])

    clean_all = spark.read.parquet(out_ok.as_uri())
    if a.start:
        clean_all = clean_all.where(F.col("year") * 100 + F.col("month") >= int(a.start.replace("-", "")))
    if a.end:
        clean_all = clean_all.where(F.col("year") * 100 + F.col("month") <= int(a.end.replace("-", "")))
    qc = ["fare_amount", "trip_distance", "total_amount", "trip_duration_s", "avg_speed_mph"]
    probs = [0.0, 0.001, 0.01, 0.25, 0.5, 0.75, 0.99, 0.999, 1.0]
    qs = clean_all.select(*qc).approxQuantile(qc, probs, 0.00001)  # sai số nhỏ để p99.9 không trùng max
    with open(res / "after_quantiles.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["column"] + [f"p{p * 100:g}" for p in probs])
        for c, row in zip(qc, qs):
            w.writerow([c] + [round(x, 4) for x in row])
    spark.stop()

    print(f"\nTỔNG: vào {totals['rows_in']:,} | trùng {totals['dups']:,} | loại {totals['rejected']:,} "
          f"({totals['rejected'] / totals['rows_in'] * 100:.2f}%) | đạt {totals['clean']:,} "
          f"({totals['clean'] / totals['rows_in'] * 100:.2f}%) | sửa passenger_count {totals['pax_repaired']:,}")
    print("\nSố dòng vi phạm từng luật (một dòng có thể vi phạm nhiều luật):")
    for c, d, _ in rules:
        print(f"  {c:<22}{rule_totals[c]:>12,}  {d}")
    print("\nPhân vị SAU làm sạch:")
    for c, row in zip(qc, qs):
        print(f"  {c:<16}" + "  ".join(f"{x:>11,.2f}" for x in row))
    print(f"\nKết quả: {res}\nDữ liệu: {out_ok}  và  {out_bad}\n{'OK – mọi tháng bảo toàn số dòng.' if not bad else f'CẢNH BÁO: {bad} tháng lệch số dòng.'}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())

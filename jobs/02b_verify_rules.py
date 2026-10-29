#!/usr/bin/env python3
"""Kiểm chứng các luật làm sạch bằng dữ liệu thật (chạy SAU 02_clean.py). Mục tiêu: biến mọi "giả thuyết" thành số đo.

Đầu vào : data/lake (dữ liệu gốc đã nạp), data/clean/yellow (đạt), data/clean/yellow_rejected (bị loại + lý do).
Đầu ra  : results/02b_verify_rules/*.csv (mỗi file trả lời một câu hỏi), in tóm tắt ra màn hình.

  v0_rule_overlap.csv / v0_reason_combos.csv   mỗi luật loại bao nhiêu dòng RIÊNG nó (không bị luật khác bắt) -> tác động thật
  v1_payment_vs_fare_sign.csv                  giá âm/0/dương theo payment_type  -> giả thuyết "tiền âm là hoàn tiền/tranh chấp"
  v2_zero_distance_profile.csv                 dòng quãng đường = 0 theo tháng, nhà cung cấp, loại thanh toán, thời lượng, mức giá...
  v2b_zero_distance_fare_explained.csv         giá của dòng quãng đường 0 có giải thích được bằng phí khởi điểm + phí thời gian không
  v2c_zero_distance_rate_vendor_month.csv      tỉ lệ quãng đường 0 theo nhà cung cấp x tháng -> lỗi theo nhà cung cấp?
  v3_flex_fare_check.csv                       payment_type = 0 có đúng là tập dòng null 5 cột không
  v4_soft_candidates.csv                       ứng viên luật bổ sung trên dữ liệu ĐẠT (giá < 3 USD, tốc độ cao, chuyến rất ngắn...)
  v5_sensitivity.csv                           KPI đổi bao nhiêu % khi nới/siết luật  -> kết luận có bền vững không
  v6_reject_by_month_reason.csv                dòng bị loại theo tháng x luật (xu hướng theo thời gian)
  v7_consistency_checks.csv                    kiểm tra nhất quán nghiệp vụ (tổng tiền = tổng các khoản, tip tiền mặt, giá JFK, phí sân bay)

Hằng số nghiệp vụ lấy từ trang giá cước TLC (nyc.gov/site/tlc/passengers/taxi-fare.page; trang không ghi ngày hiệu lực,
nên giả định áp dụng cho 2024): phí khởi điểm 3,00 USD; phí thời gian 0,70 USD / 60 giây khi xe đứng hoặc chậm;
JFK đồng giá 70 USD. Nhãn mã theo data dictionary TLC: payment_type 0 Flex Fare, 1 Credit card, 2 Cash, 3 No charge,
4 Dispute, 5 Unknown, 6 Voided; VendorID 1 Creative Mobile, 2 Curb Mobility, 6 Myle, 7 Helix.

Chạy:  python jobs/02b_verify_rules.py            (có thể thêm --start 2024-01 --end 2024-03 để thử)
Chưa đo thời gian trên dữ liệu thật; đọc nhiều lượt trên ~40 triệu dòng nên có thể vài phút đến chục phút.
"""
import argparse
import sys
import time
from functools import reduce
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, get_spark, results_dir  # noqa: E402

PAY_LABEL = {0: "Flex Fare", 1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided"}
VENDOR_LABEL = {1: "Creative Mobile", 2: "Curb Mobility", 6: "Myle", 7: "Helix"}
FLAG_DROP, TIME_RATE_PER_MIN, JFK_FLAT = 3.00, 0.70, 70.00
MONEY_PARTS = ["extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "congestion_surcharge",
               "airport_fee", "cbd_congestion_fee"]
FLEX_NULL_COLS = ["passenger_count", "ratecodeid", "store_and_fwd_flag", "congestion_surcharge", "airport_fee"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--type", default="yellow")
    ap.add_argument("--lake", default=str(ROOT / "data" / "lake"))
    ap.add_argument("--clean", default=str(ROOT / "data" / "clean"))
    ap.add_argument("--results", default=None, help="mặc định: results/02b_verify_rules")
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--cores", type=int, default=2)
    ap.add_argument("--driver-mem", default="3g")
    a = ap.parse_args()

    import pandas as pd
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    res = results_dir(__file__, a.results)
    spark = get_spark("verify_rules", cores=a.cores, driver_mem=a.driver_mem)
    ymc = F.col("year") * 100 + F.col("month")

    def load(path):
        d = spark.read.parquet(Path(path).as_uri())
        if a.start:
            d = d.where(ymc >= int(a.start.replace("-", "")))
        if a.end:
            d = d.where(ymc <= int(a.end.replace("-", "")))
        return d

    lake = load(Path(a.lake) / a.type)
    clean = load(Path(a.clean) / a.type)
    rej = load(Path(a.clean) / f"{a.type}_rejected").persist(StorageLevel.MEMORY_AND_DISK)
    zones = spark.read.parquet((Path(a.lake) / "dim_zone").as_uri())

    def pdf(rows):
        return pd.DataFrame([r.asDict() for r in rows])

    def save(df, name):
        df.to_csv(res / name, index=False, encoding="utf-8")
        return df

    def show(title, df, n=40):
        print(f"\n=== {title}")
        print(df.head(n).to_string(index=False))

    t_all = time.time()
    n_rej = rej.count()
    print(f"Dòng bị loại: {n_rej:,}")

    # ---------------- V0: tác động riêng của từng luật ----------------
    arr = rej.select(F.split("reject_reasons", r"\|").alias("arr"))
    per_rule = pdf(arr.select(F.explode("arr").alias("rule"), (F.size("arr") == 1).alias("only"))
                   .groupBy("rule").agg(F.count(F.lit(1)).alias("rows_total"),
                                        F.sum(F.col("only").cast("int")).alias("rows_only_this_rule")).collect())
    per_rule["pct_only_of_rule"] = (per_rule["rows_only_this_rule"] / per_rule["rows_total"] * 100).round(2)
    per_rule = per_rule.sort_values("rows_total", ascending=False)
    save(per_rule, "v0_rule_overlap.csv")
    combos = pdf(rej.groupBy("reject_reasons").count().orderBy(F.desc("count")).limit(60).collect())
    save(combos, "v0_reason_combos.csv")
    show("V0 – mỗi luật bắt bao nhiêu dòng, bao nhiêu dòng CHỈ bị luật đó bắt", per_rule)

    # ---------------- V1: giá âm/0/dương theo payment_type ----------------
    fare, tot = F.col("fare_amount"), F.col("total_amount")
    parts_nonpos = reduce(lambda x, y: x & y, [F.coalesce(F.col(c), F.lit(0.0)) <= 0 for c in MONEY_PARTS])
    fare_class = F.when(fare < 0, "fare<0").when(fare == 0, "fare=0").otherwise("fare>0")
    neg_pattern = F.when(fare < 0, F.when(parts_nonpos & (tot < 0), "đảo toàn bộ (mọi khoản <= 0)")
                         .otherwise("chỉ một phần âm")).otherwise("-")
    v1 = pdf(lake.groupBy("payment_type", fare_class.alias("fare_class"), neg_pattern.alias("neg_pattern"))
             .count().collect()).rename(columns={"count": "rows"})
    v1["label"] = v1["payment_type"].map(PAY_LABEL)
    tot_pay = v1.groupby("payment_type")["rows"].transform("sum")
    tot_cls = v1.groupby("fare_class")["rows"].transform("sum")
    v1["pct_within_payment_type"] = (v1["rows"] / tot_pay * 100).round(3)
    v1["pct_within_fare_class"] = (v1["rows"] / tot_cls * 100).round(3)
    v1 = v1.sort_values(["fare_class", "rows"], ascending=[True, False])[
        ["payment_type", "label", "fare_class", "neg_pattern", "rows", "pct_within_payment_type", "pct_within_fare_class"]]
    save(v1, "v1_payment_vs_fare_sign.csv")
    show("V1 – giá âm theo payment_type (cột pct_within_fare_class cho biết loại thanh toán nào chiếm các dòng giá âm)",
         v1[v1["fare_class"] == "fare<0"])

    # ---------------- V2: dòng quãng đường = 0 ----------------
    zd = rej.where(F.col("reject_reasons").contains("DIST_NONPOSITIVE")).persist(StorageLevel.MEMORY_AND_DISK)
    n_zd = zd.count()
    dur = F.col("trip_duration_s")
    dims = [
        ("month", F.col("month").cast("string")),
        ("vendorid", F.col("vendorid").cast("string")),
        ("payment_type", F.col("payment_type").cast("string")),
        ("ratecodeid", F.coalesce(F.col("ratecodeid").cast("string"), F.lit("null"))),
        ("same_zone", F.when(F.col("pulocationid") == F.col("dolocationid"), "pu==do").otherwise("pu!=do")),
        ("duration_bucket", F.when(dur <= 0, "<=0s").when(dur < 60, "<1 phút").when(dur < 300, "1-5 phút")
         .when(dur < 900, "5-15 phút").when(dur < 3600, "15-60 phút").otherwise(">60 phút")),
        ("fare_bucket", F.when(fare <= 0, "<=0").when(fare <= 3, "(0,3]").when(fare <= 10, "(3,10]")
         .when(fare <= 30, "(10,30]").when(fare <= 100, "(30,100]").otherwise(">100")),
        ("rejected_only_by_dist_rule", F.when(F.col("reject_reasons") == "DIST_NONPOSITIVE", "chỉ luật này")
         .otherwise("cùng luật khác")),
    ]
    frames = []
    for name, expr in dims:
        d = pdf(zd.groupBy(expr.alias("value")).agg(F.count(F.lit(1)).alias("rows"), F.avg(fare).alias("avg_fare"),
                                                   F.avg(dur).alias("avg_duration_s")).collect())
        d.insert(0, "dimension", name)
        frames.append(d)
    v2 = pd.concat(frames, ignore_index=True)
    v2["pct"] = (v2["rows"] / n_zd * 100).round(3)
    v2["avg_fare"] = v2["avg_fare"].round(2)
    v2["avg_duration_s"] = v2["avg_duration_s"].round(1)
    v2["value_label"] = v2.apply(lambda r: PAY_LABEL.get(int(r["value"]), "") if r["dimension"] == "payment_type" and str(r["value"]).lstrip("-").isdigit()
                                 else (VENDOR_LABEL.get(int(r["value"]), "") if r["dimension"] == "vendorid" and str(r["value"]).isdigit() else ""), axis=1)
    save(v2, "v2_zero_distance_profile.csv")
    show(f"V2 – {n_zd:,} dòng quãng đường = 0: phân rã theo từng chiều", v2[v2["dimension"].isin(["fare_bucket", "rejected_only_by_dist_rule", "same_zone", "duration_bucket", "payment_type"])], 60)

    exp_fare = F.lit(FLAG_DROP) + F.lit(TIME_RATE_PER_MIN) * (dur / 60.0)
    cat = (F.when(fare <= 0, "1. giá <= 0 (nhóm đảo giao dịch)")
           .when(fare <= FLAG_DROP + 0.5, "2. chỉ phí khởi điểm (~3 USD): giống chuyến bị huỷ ngay")
           .when(F.abs(fare - exp_fare) <= 1.0, "3. khởi điểm + phí thời gian: xe đứng yên, đồng hồ chạy")
           .when(fare > exp_fare + 1.0, "4. giá CAO hơn mức thời gian giải thích: nghi quãng đường không được ghi")
           .otherwise("5. giá thấp hơn mức thời gian (khác)"))
    v2b = pdf(zd.groupBy(cat.alias("category")).agg(F.count(F.lit(1)).alias("rows"), F.avg(fare).alias("avg_fare"),
                                                    (F.avg(dur) / 60).alias("avg_duration_min"),
                                                    F.avg(exp_fare).alias("avg_expected_fare_time_only")).orderBy("category").collect())
    v2b["pct"] = (v2b["rows"] / n_zd * 100).round(2)
    v2b[["avg_fare", "avg_duration_min", "avg_expected_fare_time_only"]] = v2b[["avg_fare", "avg_duration_min", "avg_expected_fare_time_only"]].round(2)
    save(v2b, "v2b_zero_distance_fare_explained.csv")
    show("V2b – giá của dòng quãng đường = 0 có giải thích được không", v2b)

    v2c = pdf(lake.groupBy("vendorid", "month").agg(F.count(F.lit(1)).alias("rows"),
                                                     F.sum(F.when(F.col("trip_distance") == 0, 1).otherwise(0)).alias("zero_distance_rows")).collect())
    v2c["zero_distance_pct"] = (v2c["zero_distance_rows"] / v2c["rows"] * 100).round(3)
    v2c["vendor_label"] = v2c["vendorid"].map(VENDOR_LABEL)
    v2c = v2c.sort_values(["vendorid", "month"])
    save(v2c, "v2c_zero_distance_rate_vendor_month.csv")
    show("V2c – tỉ lệ quãng đường = 0 theo nhà cung cấp (nếu một nhà cung cấp cao bất thường -> lỗi thiết bị)",
         v2c.pivot(index="month", columns="vendorid", values="zero_distance_pct"))

    # ---------------- V3: Flex Fare ----------------
    all_null = reduce(lambda x, y: x & y, [F.col(c).isNull() for c in FLEX_NULL_COLS])
    any_null = reduce(lambda x, y: x | y, [F.col(c).isNull() for c in FLEX_NULL_COLS])
    v3a = pdf(lake.groupBy((F.col("payment_type") == 0).alias("is_flex"), all_null.alias("all5_null"), any_null.alias("any5_null"),
                           F.col("vendorid")).count().collect())
    v3a["vendor_label"] = v3a["vendorid"].map(VENDOR_LABEL)
    v3a = v3a.sort_values("count", ascending=False).rename(columns={"count": "rows"})
    save(v3a, "v3_flex_fare_check.csv")
    flex = v3a[v3a["is_flex"]]["rows"].sum()
    flex_all_null = v3a[v3a["is_flex"] & v3a["all5_null"]]["rows"].sum()
    nonflex_any_null = v3a[~v3a["is_flex"] & v3a["any5_null"]]["rows"].sum()
    print(f"\n=== V3 – Flex Fare: payment_type=0 có {flex:,} dòng; trong đó null đủ 5 cột: {flex_all_null:,}; "
          f"dòng KHÔNG phải flex mà có null ở cột nào đó trong 5 cột: {nonflex_any_null:,}")
    show("V3 – theo nhà cung cấp", v3a.groupby(["vendorid", "vendor_label", "is_flex"], dropna=False)["rows"].sum().reset_index())

    # ---------------- V4: ứng viên luật bổ sung (trên dữ liệu ĐẠT) ----------------
    d_, sp, dist = F.col("trip_duration_s"), F.col("avg_speed_mph"), F.col("trip_distance")
    cands = [
        ("FARE_LT_3 (quãng đường > 0)", (dist > 0) & (fare < FLAG_DROP)), ("FARE_LT_1", fare < 1),
        ("DUR_LT_10S", d_ < 10), ("DUR_LT_30S", d_ < 30), ("DUR_LT_60S", d_ < 60),
        ("DUR_GT_3H", d_ > 3 * 3600), ("DUR_GT_6H", d_ > 6 * 3600),
        ("SPEED_GT_40MPH", sp > 40), ("SPEED_GT_60MPH", sp > 60), ("SPEED_GT_80MPH", sp > 80),
        ("DIST_GT_30", dist > 30), ("DIST_GT_50", dist > 50),
        ("DIST_LT_0.1 (> 0)", (dist > 0) & (dist < 0.1)),
        ("FARE_PER_MILE_GT_30", (dist > 0) & (fare / dist > 30)), ("FARE_PER_MILE_GT_100", (dist > 0) & (fare / dist > 100)),
        ("TOTAL_LT_FARE", tot < fare),
    ]
    ex = [F.count(F.lit(1)).alias("__n")]
    for i, (_, c) in enumerate(cands):
        ex += [F.sum(F.when(c, 1).otherwise(0)).alias(f"n{i}"), F.avg(F.when(c, fare)).alias(f"f{i}"),
               F.avg(F.when(c, dist)).alias(f"d{i}"), F.avg(F.when(c, d_)).alias(f"t{i}"), F.avg(F.when(c, sp)).alias(f"s{i}")]
    r = clean.agg(*ex).first().asDict()
    n_clean = r["__n"]
    v4 = pd.DataFrame([{"candidate": nm, "rows_in_clean": r[f"n{i}"], "pct_of_clean": round(r[f"n{i}"] / n_clean * 100, 4),
                        "avg_fare": r[f"f{i}"], "avg_distance": r[f"d{i}"], "avg_duration_s": r[f"t{i}"], "avg_speed_mph": r[f"s{i}"]}
                       for i, (nm, _) in enumerate(cands)]).round(2)
    save(v4, "v4_soft_candidates.csv")
    show(f"V4 – ứng viên luật bổ sung trên {n_clean:,} dòng ĐẠT (nhìn avg_* để biết nhóm này trông giống chuyến thật hay lỗi)", v4)

    # ---------------- V5: độ nhạy ----------------
    scen = {
        "A_baseline (luật hiện tại)": F.lit(True),
        "C_speed<=60mph": sp <= 60,
        "D_fare>=3USD": fare >= FLAG_DROP,
        "E_duration>=30s": d_ >= 30,
        "F_duration<=6h": d_ <= 6 * 3600,
        "G_chặt: C+D+E+F+distance<=60": (sp <= 60) & (fare >= FLAG_DROP) & (d_ >= 30) & (d_ <= 6 * 3600) & (dist <= 60),
    }
    ex = []
    for i, (_, c) in enumerate(scen.items()):
        ex += [F.sum(F.when(c, 1).otherwise(0)).alias(f"n{i}"), F.sum(F.when(c, tot)).alias(f"rev{i}"),
               F.sum(F.when(c, fare)).alias(f"fare{i}"), F.sum(F.when(c, dist)).alias(f"dist{i}"), F.sum(F.when(c, d_)).alias(f"dur{i}")]
    r = clean.agg(*ex).first().asDict()
    rows5 = []
    for i, name in enumerate(scen):
        rows5.append({"scenario": name, "rows": r[f"n{i}"], "revenue": r[f"rev{i}"], "sum_fare": r[f"fare{i}"],
                      "sum_dist": r[f"dist{i}"], "sum_dur": r[f"dur{i}"]})
    only_dist = rej.where(F.col("reject_reasons") == "DIST_NONPOSITIVE").agg(
        F.count(F.lit(1)).alias("n"), F.sum(tot).alias("rev"), F.sum(fare).alias("fare"), F.sum(dist).alias("dist"),
        F.sum(d_).alias("dur")).first()
    base = rows5[0]
    rows5.insert(1, {"scenario": "B_giữ lại dòng quãng đường=0 (chỉ bị luật này loại)",
                     "rows": base["rows"] + only_dist["n"], "revenue": base["revenue"] + (only_dist["rev"] or 0),
                     "sum_fare": base["sum_fare"] + (only_dist["fare"] or 0), "sum_dist": base["sum_dist"] + (only_dist["dist"] or 0),
                     "sum_dur": base["sum_dur"] + (only_dist["dur"] or 0)})
    v5 = pd.DataFrame(rows5)
    v5["avg_total_amount"] = v5["revenue"] / v5["rows"]
    v5["avg_fare"] = v5["sum_fare"] / v5["rows"]
    v5["avg_distance"] = v5["sum_dist"] / v5["rows"]
    v5["avg_duration_s"] = v5["sum_dur"] / v5["rows"]
    for col in ["rows", "revenue", "avg_total_amount", "avg_fare", "avg_distance", "avg_duration_s"]:
        v5[f"delta_pct_{col}"] = ((v5[col] / v5.loc[0, col] - 1) * 100).round(3)
    v5 = v5.drop(columns=["sum_fare", "sum_dist", "sum_dur"]).round(3)
    save(v5, "v5_sensitivity.csv")
    show("V5 – KPI đổi bao nhiêu % so với baseline khi đổi luật (đổi < ~1% = kết luận bền vững)",
         v5[["scenario", "rows", "delta_pct_rows", "delta_pct_revenue", "delta_pct_avg_total_amount", "delta_pct_avg_distance", "delta_pct_avg_duration_s"]])

    # ---------------- V6: bị loại theo tháng x luật ----------------
    v6 = pdf(rej.select("year", "month", F.explode(F.split("reject_reasons", r"\|")).alias("rule"))
             .groupBy("year", "month", "rule").count().collect()).rename(columns={"count": "rows"})
    mc = pdf(clean.groupBy("year", "month").count().collect()).rename(columns={"count": "clean_rows"})
    mr = pdf(rej.groupBy("year", "month").count().collect()).rename(columns={"count": "rejected_rows"})
    mt = mc.merge(mr, on=["year", "month"])
    mt["month_total"] = mt["clean_rows"] + mt["rejected_rows"]
    v6 = v6.merge(mt[["year", "month", "month_total"]], on=["year", "month"])
    v6["pct_of_month"] = (v6["rows"] / v6["month_total"] * 100).round(3)
    v6 = v6.sort_values(["rule", "year", "month"])
    save(v6, "v6_reject_by_month_reason.csv")
    show("V6 – % dòng của tháng bị luật bắt (xu hướng theo tháng)", v6.pivot(index="month", columns="rule", values="pct_of_month"))

    # ---------------- V7: nhất quán nghiệp vụ (trên dữ liệu ĐẠT) ----------------
    parts_sum = F.coalesce(fare, F.lit(0.0)) + reduce(lambda x, y: x + y, [F.coalesce(F.col(c), F.lit(0.0)) for c in MONEY_PARTS])
    airports = [rw[0] for rw in zones.where(F.col("zone").rlike("(?i)JFK|LaGuardia")).select("locationid").collect()]
    airport_pu = F.col("pulocationid").isin(airports) if airports else F.lit(False)
    r = clean.agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.when(F.abs(tot - parts_sum) > 0.011, 1).otherwise(0)).alias("total_mismatch"),
        F.sum(F.when((F.col("payment_type") == 2) & (F.col("tip_amount") > 0), 1).otherwise(0)).alias("cash_tip_gt0"),
        F.sum(F.when(F.col("payment_type") == 2, 1).otherwise(0)).alias("cash_rows"),
        F.sum(F.when(F.col("ratecodeid") == 2, 1).otherwise(0)).alias("jfk_rate_rows"),
        F.sum(F.when((F.col("ratecodeid") == 2) & (F.abs(fare - JFK_FLAT) <= 0.5), 1).otherwise(0)).alias("jfk_rate_fare70"),
        F.sum(F.when(F.col("airport_fee") > 0, 1).otherwise(0)).alias("airport_fee_rows"),
        F.sum(F.when((F.col("airport_fee") > 0) & ~airport_pu, 1).otherwise(0)).alias("airport_fee_outside_airport_pickup"),
        F.sum(F.when((F.col("payment_type") == 0) & (F.col("tip_amount") > 0), 1).otherwise(0)).alias("flex_with_tip"),
    ).first().asDict()
    n = r["n"]
    chk = [("total_amount khác tổng các khoản (> 0,01 USD)", r["total_mismatch"], n),
           ("tip > 0 trên chuyến trả TIỀN MẶT (dictionary: tip tiền mặt không được ghi)", r["cash_tip_gt0"], r["cash_rows"]),
           ("ratecodeid = 2 (JFK) nhưng fare không ≈ 70 USD", r["jfk_rate_rows"] - r["jfk_rate_fare70"], r["jfk_rate_rows"]),
           ("airport_fee > 0 nhưng điểm đón không phải JFK/LaGuardia", r["airport_fee_outside_airport_pickup"], r["airport_fee_rows"]),
           ("Flex Fare có tip > 0", r["flex_with_tip"], None)]
    v7 = pd.DataFrame([{"check": c, "violations": v, "out_of": o, "pct": (round(v / o * 100, 4) if o else None)} for c, v, o in chk])
    v7["note"] = ""
    if not airports:
        v7.loc[3, "note"] = "dim_zone không có JFK/LaGuardia -> không kiểm được"
    save(v7, "v7_consistency_checks.csv")
    show("V7 – kiểm tra nhất quán nghiệp vụ trên dữ liệu ĐẠT", v7)
    if not airports:
        print("  (dim_zone không có JFK/LaGuardia nên kiểm phí sân bay không có ý nghĩa)")

    spark.stop()
    print(f"\nXong sau {(time.time() - t_all) / 60:.1f} phút. Kết quả: {res}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

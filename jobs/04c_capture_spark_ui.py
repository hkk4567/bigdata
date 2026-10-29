#!/usr/bin/env python3
"""Chụp ảnh Spark UI cho báo cáo (mục 7.2) mà KHÔNG phải chạy lại cả bộ benchmark.

Vì sao cần: Spark UI chỉ tồn tại khi SparkSession còn sống. 04_benchmark.py chỉ lưu ảnh nếu chạy với --capture-ui và đã cài
Playwright. Script này chạy lại MỘT LẦN các truy vấn đại diện của từng thí nghiệm (không đo thời gian, không lấy trung vị),
dừng đúng lúc UI còn sống và chụp:
  - tab Jobs / Stages / Storage / Environment / Executors / SQL
  - trang chi tiết từng truy vấn SQL (sơ đồ DAG + số liệu từng toán tử: số dòng đọc, byte đọc, số file/partition được đọc)
  - 5 stage lâu nhất
kèm JSON và CSV tóm tắt từ REST API (jobs_summary.csv, stages_summary.csv, sql_summary.csv ...).

Mỗi thí nghiệm dùng một SparkSession MỚI để UI chỉ chứa truy vấn của thí nghiệm đó.
Truy vấn giống hệt 04_benchmark.py (cùng NEED_COLS, cùng phép gom ngày x khu đón, ghi noop):
  pruning : lọc theo cột phân vùng (month) và lọc bằng hàm month(pickup)   -> E2
  shuffle : partitions=16 (AQE tắt), 200 (AQE tắt), 200 (AQE bật)          -> E3
  format  : Parquet và CSV cùng 3 tháng                                    -> E1
  scale   : 1 tháng và 12 tháng                                            -> E5
  cores   : local[1] và local[8] (hoặc số nhân tối đa của máy)             -> E4

Yêu cầu: pip install playwright && python -m playwright install chromium   (không có thì chỉ lưu JSON/CSV)
Chạy (trong thư mục dự án, đã có data/clean_v2):
  python jobs/04c_capture_spark_ui.py                         # pruning, shuffle, format (đủ cho mục 7.2)
  python jobs/04c_capture_spark_ui.py --only pruning          # chỉ E2
  python jobs/04c_capture_spark_ui.py --only all              # cả 5 thí nghiệm
  python jobs/04c_capture_spark_ui.py --no-screenshots        # chỉ JSON/CSV
Ghi vào: results/04_benchmark/spark_ui_png/<tên>/   (thư mục riêng, không đè spark_ui/ do 04_benchmark --capture-ui tạo)
Nếu Playwright không tải được Chromium: đặt biến môi trường CHROMIUM_PATH tới chrome.exe có sẵn.
"""
import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, get_spark  # noqa: E402
from ui_capture import capture  # noqa: E402

NEED_COLS = ["tpep_pickup_datetime", "pulocationid", "fare_amount", "total_amount"]
DEFAULT = ["pruning", "shuffle", "format"]
ALL = ["pruning", "shuffle", "format", "scale", "cores"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--type", default="yellow")
    ap.add_argument("--clean", default=str(ROOT / "data" / "clean_v2"))
    ap.add_argument("--bench-data", default=str(ROOT / "data" / "bench"), help="nơi 04_benchmark.py đã tạo CSV cho thí nghiệm format")
    ap.add_argument("--out", default=str(ROOT / "results" / "04_benchmark" / "spark_ui_png"))
    ap.add_argument("--only", nargs="*", default=DEFAULT, choices=ALL + ["all"])
    ap.add_argument("--big-months", type=int, default=12, help="số tháng cho shuffle/cores (như 04_benchmark)")
    ap.add_argument("--cores", type=int, default=2)
    ap.add_argument("--driver-mem", default="3g")
    ap.add_argument("--no-screenshots", action="store_true", help="chỉ lưu JSON/CSV")
    a = ap.parse_args()
    exps = ALL if "all" in a.only else a.only
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    src = Path(a.clean) / a.type
    if not src.exists():
        print(f"Không thấy {src}. Chạy 01_ingest_to_lake.py và 02_clean_v2.py trước (hoặc dùng --clean).")
        return 1

    from pyspark.sql import functions as F

    def agg_noop(spark, df, label):
        """cùng phép gom với 04_benchmark.spark_run: (ngày, khu đón) -> số chuyến, doanh thu, fare TB; ghi 'noop' để ép chạy hết."""
        spark.sparkContext.setJobDescription(label)
        t0 = time.perf_counter()
        (df.select(*NEED_COLS)
         .groupBy(F.to_date("tpep_pickup_datetime").alias("d"), "pulocationid")
         .agg(F.count(F.lit(1)).alias("trips"), F.sum("total_amount").alias("revenue"), F.avg("fare_amount").alias("avg_fare"))
         .write.format("noop").mode("overwrite").save())
        print(f"  {label}: {time.perf_counter() - t0:.2f}s")

    def session(name, cores=None, partitions=16, aqe=None):
        spark = get_spark(name, cores=cores or a.cores, driver_mem=a.driver_mem, shuffle_partitions=partitions)
        if aqe is not None:
            spark.conf.set("spark.sql.adaptive.enabled", "true" if aqe else "false")
        return spark

    def shot(spark, name):
        capture(spark, out / name, tag=name, screenshots=not a.no_screenshots)

    parquet = lambda spark: spark.read.parquet(src.as_uri())  # noqa: E731
    months = lambda spark: max(r.month for r in parquet(spark).select("month").distinct().collect())  # noqa: E731

    if "pruning" in exps:
        print("== E2 pruning ==")
        spark = session("ui_pruning")
        m = min(6, months(spark))
        spark.sparkContext.setJobDescription("warm-up: đọc metadata")
        parquet(spark).where(F.col("month") == m).count()
        for label, df in ((f"E2 partition_filter month={m}", parquet(spark).where(F.col("month") == m)),
                          (f"E2 function_filter month={m}", parquet(spark).where(F.month("tpep_pickup_datetime") == m))):
            spark.sparkContext.setJobDescription(label)
            df.select(F.count(F.lit(1)).alias("n"), F.sum("total_amount").alias("rev")).first()
            print(f"  {label}: xong")
        shot(spark, "pruning")
        spark.stop()

    if "shuffle" in exps:
        print("== E3 shuffle partitions ==")
        for p, aqe in ((16, False), (200, False), (200, True)):
            spark = session(f"ui_shuffle_{p}", partitions=p, aqe=aqe)
            big = min(a.big_months, months(spark))
            tag = f"shuffle_p{p}_aqe_{'on' if aqe else 'off'}"
            agg_noop(spark, parquet(spark).where(F.col("month") <= big), f"E3 partitions={p} AQE={'on' if aqe else 'off'}")
            shot(spark, tag)
            spark.stop()

    if "format" in exps:
        print("== E1 Parquet và CSV ==")
        spark = session("ui_format")
        n = min(3, months(spark))
        csv_dir = Path(a.bench_data) / f"{a.type}_csv_{n}m"
        if csv_dir.exists():
            schema = parquet(spark).schema
            agg_noop(spark, parquet(spark).where(F.col("month") <= n), f"E1 parquet_{n}m")
            agg_noop(spark, spark.read.option("header", True).schema(schema).csv(csv_dir.as_uri()), f"E1 csv_{n}m")
            shot(spark, "format")
        else:
            print(f"  Không thấy {csv_dir} (04_benchmark.py tạo thư mục này). Chạy: python jobs/04_benchmark.py --experiments format")
        spark.stop()

    if "scale" in exps:
        print("== E5 quy mô ==")
        spark = session("ui_scale")
        top = months(spark)
        for n in sorted({1, top}):
            agg_noop(spark, parquet(spark).where(F.col("month") <= n), f"E5 spark_{n}m")
        shot(spark, "scale")
        spark.stop()

    if "cores" in exps:
        print("== E4 số nhân ==")
        for c in sorted({1, min(8, os.cpu_count() or 1)}):
            spark = session(f"ui_cores_{c}", cores=c)
            big = min(a.big_months, months(spark))
            agg_noop(spark, parquet(spark).where(F.col("month") <= big), f"E4 local[{c}]")
            shot(spark, f"cores_{c}")
            spark.stop()

    print(f"\nXong. Ảnh và JSON/CSV: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

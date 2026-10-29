#!/usr/bin/env python3
"""Thực nghiệm hiệu năng (mục 6 báo cáo): đo thời gian (latency) và thông lượng (dòng/giây) của cùng một khối lượng công việc.

Khối lượng công việc W: đọc data/clean_v2/yellow rồi gom theo (ngày đón, khu vực đón):
    số chuyến, tổng total_amount, fare trung bình   (~95 nghìn nhóm, có shuffle)
Mỗi cấu hình: 1 lượt khởi động (bỏ khỏi thống kê: nạp JVM, cache tệp) + N lượt đo (mặc định 3); báo median/min/max.
Spark đo bằng ghi ra sink "noop" (chạy đủ kế hoạch, không tốn I/O ghi). Pandas đo trọn đọc + gom.

Các thí nghiệm (--experiments):
  scale    kích thước dữ liệu 1/3/6/12 tháng: Spark (local[cores]) so với Pandas (1/3/6 tháng; 12 tháng chỉ khi --pandas-12)
  shuffle  spark.sql.shuffle.partitions = 2/8/16/64/200 (tắt AQE để thấy tác động thật) + 1 dòng AQE bật
  format   Parquet so với CSV (3 tháng): thời gian truy vấn + dung lượng đĩa + thời gian tạo CSV (lần đầu)
  cores    local[1], local[2], local[4]… (bỏ qua số lớn hơn số nhân CPU)
  pruning  lọc theo cột phân vùng (month = m: bỏ qua cả thư mục) so với lọc bằng hàm trên cột giờ (phải đọc hết)

Kết quả: results/04_benchmark/bench_runs.csv (từng lượt), bench_summary.csv, env.json.
Spark UI để đưa vào báo cáo: --capture-ui TỰ ĐỘNG lưu sau mỗi thí nghiệm vào results/04_benchmark/spark_ui/<thí nghiệm>/ :
  JSON/CSV từ REST API (jobs, stages, sql, executors, environment: đủ số liệu) + ảnh PNG full-page mọi tab và từng truy vấn SQL
  (ảnh cần: pip install playwright && python -m playwright install chromium). Xem jobs/ui_capture.py.
  Chụp tay: --pause (dừng cuối thí nghiệm đầu, mở http://localhost:4040).
Lưu ý: kết quả phụ thuộc máy; đóng các ứng dụng nặng khi đo; không thể so sánh trực tiếp với máy khác.

Chạy:  python jobs/04_benchmark.py                       (đủ thí nghiệm; có thể khá lâu)
       python jobs/04_benchmark.py --experiments scale   (một thí nghiệm)
       python jobs/04_benchmark.py --quick               (1 lượt đo, cỡ nhỏ: để thử trước)
"""
import argparse
import csv
import json
import os
import platform
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, get_spark, results_dir  # noqa: E402

NEED_COLS = ["tpep_pickup_datetime", "pulocationid", "fare_amount", "total_amount"]
ALL_EXPS = ["scale", "shuffle", "format", "cores", "pruning"]


def dir_size(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--type", default="yellow")
    ap.add_argument("--clean", default=str(ROOT / "data" / "clean_v2"))
    ap.add_argument("--bench-data", default=str(ROOT / "data" / "bench"), help="nơi lưu bản CSV tạo cho thí nghiệm format")
    ap.add_argument("--results", default=None, help="mặc định: results/04_benchmark")
    ap.add_argument("--experiments", nargs="*", default=["all"], choices=ALL_EXPS + ["all"])
    ap.add_argument("--sizes", nargs="*", type=int, default=[1, 3, 6, 12], help="số tháng cho thí nghiệm scale")
    ap.add_argument("--pandas-sizes", nargs="*", type=int, default=[1, 3, 6])
    ap.add_argument("--pandas-12", action="store_true", help="thử cả Pandas 12 tháng (có thể hết RAM 8 GB)")
    ap.add_argument("--big-months", type=int, default=12, help="số tháng cho shuffle/cores")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--cores", type=int, default=2)
    ap.add_argument("--driver-mem", default="3g")
    ap.add_argument("--rebuild-csv", action="store_true")
    ap.add_argument("--pause", action="store_true", help="dừng để chụp Spark UI bằng tay")
    ap.add_argument("--capture-ui", action="store_true", help="tự động lưu số liệu + ảnh Spark UI sau mỗi thí nghiệm")
    ap.add_argument("--no-screenshots", action="store_true", help="với --capture-ui: chỉ lưu JSON/CSV, không chụp ảnh")
    ap.add_argument("--quick", action="store_true", help="1 lượt đo, không khởi động, cỡ nhỏ")
    a = ap.parse_args()
    exps = ALL_EXPS if "all" in a.experiments else a.experiments
    if a.quick:
        a.runs, a.warmup, a.sizes, a.pandas_sizes = 1, 0, [1, 3], [1]
        a.big_months = 3

    import pandas as pd
    import pyarrow.parquet as pq
    from pyspark.sql import functions as F

    res = results_dir(__file__, a.results)
    src = Path(a.clean) / a.type
    spark = get_spark("benchmark", cores=a.cores, driver_mem=a.driver_mem)
    max_month = max(r.month for r in spark.read.parquet(src.as_uri()).select("month").distinct().collect())
    clip = lambda ns: sorted({min(n, max_month) for n in ns})  # noqa: E731
    sizes, pandas_sizes = clip(a.sizes), clip(a.pandas_sizes + ([12] if a.pandas_12 else []))
    big = min(a.big_months, max_month)
    (res / "env.json").write_text(json.dumps({
        "python": sys.version, "platform": platform.platform(), "cpu_count": os.cpu_count(),
        "pyspark": __import__("pyspark").__version__, "pandas": pd.__version__, "pyarrow": __import__("pyarrow").__version__,
        "spark_cores": a.cores, "driver_mem": a.driver_mem, "args": vars(a), "available_months": max_month,
    }, indent=2, ensure_ascii=False))

    log = open(res / "bench_runs.csv", "w", newline="", encoding="utf-8")
    lw = csv.writer(log)
    lw.writerow(["experiment", "config", "run", "is_warmup", "rows", "seconds", "rows_per_s", "note"])
    rows_cache = {}

    def n_rows(n_months):
        if n_months not in rows_cache:
            rows_cache[n_months] = spark.read.parquet(src.as_uri()).where(F.col("month") <= n_months).count()
        return rows_cache[n_months]

    def record(exp, cfg, fn, rows, note_fn=None):
        """fn() chạy một lượt, trả về ghi chú (tuỳ chọn). Khởi động + a.runs lượt đo."""
        for i in range(a.warmup + a.runs):
            warm = i < a.warmup
            t0 = time.perf_counter()
            try:
                note = fn() or ""
                secs = time.perf_counter() - t0
                status = ""
            except MemoryError:
                note, secs, status = "MemoryError", float("nan"), "FAIL"
            lw.writerow([exp, cfg, i + 1 - a.warmup if not warm else 0, warm, rows, f"{secs:.3f}",
                         f"{rows / secs:.0f}" if secs == secs and secs > 0 else "", note or status])
            log.flush()
            print(f"[{exp}] {cfg:<28} {'warmup' if warm else f'run {i + 1 - a.warmup}'}: {secs:7.2f}s  {note}")
            if status == "FAIL":
                break

    def agg_df(df):
        return (df.groupBy(F.to_date("tpep_pickup_datetime").alias("d"), "pulocationid")
                .agg(F.count(F.lit(1)).alias("trips"), F.sum("total_amount").alias("revenue"), F.avg("fare_amount").alias("avg_fare")))

    def spark_run(df, validate=[True]):
        out = agg_df(df.select(*NEED_COLS))
        if validate[0]:  # lượt đầu tiên của mỗi cấu hình: tính tổng kiểm chứng (chính là lượt khởi động)
            r = out.agg(F.sum("trips"), F.sum("revenue")).first()
            return f"check trips={int(r[0]):,} revenue={r[1]:,.0f}"
        out.write.format("noop").mode("overwrite").save()
        return ""

    def spark_fn(make_df):
        state = {"i": 0}

        def fn():
            first = state["i"] == 0
            state["i"] += 1
            return spark_run(make_df(), validate=[first and a.warmup > 0])
        return fn

    def pandas_fn(n_months):
        state = {"i": 0}

        def fn():
            t = pq.read_table(src, columns=NEED_COLS, filters=[("month", "<=", n_months)])
            df = t.to_pandas()
            df["d"] = df["tpep_pickup_datetime"].dt.floor("D")
            g = df.groupby(["d", "pulocationid"]).agg(trips=("fare_amount", "size"), revenue=("total_amount", "sum"),
                                                      avg_fare=("fare_amount", "mean"))
            note = f"check trips={int(g['trips'].sum()):,} revenue={g['revenue'].sum():,.0f}" if state["i"] == 0 and a.warmup > 0 else ""
            state["i"] += 1
            return note
        return fn

    first_exp_done = False

    def maybe_pause(exp=""):
        nonlocal first_exp_done
        if a.capture_ui:
            from ui_capture import capture
            capture(spark, res / "spark_ui" / exp, tag=exp, screenshots=not a.no_screenshots)
        if a.pause and not first_exp_done:
            input("\n>>> Mở http://localhost:4040 (Jobs, Stages, SQL), chụp ảnh màn hình rồi nhấn Enter để chạy tiếp... ")
        first_exp_done = True

    parquet_df = lambda n: (lambda: spark.read.parquet(src.as_uri()).where(F.col("month") <= n))  # noqa: E731

    if "scale" in exps:
        for n in sizes:
            record("scale", f"spark_{n}m", spark_fn(parquet_df(n)), n_rows(n))
        maybe_pause("scale")
        for n in pandas_sizes:
            record("scale", f"pandas_{n}m", pandas_fn(n), n_rows(n))

    if "shuffle" in exps:
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        for p in [2, 8, 16, 64, 200]:
            spark.conf.set("spark.sql.shuffle.partitions", str(p))
            record("shuffle", f"partitions={p} (AQE off)", spark_fn(parquet_df(big)), n_rows(big))
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.shuffle.partitions", "200")
        record("shuffle", "partitions=200 (AQE on)", spark_fn(parquet_df(big)), n_rows(big))
        spark.conf.set("spark.sql.shuffle.partitions", "16")
        maybe_pause("shuffle")

    if "format" in exps:
        n = min(3, max_month)
        csv_dir = Path(a.bench_data) / f"{a.type}_csv_{n}m"
        schema = spark.read.parquet(src.as_uri()).schema
        if a.rebuild_csv or not csv_dir.exists():
            t0 = time.perf_counter()
            (spark.read.parquet(src.as_uri()).where(F.col("month") <= n).write.mode("overwrite")
             .option("header", True).csv(csv_dir.as_uri()))
            print(f"[format] tạo CSV {n} tháng: {time.perf_counter() - t0:.1f}s")
            lw.writerow(["format", f"build_csv_{n}m", 0, True, n_rows(n), f"{time.perf_counter() - t0:.3f}", "", "một lần"])
        pq_size = sum(dir_size(src / "year=2024" / f"month={m}") for m in range(1, n + 1) if (src / "year=2024" / f"month={m}").exists())
        csv_size = dir_size(csv_dir)
        record("format", f"parquet_{n}m", spark_fn(parquet_df(n)), n_rows(n))
        csv_df = lambda: spark.read.option("header", True).schema(schema).csv(csv_dir.as_uri())  # noqa: E731
        record("format", f"csv_{n}m", spark_fn(csv_df), n_rows(n))
        lw.writerow(["format", "disk_bytes", 0, True, n_rows(n), "", "", f"parquet={pq_size} csv={csv_size} ratio_csv/parquet={csv_size / max(pq_size, 1):.2f}"])
        print(f"[format] đĩa: parquet {pq_size / 1e6:.0f} MB | csv {csv_size / 1e6:.0f} MB | csv/parquet = {csv_size / max(pq_size, 1):.2f}")
        maybe_pause("format")

    if "pruning" in exps:
        m = min(6, max_month)
        one = lambda df: df.select(F.count(F.lit(1)).alias("n"), F.sum("total_amount").alias("rev"))  # noqa: E731

        def prune_fn(use_partition):
            def fn():
                df = spark.read.parquet(src.as_uri())
                df = df.where(F.col("month") == m) if use_partition else df.where(F.month("tpep_pickup_datetime") == m)
                r = one(df).first()
                return f"n={r[0]:,}"
            return fn
        rows_m = spark.read.parquet(src.as_uri()).where(F.col("month") == m).count()
        record("pruning", f"partition_filter month={m}", prune_fn(True), rows_m)
        record("pruning", f"function_filter month={m}", prune_fn(False), rows_m)
        maybe_pause("pruning")

    if "cores" in exps:
        spark.stop()
        for c in [1, 2, 4, 8]:
            if c > (os.cpu_count() or 1):
                continue
            spark = get_spark(f"benchmark_c{c}", cores=c, driver_mem=a.driver_mem)
            rows_cache.clear()
            record("cores", f"local[{c}]", spark_fn(parquet_df(big)), n_rows(big))
            if a.capture_ui:
                from ui_capture import capture
                capture(spark, res / "spark_ui" / f"cores_{c}", tag=f"cores_{c}", screenshots=not a.no_screenshots)
            spark.stop()
        spark = get_spark("benchmark", cores=a.cores, driver_mem=a.driver_mem)
    log.close()

    runs = pd.read_csv(res / "bench_runs.csv")
    m = runs[(~runs["is_warmup"].astype(bool)) & runs["seconds"].notna()]
    rows = []
    for (e, c), g in m.groupby(["experiment", "config"], sort=False):
        med = statistics.median(g["seconds"])
        rows.append({"experiment": e, "config": c, "rows": int(g["rows"].iloc[0]), "runs": len(g), "median_s": round(med, 2),
                     "min_s": round(g["seconds"].min(), 2), "max_s": round(g["seconds"].max(), 2),
                     "rows_per_s_median": int(g["rows"].iloc[0] / med) if med > 0 else ""})
    summ = pd.DataFrame(rows)
    summ.to_csv(res / "bench_summary.csv", index=False)
    print("\n=== TỔNG HỢP (median của các lượt đo, không tính khởi động)")
    print(summ.to_string(index=False))
    spark.stop()
    print(f"\nKết quả: {res}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Bước 4b - ĐO RAM: Pandas có thật sự "hết RAM" khi gom 12 tháng không? Spark dùng bao nhiêu?

Mỗi lượt đo chạy trong MỘT TIẾN TRÌNH CON RIÊNG (bộ nhớ sạch, không dính lượt trước). Tiến trình cha
lấy mẫu RSS (RAM thật) của cả cây tiến trình (Python + JVM của Spark) mỗi 0,1 giây và ghi đỉnh.

Công cụ (engine):
  pandas_4col  đọc 4 cột cần thiết (giống thí nghiệm scale ở 04_benchmark) rồi groupby
  pandas_all   đọc TẤT CẢ cột của dữ liệu sạch (kịch bản "cứ đọc hết"), rồi groupby
  spark        Spark local[cores], driver-mem như 04_benchmark, ghi ra sink noop

AN TOÀN: nếu RSS cả cây vượt --rss-limit-gb (mặc định 5,5) hoặc quá --timeout giây, tiến trình cha DỪNG
lượt đó và ghi status = KILLED_RSS_LIMIT / KILLED_TIMEOUT. Đây là ngưỡng do mình đặt để máy khỏi treo,
KHÔNG phải "hết RAM" tự nhiên - báo cáo phải nói rõ như vậy. status OOM chỉ ghi khi tiến trình tự chết
vì MemoryError. Máy có ~8 GB: hệ điều hành + trình duyệt đã chiếm 2-3 GB, nên hãy đóng ứng dụng khác khi chạy.

Chạy:
  python jobs/04b_memory_benchmark.py                       # engine x {1,3,6,12} tháng, 1 lượt mỗi ô
  python jobs/04b_memory_benchmark.py --months 1 3 --quick  # thử nhanh
  python jobs/04b_memory_benchmark.py --rss-limit-gb 6.5    # nới ngưỡng nếu máy còn trống RAM
Kết quả: results/04b_memory_benchmark/mem_runs.csv, env.json
"""
import argparse
import csv
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, get_spark, results_dir  # noqa: E402

NEED_COLS = ["tpep_pickup_datetime", "pulocationid", "fare_amount", "total_amount"]
ENGINES = ["pandas_4col", "pandas_all", "spark"]


# ------------------------------------------------------------------ tiến trình con
def worker(a):
    """Chạy đúng một lượt và in một dòng JSON 'RESULT {...}' ra stdout."""
    import pyarrow.parquet as pq

    src = Path(a.clean) / a.type
    t0 = time.perf_counter()
    if a.engine.startswith("pandas"):
        cols = NEED_COLS if a.engine == "pandas_4col" else None
        df = pq.read_table(src, columns=cols, filters=[("month", "<=", a.n)]).to_pandas()
        rows = len(df)
        df["d"] = df["tpep_pickup_datetime"].dt.floor("D")
        g = df.groupby(["d", "pulocationid"]).agg(trips=("fare_amount", "size"), revenue=("total_amount", "sum"))
        check = f"trips={int(g['trips'].sum()):,} groups={len(g):,}"
    else:
        from pyspark.sql import functions as F
        spark = get_spark("mem_benchmark", cores=a.cores, driver_mem=a.driver_mem)
        df = spark.read.parquet(src.as_uri()).where(F.col("month") <= a.n).select(*NEED_COLS)
        out = (df.groupBy(F.to_date("tpep_pickup_datetime").alias("d"), "pulocationid")
               .agg(F.count(F.lit(1)).alias("trips"), F.sum("total_amount").alias("revenue")))
        r = out.agg(F.sum("trips"), F.count(F.lit(1))).first()
        rows, check = int(r[0]), f"trips={int(r[0]):,} groups={int(r[1]):,}"
        spark.stop()
    secs = time.perf_counter() - t0
    print("RESULT " + json.dumps({"rows": rows, "seconds": round(secs, 2), "check": check}), flush=True)


# ------------------------------------------------------------------ tiến trình cha
def tree_rss(proc):
    import psutil
    try:
        procs = [proc] + proc.children(recursive=True)
    except psutil.Error:
        return 0
    total = 0
    for p in procs:
        try:
            total += p.memory_info().rss
        except psutil.Error:
            pass
    return total


def run_one(a, engine, n):
    import psutil
    cmd = [sys.executable, str(Path(__file__).resolve()), "--worker", "--engine", engine, "--n", str(n),
           "--clean", a.clean, "--type", a.type, "--cores", str(a.cores), "--driver-mem", a.driver_mem]
    t0 = time.perf_counter()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    ps = psutil.Process(p.pid)
    peak, min_avail, status = 0, psutil.virtual_memory().available, ""
    limit = int(a.rss_limit_gb * 1024 ** 3)
    while p.poll() is None:
        rss = tree_rss(ps)
        peak = max(peak, rss)
        min_avail = min(min_avail, psutil.virtual_memory().available)
        if rss > limit:
            status = "KILLED_RSS_LIMIT"
        elif time.perf_counter() - t0 > a.timeout:
            status = "KILLED_TIMEOUT"
        if status:
            for c in ps.children(recursive=True):
                try:
                    c.kill()
                except psutil.Error:
                    pass
            p.kill()
            break
        time.sleep(0.1)
    out, err = p.communicate()
    secs = time.perf_counter() - t0
    info = {}
    for line in out.splitlines():
        if line.startswith("RESULT "):
            info = json.loads(line[7:])
    if not status:
        if info:
            status = "OK"
        else:
            status = "OOM" if "MemoryError" in err else f"FAILED(exit {p.returncode})"
    note = info.get("check", "") if info else (err.strip().splitlines()[-1][:150] if err.strip() else "")
    return {"engine": engine, "months": n, "rows": info.get("rows", ""), "status": status,
            "worker_seconds": info.get("seconds", ""), "wall_seconds": round(secs, 2),
            "peak_rss_mb": round(peak / 1024 ** 2), "min_available_mb": round(min_avail / 1024 ** 2), "note": note}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--type", default="yellow")
    ap.add_argument("--clean", default=str(ROOT / "data" / "clean_v2"))
    ap.add_argument("--results", default=None, help="mặc định: results/04b_memory_benchmark")
    ap.add_argument("--engines", nargs="*", default=ENGINES, choices=ENGINES)
    ap.add_argument("--months", nargs="*", type=int, default=[1, 3, 6, 12])
    ap.add_argument("--cores", type=int, default=2)
    ap.add_argument("--driver-mem", default="3g")
    ap.add_argument("--rss-limit-gb", type=float, default=5.5, help="vượt ngưỡng này thì dừng lượt đo (bảo vệ máy)")
    ap.add_argument("--timeout", type=int, default=900, help="giây tối đa mỗi lượt")
    ap.add_argument("--quick", action="store_true", help="chỉ tháng 1 và 3")
    ap.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--engine", help=argparse.SUPPRESS)
    ap.add_argument("--n", type=int, help=argparse.SUPPRESS)
    a = ap.parse_args()
    if a.worker:
        return worker(a)
    if a.quick:
        a.months = [1, 3]

    import psutil
    import pyarrow.parquet as pq
    res = results_dir(__file__, a.results)
    vm = psutil.virtual_memory()
    max_month = int(max(pq.read_table(Path(a.clean) / a.type, columns=["month"]).column("month").unique().to_pylist()))
    months = sorted({min(n, max_month) for n in a.months})
    (res / "env.json").write_text(json.dumps({
        "python": sys.version, "platform": platform.platform(), "cpu_count": os.cpu_count(),
        "total_ram_mb": round(vm.total / 1024 ** 2), "available_ram_mb_at_start": round(vm.available / 1024 ** 2),
        "args": vars(a), "available_months": max_month}, indent=2, ensure_ascii=False))
    print(f"RAM máy: {vm.total / 1024 ** 3:.1f} GB, còn trống {vm.available / 1024 ** 3:.1f} GB. Ngưỡng dừng: {a.rss_limit_gb} GB RSS.")

    fields = ["engine", "months", "rows", "status", "worker_seconds", "wall_seconds", "peak_rss_mb", "min_available_mb", "note"]
    with open(res / "mem_runs.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for engine in a.engines:
            for n in months:
                r = run_one(a, engine, n)
                w.writerow(r)
                f.flush()
                print(f"[{engine:<11}] {n:>2} tháng: {r['status']:<18} đỉnh RAM {r['peak_rss_mb']:>6} MB, "
                      f"{r['wall_seconds']:>7} s  {r['note']}")
                if r["status"] != "OK":  # lớn hơn chắc chắn cũng hỏng: bỏ qua cho đỡ treo máy
                    print(f"    -> bỏ qua các cỡ lớn hơn của {engine}")
                    break
    print(f"\nXong -> {res / 'mem_runs.csv'}")


if __name__ == "__main__":
    main()

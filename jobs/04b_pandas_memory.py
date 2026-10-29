#!/usr/bin/env python3
"""Đo BỘ NHỚ đỉnh (peak RAM) và thời gian của Pandas so với Spark trên cùng một phép gom (bổ sung cho 04_benchmark.py).

Mục đích: trả lời có bằng chứng "Pandas có chạy nổi 12 tháng trên máy 8 GB không, và tốn bao nhiêu RAM?"
(04_benchmark.py chỉ đo thời gian, chưa đo RAM, và Pandas mới chạy đến 6 tháng).

Phép gom giống 04_benchmark.py: gom theo (ngày đón, khu vực đón) -> số chuyến, tổng total_amount, fare trung bình.
Ba "động cơ":
  pandas_narrow  Pandas, chỉ đọc 4 cột cần dùng (cách làm tối ưu của Pandas)
  pandas_wide    Pandas, đọc TẤT CẢ cột (cách làm ngây thơ: read_parquet rồi groupby)
  spark          Spark local[cores], driver-mem như 04_benchmark (cần ~ cùng 4 cột; ghi sink noop)

Cách đo (quan trọng, để kết quả đáng tin):
  - MỖI lượt chạy trong một TIẾN TRÌNH CON riêng, nên bộ nhớ không dính lượt trước và nếu hết RAM chỉ tiến trình con chết.
  - Tiến trình cha lấy mẫu RSS (bộ nhớ thật) của cả cây tiến trình con (Spark có JVM là tiến trình con) mỗi 50 ms, giữ giá trị đỉnh.
  - Thời gian là của phần truy vấn (đọc + gom), không tính khởi động Python/JVM.
  - --limit-gb N: cha tự giết tiến trình con khi RSS vượt N GB (giả lập máy có N GB RAM; ghi trạng thái OVER_LIMIT).
  - Trạng thái: OK | MEMORY_ERROR (Python MemoryError) | KILLED (bị hệ điều hành giết, thường do hết RAM) | OVER_LIMIT | TIMEOUT.
Lưu ý: RSS đỉnh phụ thuộc máy/HĐH; đóng ứng dụng nặng khi đo. Không chạy song song với job khác.

Chạy:  python jobs/04b_pandas_memory.py                       (1/3/6/12 tháng, 3 động cơ, 3 lượt; có thể hơi lâu)
       python jobs/04b_pandas_memory.py --sizes 12 --engines pandas_wide pandas_narrow
       python jobs/04b_pandas_memory.py --dataset fhvhv --sizes 1 2   (HVFHV từ data/lake/fhvhv; kết quả ở results/04b_pandas_memory/fhvhv/)
       python jobs/04b_pandas_memory.py --limit-gb 6          (coi như máy chỉ còn 6 GB trống)
Kết quả: results/04b_pandas_memory/mem_runs.csv (từng lượt), mem_summary.csv, env.json.
"""
import argparse
import csv
import json
import os
import platform
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, results_dir  # noqa: E402

# Mỗi bộ dữ liệu: nơi đọc, tên cột thời gian đón / khu đón / số tiền dùng để đếm-trung bình / số tiền để cộng
DATASETS = {
    "yellow": dict(type="yellow", root=ROOT / "data" / "clean_v2", ts="tpep_pickup_datetime", zone="pulocationid", avg="fare_amount", total="total_amount"),
    # HVFHV đọc thẳng từ lake (chưa làm sạch): doanh thu = base_passenger_fare, trung bình = driver_pay
    "fhvhv": dict(type="fhvhv", root=ROOT / "data" / "lake", ts="pickup_datetime", zone="pulocationid", avg="driver_pay", total="base_passenger_fare"),
}
ENGINES = ("pandas_narrow", "pandas_wide", "spark")
MARK = "@@RESULT@@"


# ----------------------------------------------------------------------------- tiến trình con
def child_main(a):
    """Chạy MỘT lượt rồi in một dòng JSON bắt đầu bằng MARK. Mọi lỗi bộ nhớ -> thoát với mã rõ ràng."""
    ds = DATASETS[a.dataset]
    NEED_COLS = [ds["ts"], ds["zone"], ds["avg"], ds["total"]]
    src = Path(a.clean) / ds["type"]
    n = a.child_months
    try:
        if a.child.startswith("pandas"):
            import pyarrow.parquet as pq
            t0 = time.perf_counter()
            cols = NEED_COLS if a.child == "pandas_narrow" else None
            df = pq.read_table(src, columns=cols, filters=[("month", "<=", n)]).to_pandas()
            rows = len(df)
            df["d"] = df[ds["ts"]].dt.floor("D")
            g = df.groupby(["d", ds["zone"]]).agg(trips=(ds["avg"], "size"), revenue=(ds["total"], "sum"),
                                                  avg_fare=(ds["avg"], "mean"))
            secs = time.perf_counter() - t0
            out = {"rows": rows, "seconds": secs, "trips": int(g["trips"].sum()), "revenue": float(g["revenue"].sum())}
        else:
            from common import get_spark
            from pyspark.sql import functions as F
            spark = get_spark("bench_mem", cores=a.cores, driver_mem=a.driver_mem)
            df = spark.read.parquet(src.as_uri()).where(F.col("month") <= n).select(*NEED_COLS)
            q = (df.groupBy(F.to_date(ds["ts"]).alias("d"), ds["zone"])
                 .agg(F.count(F.lit(1)).alias("trips"), F.sum(ds["total"]).alias("revenue"), F.avg(ds["avg"]).alias("avg_fare")))
            t0 = time.perf_counter()
            r = q.agg(F.sum("trips"), F.sum("revenue")).first()  # lượt đo: cùng kế hoạch, tổng kiểm chứng
            secs = time.perf_counter() - t0
            out = {"rows": int(r[0]), "seconds": secs, "trips": int(r[0]), "revenue": float(r[1])}
            spark.stop()
    except MemoryError:
        print(MARK + json.dumps({"status": "MEMORY_ERROR"}), flush=True)
        sys.exit(3)
    out["status"] = "OK"
    print(MARK + json.dumps(out), flush=True)


# ----------------------------------------------------------------------------- tiến trình cha
def tree_rss(proc):
    import psutil
    try:
        procs = [proc] + proc.children(recursive=True)
    except psutil.Error:
        return 0
    tot = 0
    for p in procs:
        try:
            tot += p.memory_info().rss
        except psutil.Error:
            pass
    return tot


def run_one(a, engine, months):
    import psutil
    cmd = [sys.executable, str(Path(__file__).resolve()), "--child", engine, "--child-months", str(months),
           "--clean", a.clean, "--dataset", a.dataset, "--cores", str(a.cores), "--driver-mem", a.driver_mem]
    t0 = time.perf_counter()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    ps = psutil.Process(p.pid)
    peak, over, timeout = 0, False, False
    out_buf, err_buf = [], []
    th_o = threading.Thread(target=lambda: out_buf.extend(p.stdout), daemon=True)
    th_e = threading.Thread(target=lambda: err_buf.extend(p.stderr), daemon=True)
    th_o.start()
    th_e.start()
    while p.poll() is None:
        peak = max(peak, tree_rss(ps))
        if a.limit_gb and peak > a.limit_gb * 1024 ** 3:
            over = True
        if over or time.perf_counter() - t0 > a.timeout:
            timeout = not over
            for c in ps.children(recursive=True) + [ps]:
                try:
                    c.kill()
                except psutil.Error:
                    pass
            break
        time.sleep(0.05)
    p.wait()
    th_o.join(5)
    th_e.join(5)
    wall = time.perf_counter() - t0
    res = {}
    for line in out_buf:
        if line.startswith(MARK):
            res = json.loads(line[len(MARK):])
    if over:
        res = {"status": "OVER_LIMIT"}
    elif timeout:
        res = {"status": "TIMEOUT"}
    elif not res:
        res = {"status": "KILLED" if p.returncode in (-9, 137, 3221225477, -1073741819) or p.returncode < 0 else f"ERROR({p.returncode})"}
        if os.environ.get("BENCH_DEBUG") or res["status"].startswith("ERROR"):
            sys.stderr.write("".join(err_buf)[-1500:])
    res["peak_rss_gb"] = peak / 1024 ** 3
    res["wall_s"] = wall
    return res


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", choices=list(DATASETS), default="yellow", help="yellow: data/clean_v2; fhvhv: data/lake (HVFHV chưa làm sạch)")
    ap.add_argument("--clean", default=None, help="thư mục gốc chứa <type>/ (mặc định theo --dataset)")
    ap.add_argument("--sizes", nargs="*", type=int, default=[1, 3, 6, 12])
    ap.add_argument("--engines", nargs="*", choices=ENGINES, default=list(ENGINES))
    ap.add_argument("--runs", type=int, default=3, help="số lượt đo mỗi cấu hình (mỗi lượt là một tiến trình mới)")
    ap.add_argument("--cores", type=int, default=2)
    ap.add_argument("--driver-mem", default="3g")
    ap.add_argument("--limit-gb", type=float, default=0.0, help="giết tiến trình con nếu RSS vượt ngưỡng này (0 = không giới hạn)")
    ap.add_argument("--timeout", type=float, default=1800.0, help="giây tối đa mỗi lượt")
    ap.add_argument("--results", default=None)
    ap.add_argument("--child", help=argparse.SUPPRESS)
    ap.add_argument("--child-months", type=int, help=argparse.SUPPRESS)
    a = ap.parse_args()
    a.clean = a.clean or str(DATASETS[a.dataset]["root"])
    if a.child:
        a.child_months = a.child_months
        return child_main(a)

    import psutil
    res = results_dir(__file__, a.results)
    if a.dataset != "yellow" and not a.results:  # bộ khác ghi vào thư mục con riêng
        res = res / a.dataset
        res.mkdir(parents=True, exist_ok=True)
    if a.limit_gb and not a.results:  # lần chạy có giới hạn RAM ghi vào thư mục con để không đè kết quả đầy đủ
        res = res / f"limit_{a.limit_gb:g}gb"
        res.mkdir(parents=True, exist_ok=True)
    (res / "env.json").write_text(json.dumps({
        "python": sys.version, "platform": platform.platform(), "cpu_count": os.cpu_count(),
        "ram_total_gb": round(psutil.virtual_memory().total / 1024 ** 3, 2),
        "ram_available_gb_at_start": round(psutil.virtual_memory().available / 1024 ** 3, 2),
        "args": vars(a),
    }, indent=2, ensure_ascii=False))
    print(f"RAM tổng {psutil.virtual_memory().total / 1024 ** 3:.1f} GB, trống {psutil.virtual_memory().available / 1024 ** 3:.1f} GB. "
          f"Đóng ứng dụng nặng trước khi đo.")

    runs_f = open(res / "mem_runs.csv", "w", newline="", encoding="utf-8")
    w = csv.writer(runs_f)
    w.writerow(["engine", "months", "run", "status", "rows", "query_seconds", "wall_seconds", "peak_rss_gb", "trips_check", "revenue_check"])
    table = {}
    for engine in a.engines:
        for m in sorted(a.sizes):
            for i in range(1, a.runs + 1):
                r = run_one(a, engine, m)
                w.writerow([engine, m, i, r["status"], r.get("rows", ""), f"{r['seconds']:.3f}" if "seconds" in r else "",
                            f"{r['wall_s']:.1f}", f"{r['peak_rss_gb']:.3f}", r.get("trips", ""), f"{r['revenue']:.2f}" if "revenue" in r else ""])
                runs_f.flush()
                print(f"[{engine:<13}] {m:>2} tháng  lượt {i}: {r['status']:<12} "
                      f"{('%.2fs' % r['seconds']) if 'seconds' in r else '   -  '}  đỉnh RAM {r['peak_rss_gb']:.2f} GB")
                table.setdefault((engine, m), []).append(r)
                if r["status"] != "OK":
                    break  # không thử lại cấu hình đã hỏng
    runs_f.close()

    with open(res / "mem_summary.csv", "w", newline="", encoding="utf-8") as f:
        sw = csv.writer(f)
        sw.writerow(["engine", "months", "rows", "ok_runs", "status", "median_query_s", "median_peak_rss_gb", "max_peak_rss_gb"])
        for (engine, m), rs in table.items():
            ok = [r for r in rs if r["status"] == "OK"]
            status = "OK" if ok and len(ok) == len(rs) else rs[-1]["status"]
            sw.writerow([engine, m, ok[0]["rows"] if ok else "", len(ok), status,
                         f"{statistics.median(r['seconds'] for r in ok):.3f}" if ok else "",
                         f"{statistics.median(r['peak_rss_gb'] for r in rs):.3f}", f"{max(r['peak_rss_gb'] for r in rs):.3f}"])
    print(f"\nXong -> {res}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Kiểm tra môi trường: Java, PySpark, ghi/đọc Parquet, tổng hợp. Ghi kết quả vào results/00_check_env/env_check.json.

Chạy:  python jobs/00_check_env.py
"""
import json
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import get_spark, results_dir  # noqa: E402


def java_version():
    if not shutil.which("java"):
        return None
    p = subprocess.run(["java", "-version"], capture_output=True, text=True)
    return (p.stderr or p.stdout).splitlines()[0]


def main():
    info = {"python": sys.version.split()[0], "os": platform.platform(), "java": java_version()}
    if info["java"] is None:
        print("THIẾU Java. Cài JDK 17 rồi chạy lại (xem README).")
        return 1

    import pyspark
    from pyspark.sql import functions as F

    info["pyspark"] = pyspark.__version__
    spark = get_spark("check_env")
    t0 = time.time()
    n = 2_000_000
    df = spark.range(n).withColumn("zone", (F.col("id") % 265).cast("int")).withColumn("fare", F.rand(7) * 50)
    tmp = tempfile.mkdtemp(prefix="bd_check_")
    try:
        path = (Path(tmp) / "t.parquet").as_uri()  # file:///... chạy được cả trên Windows
        df.write.partitionBy("zone").mode("overwrite").parquet(path)
        back = spark.read.parquet(path)
        agg = back.groupBy("zone").agg(F.count("*").alias("trips"), F.avg("fare").alias("avg_fare"))
        rows = agg.count()
        total = agg.agg(F.sum("trips")).first()[0]
    finally:
        spark.stop()
        shutil.rmtree(tmp, ignore_errors=True)  # Windows hay giữ khoá file, nên bỏ qua lỗi xoá
    info.update({"rows_generated": n, "zones_aggregated": rows, "rows_after_roundtrip": total,
                 "seconds": round(time.time() - t0, 2), "ok": total == n and rows == 265})

    out = results_dir(__file__) / "env_check.json"
    out.write_text(json.dumps(info, indent=2, ensure_ascii=False))
    print(json.dumps(info, indent=2, ensure_ascii=False))
    print("OK – môi trường sẵn sàng." if info["ok"] else "LỖI – kết quả không khớp.")
    return 0 if info["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

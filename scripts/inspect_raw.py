#!/usr/bin/env python3
"""Kiểm tra schema/số dòng của các file Parquet thô bằng pyarrow (không cần Spark, rất nhanh).

Chạy:  python scripts/inspect_raw.py [--raw data/raw] [--type yellow]
Ghi:   results/inspect_raw/schema_inspection.csv (file, cột, kiểu) và results/inspect_raw/raw_files_summary.csv (file, số dòng, dung lượng)
In ra: các cột có kiểu khác nhau hoặc thiếu giữa các tháng -> đầu vào cho bước ép schema ở 01_ingest_to_lake.py
"""
import argparse
import csv
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default=str(ROOT / "data" / "raw"))
    ap.add_argument("--type", default="yellow")
    ap.add_argument("--results", default=str(ROOT / "results" / "inspect_raw"))
    a = ap.parse_args()

    files = sorted(Path(a.raw).glob(f"{a.type}_tripdata_*.parquet"))
    if not files:
        print(f"Không thấy file {a.type}_tripdata_*.parquet trong {a.raw}")
        return 1
    res = Path(a.results)
    res.mkdir(parents=True, exist_ok=True)

    types = defaultdict(dict)  # cột (chữ thường) -> {file: kiểu}
    summary, total = [], 0
    with open(res / "schema_inspection.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["file", "column", "arrow_type"])
        for p in files:
            pf = pq.ParquetFile(p)
            for fld in pf.schema_arrow:
                w.writerow([p.name, fld.name, str(fld.type)])
                types[fld.name.lower()][p.name] = f"{fld.name}:{fld.type}"
            n = pf.metadata.num_rows
            total += n
            summary.append([p.name, n, pf.metadata.num_row_groups, p.stat().st_size])
    with open(res / "raw_files_summary.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["file", "rows", "row_groups", "bytes"])
        w.writerows(summary)

    print(f"{len(files)} file, tổng {total:,} dòng, {sum(s[3] for s in summary)/1e6:.0f} MB")
    for s in summary:
        print(f"  {s[0]:<34} {s[1]:>12,} dòng  {s[3]/1e6:7.1f} MB")
    print("\nCột khác kiểu / thiếu giữa các file:")
    any_diff = False
    for col, per_file in sorted(types.items()):
        variants = set(per_file.values())
        missing = [p.name for p in files if p.name not in per_file]
        if len(variants) > 1 or missing:
            any_diff = True
            print(f"  - {col}: {sorted(variants)}" + (f"  (thiếu ở {len(missing)} file, vd {missing[0]})" if missing else ""))
    if not any_diff:
        print("  (không có – schema đồng nhất)")
    print(f"\nĐã ghi {res / 'schema_inspection.csv'} và {res / 'raw_files_summary.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

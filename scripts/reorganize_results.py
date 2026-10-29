#!/usr/bin/env python3
"""Chuyển các file kết quả cũ (nằm phẳng trong results/) vào thư mục riêng của từng bước. An toàn khi chạy nhiều lần.

Chạy:  python scripts/reorganize_results.py            # thực hiện
       python scripts/reorganize_results.py --dry-run  # chỉ in ra việc sẽ làm
Quy ước mới: results/<tên script không đuôi>/  (xem README).
"""
import argparse
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MOVES = {
    "download_data": ["download_log.csv"],
    "00_check_env": ["env_check.json"],
    "inspect_raw": ["schema_inspection.csv", "raw_files_summary.csv"],
    "01_ingest_to_lake": ["ingest_log.csv"],
    "02a_profile_raw": ["dq_profile.csv", "dq_profile_by_month.csv", "dq_nulls.csv", "dq_quantiles.csv",
                        "dq_categorical.csv", "dq_duplicates.csv"],
}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    res, moved = ROOT / "results", 0
    for folder, names in MOVES.items():
        for n in names:
            src, dst = res / n, res / folder / n
            if not src.exists():
                continue
            if dst.exists():
                print(f"BỎ QUA (đã có ở đích): {src.name} -> {folder}/")
                continue
            print(f"{src.name} -> {folder}/")
            if not a.dry_run:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(src), str(dst))
            moved += 1
    print(f"Xong. {'Sẽ chuyển' if a.dry_run else 'Đã chuyển'} {moved} file.")


if __name__ == "__main__":
    main()

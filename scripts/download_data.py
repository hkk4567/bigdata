#!/usr/bin/env python3
"""Tải NYC TLC Trip Record Data (Parquet) theo loại xe và khoảng tháng.

Ví dụ:
  python scripts/download_data.py --type yellow --start 2024-01 --end 2024-12
  python scripts/download_data.py --type fhvhv --start 2024-01 --end 2024-01
  python scripts/download_data.py --zones-only
  python scripts/download_data.py --type yellow --start 2024-01 --end 2024-03 --dry-run

Đặc điểm: chỉ dùng thư viện chuẩn; bỏ qua file đã tải đủ; tải qua file .part
rồi đổi tên (không để lại file dở); thử lại khi lỗi mạng; ghi log vào results/download_data/download_log.csv.
"""
import argparse
import csv
import datetime as dt
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
TRIP_PATH = "trip-data/{type}_tripdata_{ym}.parquet"
ZONES_PATH = "misc/taxi_zone_lookup.csv"
TYPES = ("yellow", "green", "fhv", "fhvhv")
ROOT = Path(__file__).resolve().parent.parent


def month_range(start: str, end: str):
    y, m = map(int, start.split("-"))
    ye, me = map(int, end.split("-"))
    while (y, m) <= (ye, me):
        yield f"{y:04d}-{m:02d}"
        m += 1
        if m == 13:
            y, m = y + 1, 1


def remote_size(url: str):
    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req, timeout=30) as r:
        n = r.headers.get("Content-Length")
        return int(n) if n else None


def download(url: str, dest: Path, retries: int = 3):
    """Trả về (trạng thái, số byte, giây)."""
    t0 = time.time()
    try:
        expected = remote_size(url)
    except urllib.error.HTTPError as e:
        return f"http_{e.code}", 0, 0.0
    except Exception as e:  # mạng lỗi khi HEAD
        expected = None
        print(f"  (không đọc được kích thước: {e})")
    if dest.exists() and expected is not None and dest.stat().st_size == expected:
        return "skipped_exists", dest.stat().st_size, 0.0
    part = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=60) as r, open(part, "wb") as f:
                while chunk := r.read(1 << 20):
                    f.write(chunk)
            if expected is not None and part.stat().st_size != expected:
                raise IOError(f"kích thước lệch: {part.stat().st_size} != {expected}")
            part.replace(dest)
            return "downloaded", dest.stat().st_size, time.time() - t0
        except Exception as e:
            print(f"  lần {attempt}/{retries} lỗi: {e}")
            time.sleep(2 * attempt)
    return "failed", 0, time.time() - t0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--type", choices=TYPES, default="yellow")
    ap.add_argument("--start", help="tháng bắt đầu, dạng YYYY-MM")
    ap.add_argument("--end", help="tháng kết thúc, dạng YYYY-MM")
    ap.add_argument("--out", default=str(ROOT / "data" / "raw"))
    ap.add_argument("--base-url", default=BASE_URL, help="đổi nguồn (dùng để test)")
    ap.add_argument("--zones-only", action="store_true", help="chỉ tải bảng taxi zone lookup")
    ap.add_argument("--no-zones", action="store_true", help="không tải bảng taxi zone lookup")
    ap.add_argument("--dry-run", action="store_true", help="chỉ in URL, không tải")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    jobs = []
    if not args.zones_only:
        if not (args.start and args.end):
            ap.error("cần --start và --end (hoặc dùng --zones-only)")
        for ym in month_range(args.start, args.end):
            jobs.append((f"{args.base_url}/{TRIP_PATH.format(type=args.type, ym=ym)}",
                         out / f"{args.type}_tripdata_{ym}.parquet"))
    if not args.no_zones:
        jobs.append((f"{args.base_url}/{ZONES_PATH}", out / "taxi_zone_lookup.csv"))

    if args.dry_run:
        for url, dest in jobs:
            print(f"{url}\n  -> {dest}")
        return 0

    log_path = ROOT / "results" / "download_data" / "download_log.csv"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    new_log = not log_path.exists()
    failed = 0
    with open(log_path, "a", newline="") as lf:
        w = csv.writer(lf)
        if new_log:
            w.writerow(["timestamp", "file", "status", "bytes", "seconds"])
        for url, dest in jobs:
            print(f"[{dest.name}] ...")
            status, size, secs = download(url, dest)
            mb = size / 1e6
            print(f"  {status}  {mb:.1f} MB  {secs:.1f}s")
            w.writerow([dt.datetime.now().isoformat(timespec="seconds"), dest.name, status, size, f"{secs:.2f}"])
            failed += status not in ("downloaded", "skipped_exists")
    print(f"Xong. Lỗi/không có: {failed}/{len(jobs)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

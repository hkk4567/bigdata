#!/usr/bin/env python3
"""Windows only: tải winutils.exe + hadoop.dll vào tools/hadoop/bin (trong dự án, không cần quyền admin).

Vì sao cần: Spark dùng thư viện Hadoop để ghi file cục bộ; trên Windows Hadoop gọi winutils.exe
để đặt quyền thư mục. Thiếu file này sẽ gặp lỗi "HADOOP_HOME and hadoop.home.dir are unset".

Chạy:  python scripts/setup_windows_hadoop.py
Sau đó jobs/common.py tự đặt HADOOP_HOME trỏ tới tools/hadoop (không cần cấu hình thêm).

Lưu ý bảo mật: đây là bản build không chính thức của cộng đồng (repo cdarlint/winutils, Hadoop 3.3.6),
không phải của Apache. Script in SHA-256 để bạn ghi vào báo cáo/phụ lục. Nếu không muốn dùng binary
bên thứ ba, hãy chạy dự án trong WSL2 (Ubuntu) hoặc Docker (xem README).
"""
import argparse
import hashlib
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEST = ROOT / "tools" / "hadoop" / "bin"
FILES = ("winutils.exe", "hadoop.dll")
VERSION_DIR = "hadoop-3.3.6"
BRANCHES = ("master", "main")
DEFAULT_BASE = "https://raw.githubusercontent.com/cdarlint/winutils"


def fetch(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=60) as r:
        return r.read()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base-url", default=DEFAULT_BASE, help="đổi nguồn (dùng để test)")
    ap.add_argument("--force", action="store_true", help="tải lại dù đã có")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    DEST.mkdir(parents=True, exist_ok=True)
    for name in FILES:
        target = DEST / name
        if target.exists() and not args.force:
            print(f"{name}: đã có ({target.stat().st_size} byte) – bỏ qua")
            continue
        data, used = None, None
        for br in BRANCHES:
            url = f"{args.base_url}/{br}/{VERSION_DIR}/bin/{name}"
            if args.dry_run:
                print(url)
                used = url
                break
            try:
                data, used = fetch(url), url
                break
            except urllib.error.HTTPError as e:
                print(f"  {url} -> HTTP {e.code}")
            except Exception as e:
                print(f"  {url} -> {e}")
        if args.dry_run:
            continue
        if data is None:
            print(f"KHÔNG tải được {name}. Tải tay từ trang cdarlint/winutils (thư mục {VERSION_DIR}/bin) vào {DEST}")
            return 1
        target.write_bytes(data)
        print(f"{name}: {len(data)} byte, sha256={hashlib.sha256(data).hexdigest()}\n  nguồn: {used}")
    print(f"Xong. HADOOP_HOME sẽ tự trỏ tới: {DEST.parent}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

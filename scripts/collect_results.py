#!/usr/bin/env python3
"""Gom mọi file kết quả cần gửi để soát vào MỘT thư mục (và một file zip) cho dễ gửi.

Mặc định lấy từ results/:  mọi .csv .json .txt .log .md (nhỏ), bỏ ảnh .png và file lớn; riêng thư mục spark_ui/:
chỉ lấy *_summary.csv và capture_info.json (đủ số liệu; JSON gốc nặng, ảnh thì giữ lại để chèn báo cáo).
Thêm vào thư mục gửi: NOTES.md (nhật ký làm việc).

Chạy:   python scripts/collect_results.py                  -> handoff/  và  handoff_YYYYmmdd_HHMM.zip
Tuỳ chọn:
  --steps 03_aggregate 04_benchmark   chỉ lấy các thư mục kết quả này (mặc định: tất cả trong results/)
  --png-main                          kèm ảnh tab tổng (jobs/stages/sql/executors/environment/storage .png) của spark_ui
  --png-all                           kèm MỌI ảnh (có thể rất nặng)
  --json                              kèm JSON gốc của spark_ui
  --max-mb 5                          bỏ file lớn hơn số MB này (ghi vào danh sách bị bỏ)
Chỉ ĐỌC và SAO CHÉP; không sửa/xoá file nào trong results/.
"""
import argparse
import datetime as dt
import shutil
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TEXT_EXT = {".csv", ".json", ".txt", ".log", ".md"}
MAIN_TABS = {"jobs.png", "stages.png", "sql.png", "executors.png", "environment.png", "storage.png"}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", default=str(ROOT / "results"))
    ap.add_argument("--out", default=str(ROOT / "handoff"))
    ap.add_argument("--steps", nargs="*")
    ap.add_argument("--png-main", action="store_true")
    ap.add_argument("--png-all", action="store_true")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--max-mb", type=float, default=5.0)
    ap.add_argument("--no-zip", action="store_true")
    a = ap.parse_args()

    res, out = Path(a.results), Path(a.out)
    if not res.exists():
        print(f"Không thấy thư mục {res}. Chạy các bước trước đã.")
        return 1
    if out.exists():
        shutil.rmtree(out)  # chỉ xoá thư mục handoff cũ do chính script này tạo
    out.mkdir(parents=True)

    steps = sorted(d for d in res.iterdir() if d.is_dir() and (not a.steps or d.name in a.steps))
    copied, skipped = [], []
    for step in steps:
        for f in sorted(step.rglob("*")):
            if not f.is_file():
                continue
            rel = f.relative_to(res)
            in_ui = "spark_ui" in rel.parts
            ext = f.suffix.lower()
            keep = False
            if in_ui:
                if f.name.endswith("_summary.csv") or f.name == "capture_info.json":
                    keep = True
                elif ext == ".json":
                    keep = a.json
                elif ext == ".png":
                    keep = a.png_all or (a.png_main and f.name in MAIN_TABS)
            elif ext in TEXT_EXT:
                keep = True
            elif ext == ".png":
                keep = a.png_all
            if not keep:
                continue
            mb = f.stat().st_size / 1e6
            if mb > a.max_mb:
                skipped.append((str(rel), f"{mb:.1f} MB > --max-mb"))
                continue
            dest = out / "results" / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(f, dest)
            copied.append((str(rel), f.stat().st_size))
    notes = ROOT / "NOTES.md"
    if notes.exists() and not a.steps:
        shutil.copy2(notes, out / "NOTES.md")
        copied.append(("NOTES.md", notes.stat().st_size))

    total = sum(s for _, s in copied)
    lines = [f"Gom lúc {dt.datetime.now().isoformat(timespec='seconds')} từ {res}", f"{len(copied)} file, {total / 1e6:.2f} MB", ""]
    lines += [f"{n:<70}{s / 1e3:>10.1f} KB" for n, s in copied]
    if skipped:
        lines += ["", "BỊ BỎ (quá lớn):"] + [f"  {n}  ({why})" for n, why in skipped]
    (out / "MANIFEST.txt").write_text("\n".join(lines), encoding="utf-8")

    print(f"Thư mục gửi: {out}  ({len(copied)} file, {total / 1e6:.2f} MB)")
    by_step = {}
    for n, _ in copied:
        by_step[n.split("/")[0].split("\\")[0]] = by_step.get(n.split("/")[0].split("\\")[0], 0) + 1
    for k, v in by_step.items():
        print(f"  {k:<28}{v:>4} file")
    for n, why in skipped:
        print(f"  BỊ BỎ: {n} ({why})")
    if not a.no_zip:
        zp = out.parent / f"{out.name}_{dt.datetime.now():%Y%m%d_%H%M}.zip"
        with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as z:
            for f in out.rglob("*"):
                if f.is_file():
                    z.write(f, f.relative_to(out))
        print(f"Zip để gửi: {zp}  ({zp.stat().st_size / 1e6:.2f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

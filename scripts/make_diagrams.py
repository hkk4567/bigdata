#!/usr/bin/env python3
"""Vẽ sơ đồ cho báo cáo (mục 4 Thiết kế) bằng matplotlib -> docs/diagrams/*.png và *.svg.
  diagram01_data_flow       luồng dữ liệu: nguồn -> raw -> lake -> clean -> processed, kèm công việc (script) và nhánh đo đạc/báo cáo
  diagram02_clean_policy    chính sách làm sạch: LOẠI / SỬA / GIỮ (kèm số dòng thật nếu có results/02_clean_v2)
Các con số lấy từ results/ nếu có (không có thì bỏ số, vẫn vẽ được). Màu theo bảng màu của biểu đồ (05_make_figures.py).
Chạy:  python scripts/make_diagrams.py            (tuỳ chọn: --results-root results --out docs/diagrams)
"""
import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SURF, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
BLUE_T, ORANGE_T, AQUA_T, GRAY_T = "#e3effc", "#fdebe4", "#e0f5ec", "#efeee9"


def vi(n):
    return f"{int(n):,}".replace(",", ".")


def read_csv(p):
    try:
        with open(p, encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return []


def facts(rr):
    out = {}
    cs = read_csv(rr / "02_clean_v2" / "clean_summary_by_month.csv")
    if cs:
        out["raw"] = sum(int(r["rows_in"]) for r in cs)
        out["clean"] = sum(int(r["rows_clean"]) for r in cs)
        out["rej"] = sum(int(r["rows_rejected"]) for r in cs)
        out["dup"] = sum(int(r["duplicates_removed"]) for r in cs)
    hv = read_csv(rr / "06_profile_hvfhv" / "hv_profile_by_month.csv")
    if hv:
        out["hv"] = sum(int(r["rows"]) for r in hv)
    rc = {r["rule"]: int(r["rows"]) for r in read_csv(rr / "02_clean_v2" / "rule_counts.csv")}
    out["rules"] = rc
    return out


def setup(w, h):
    fig = plt.figure(figsize=(w, h), dpi=100, facecolor=SURF)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100 * h / w)
    ax.axis("off")
    return fig, ax


def box(ax, x, y, w, h, title, lines=(), fill=BLUE_T, edge=BLUE, tsize=10, lsize=8.6, bold=True, lw=1.3, mono=False):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0,rounding_size=0.9", fc=fill, ec=edge, lw=lw, zorder=2))
    ty = y + h - 1.6
    ax.text(x + w / 2, ty, title, ha="center", va="top", fontsize=tsize, fontweight="bold" if bold else "normal", color=INK,
            family="monospace" if mono else None, zorder=3)
    yy = ty - 2.6
    for ln in lines:
        ax.text(x + w / 2, yy, ln, ha="center", va="top", fontsize=lsize, color=INK2, zorder=3)
        yy -= 1.9


def arrow(ax, p1, p2, color=INK2, ls="-", rad=0.0, lw=1.4):
    ax.add_patch(FancyArrowPatch(p1, p2, arrowstyle="-|>", mutation_scale=12, color=color, lw=lw, ls=ls,
                                 connectionstyle=f"arc3,rad={rad}", zorder=1, shrinkA=0, shrinkB=0))


def pill(ax, cx, y, w, script, desc):
    ax.add_patch(FancyBboxPatch((cx - w / 2, y), w, 5.4, boxstyle="round,pad=0,rounding_size=1.2", fc=SURF, ec=INK2, lw=1.1, zorder=3))
    ax.text(cx, y + 3.9, script, ha="center", va="center", fontsize=8.8, fontweight="bold", color=INK, family="monospace", zorder=4)
    ax.text(cx, y + 1.5, desc, ha="center", va="center", fontsize=8, color=INK2, zorder=4)


def diagram_flow(rr, out):
    f = facts(rr)
    W, H = 15.0, 8.6
    fig, ax = setup(W, H)
    top = 100 * H / W
    ax.text(1.5, top - 2.2, "Luồng dữ liệu của hệ thống (Storage → Processing → Serving)", fontsize=14.5, fontweight="bold", color=INK, va="top")
    ax.text(1.5, top - 5.6, "Ô xanh = nơi lưu dữ liệu; ô viền tròn = công việc (script); mũi tên liền = luồng dữ liệu chính; nét đứt = đọc để đo/vẽ.",
            fontsize=9.3, color=INK2, va="top")

    bw, bh, by = 15.2, 19.5, 21.5
    xs = [1.5, 21.5, 41.5, 61.5, 82.0]
    # 1. nguồn
    src_lines = ["NYC TLC Trip Records", "(CloudFront, Parquet)", "", "Yellow Taxi 2024-01…12"]
    src_lines.append(f"{vi(f['raw'])} dòng" if "raw" in f else "")
    src_lines += ["HVFHV 2024-01…02", f"{vi(f['hv'])} dòng" if "hv" in f else "", "taxi_zone_lookup.csv"]
    box(ax, xs[0], by, bw, bh, "Nguồn công khai", src_lines, fill=GRAY_T, edge=AXIS)
    box(ax, xs[1], by, bw, bh, "data/raw", ["Parquet gốc theo tháng,", "giữ nguyên, không sửa", "", "yellow_tripdata_YYYY-MM", "fhvhv_tripdata_YYYY-MM"], mono=True, tsize=10.5)
    box(ax, xs[2], by, bw, bh, "data/lake", ["Parquet phân vùng", "year=…/month=…", "tên cột chữ thường,", "schema thống nhất,", "+ source_file", "dim_zone (265 khu)"], mono=True, tsize=10.5)
    clean_l = ["yellow (đạt)", f"{vi(f['clean'])} dòng" if "clean" in f else "", "+ cột dẫn xuất, cờ", "", "yellow_rejected", f"{vi(f['rej'])} dòng" if "rej" in f else "", "+ reject_reasons"]
    box(ax, xs[3], by, bw, bh, "data/clean_v2", clean_l, mono=True, tsize=10.5)
    box(ax, xs[4], by, bw, bh, "data/processed", ["6 bảng KPI (Parquet):", "kpi_daily", "kpi_hourly_dow", "kpi_zone_pickup", "kpi_od_top", "kpi_payment_month", "kpi_vendor_dq"], mono=True, tsize=10.5)

    # công việc giữa các ô
    jobs = [("download_data.py", "tải, thử lại, kiểm size"), ("01_ingest_to_lake.py", "chuẩn hoá schema, từng tháng"),
            ("02_clean_v2.py", "loại / sửa / giữ, từng tháng"), ("03_aggregate.py", "Spark SQL (sql/*.sql)")]
    for i, (s, d) in enumerate(jobs):
        gx0, gx1 = xs[i] + bw, xs[i + 1]
        cx = (gx0 + gx1) / 2
        arrow(ax, (gx0 + 0.3, by + bh / 2), (gx1 - 0.3, by + bh / 2), lw=1.8)
        pill(ax, cx, by + bh + 2.4, 17.8, s, d)
        ax.plot([cx, cx], [by + bh + 2.4, by + bh / 2 + 0.2], color=INK2, lw=1.0, ls=(0, (2, 2)), zorder=1)

    # hồ sơ/kiểm chứng bên dưới các ô
    ax.text(xs[2] + bw / 2, by - 1.6, "results/01_ingest_to_lake/ingest_log.csv\n(rows_in = rows_out mỗi tháng)", ha="center", va="top", fontsize=7.8, color=MUTED)
    ax.text(xs[3] + bw / 2, by - 1.6, "results/02_clean_v2/ (rule_counts, summary)\n"
            + (f"{vi(f['raw'])} = {vi(f['dup'])} trùng + {vi(f['rej'])} loại + {vi(f['clean'])} đạt" if "raw" in f else "rows_in = trùng + loại + đạt"),
            ha="center", va="top", fontsize=7.8, color=MUTED)
    ax.text(xs[4] + bw / 2, by - 1.6, "results/03_aggregate/*.csv\n(kiểm tổng chuyến = số dòng đạt)", ha="center", va="top", fontsize=7.8, color=MUTED)

    # làn đo đạc / báo cáo
    ly, lh = 2.6, 13.2
    ax.add_patch(FancyBboxPatch((1.5, ly), 97.0, lh, boxstyle="round,pad=0,rounding_size=1.0", fc="none", ec=AXIS, lw=1.0, ls=(0, (4, 3)), zorder=1))
    ax.text(2.5, ly + lh - 0.9, "Đo đạc, chẩn đoán và báo cáo (đọc từ lake / clean_v2, không ghi ngược lại)", fontsize=8.8, color=INK2, va="top", fontweight="bold")
    items = [("04_benchmark.py", "thời gian, thông lượng,", "Spark UI tự động (ui_capture.py)"),
             ("04b_pandas_memory.py", "RAM đỉnh Pandas so với Spark,", "mỗi lượt một tiến trình"),
             ("06 / 06b_*_hvfhv.py", "hồ sơ + chẩn đoán HVFHV", "(đọc data/lake/fhvhv)"),
             ("05_make_figures.py", "15 biểu đồ PNG từ results/", "")]
    iw = 22.6
    for i, (s, l1, l2) in enumerate(items):
        x = 2.5 + i * (iw + 1.8)
        ax.add_patch(FancyBboxPatch((x, ly + 1.0), iw, 8.2, boxstyle="round,pad=0,rounding_size=1.4", fc=AQUA_T, ec=AQUA, lw=1.2, zorder=2))
        ax.text(x + iw / 2, ly + 8.1, s, ha="center", va="top", fontsize=8.8, fontweight="bold", family="monospace", color=INK, zorder=3)
        ax.text(x + iw / 2, ly + 5.5, l1, ha="center", va="top", fontsize=7.9, color=INK2, zorder=3)
        ax.text(x + iw / 2, ly + 3.4, l2, ha="center", va="top", fontsize=7.9, color=INK2, zorder=3)
    fig.text(0.5, 0.005, "Spark 3.5.3 local[N] (Python 3.11, Java 17) · Pandas/PyArrow dùng để so sánh · chạy trực tiếp hoặc trong Docker (docker compose) · mỗi script ghi kết quả vào results/<tên-script>/",
             ha="center", va="bottom", fontsize=8, color=MUTED)
    save(fig, out, "diagram01_data_flow")


def diagram_policy(rr, out):
    f = facts(rr)
    rc = f.get("rules", {})
    g = lambda k: (f"{vi(rc[k])} dòng" if k in rc else "")  # noqa: E731
    W, H = 15.0, 8.8
    fig, ax = setup(W, H)
    top = 100 * H / W
    ax.text(1.5, top - 2.2, "Chính sách làm sạch (02_clean_v2.py): loại, sửa hay giữ", fontsize=14.5, fontweight="bold", color=INK, va="top")
    ax.text(1.5, top - 5.6, "Nguyên tắc: chỉ LOẠI khi không còn trường nào đáng tin; nếu chỉ một đại lượng sai thì SỬA (đặt null + cờ) để vẫn tính được các KPI còn lại.",
            fontsize=9.3, color=INK2, va="top")
    # đầu vào: hai ô nằm ngang, rồi "thanh" chia ba nhánh
    ytop = top - 16.0
    box(ax, 17.0, ytop, 30.0, 6.8, "Dòng trong data/lake (từng tháng)", [f"{vi(f['raw'])} dòng cả năm" if "raw" in f else ""], fill=GRAY_T, edge=AXIS, tsize=9.6)
    box(ax, 53.0, ytop, 30.0, 6.8, "Bỏ dòng trùng hệt trong tháng", [g("DUPLICATES_REMOVED")], fill=GRAY_T, edge=AXIS, tsize=9.6)
    arrow(ax, (47.3, ytop + 3.4), (52.7, ytop + 3.4))
    bus = ytop - 3.2
    ax.plot([68, 68], [ytop, bus], color=INK2, lw=1.4, zorder=1)
    ax.plot([17, 83], [bus, bus], color=INK2, lw=1.4, zorder=1)
    # ba nhánh
    cw, cy_top = 31.0, top - 25.5
    cols = [(1.5, ORANGE_T, ORANGE, "LOẠI (reject)", "→ data/clean_v2/yellow_rejected + reject_reasons",
             [("thời lượng ≤ 0 hoặc > 6 giờ", f"{vi(rc.get('DUR_NONPOSITIVE', 0) + rc.get('DUR_GT_MAX', 0))}" if rc else ""),
              ("tốc độ TB > 80 mph; quãng đường > 100 dặm", f"{vi(rc.get('SPEED_GT_MAX', 0) + rc.get('DIST_GT_MAX', 0))}" if rc else ""),
              ("fare ≤ 0 hoặc fare < 3 USD (khi dist > 0)", f"{vi(rc.get('FARE_NONPOSITIVE', 0) + rc.get('FARE_BELOW_MIN', 0))}" if rc else ""),
              ("total ≤ 0 hoặc có khoản phụ phí/tip/thuế < 0", f"{vi(rc.get('TOTAL_NONPOSITIVE', 0) + rc.get('NEG_COMPONENT', 0))}" if rc else ""),
              ("ngày đón ngoài tháng của file", g("PICKUP_OUT_OF_MONTH").replace(" dòng", "")),
              ("fare > 500 hoặc total > 1000 USD", f"{vi(rc.get('FARE_GT_MAX', 0) + rc.get('TOTAL_GT_MAX', 0))}" if rc else "")]),
            (34.5, BLUE_T, BLUE, "SỬA (repair)", "→ ở lại data/clean_v2/yellow, kèm cờ",
             [("passenger_count = 0 hoặc > 6 → null", g("PAX_REPAIRED_TO_NULL").replace(" dòng", "")),
              ("cờ passenger_count_repaired", ""),
              ("trip_distance = 0 → null", g("ZERO_DISTANCE_FLAGGED").replace(" dòng", "")),
              ("cờ is_zero_distance; avg_speed_mph = null", ""),
              ("Lý do: 90% dòng này chỉ sai quãng đường,", ""),
              ("giá bình thường; loại sẽ mất ~2,1% doanh thu", "")]),
            (67.5, AQUA_T, AQUA, "GIỮ NGUYÊN (keep)", "→ ở lại data/clean_v2/yellow",
             [("Flex Fare (payment_type = 0): vài cột null", ""),
              ("(passenger_count, ratecode, congestion…)", ""),
              ("Khu vực 264/265 (Unknown) là mã hợp lệ", ""),
              ("Doanh thu = tổng total_amount như đã báo cáo", ""),
              ("(lệch ~1,07% so với tổng các phần,", ""),
              ("ghi rõ trong báo cáo)", "")])]
    for x, fill, edge, ttl, sub, rows in cols:
        h = 29.0
        ax.add_patch(FancyBboxPatch((x, cy_top - h), cw, h, boxstyle="round,pad=0,rounding_size=0.9", fc=fill, ec=edge, lw=1.3, zorder=2))
        ax.text(x + cw / 2, cy_top - 1.4, ttl, ha="center", va="top", fontsize=11, fontweight="bold", color=INK, zorder=3)
        ax.text(x + cw / 2, cy_top - 4.3, sub, ha="center", va="top", fontsize=7.8, color=INK2, zorder=3)
        yy = cy_top - 8.0
        for t, n in rows:
            ax.text(x + 1.3, yy, t, ha="left", va="top", fontsize=8.1, color=INK, zorder=3)
            if n:
                ax.text(x + cw - 1.2, yy, n, ha="right", va="top", fontsize=8.1, color=INK2, zorder=3, fontweight="bold")
            yy -= 3.6
        arrow(ax, (x + cw / 2, bus), (x + cw / 2, cy_top), rad=0.0)
    if f:
        tot = f["raw"]
        fig.text(0.5, 0.02, f"Kiểm bảo toàn: {vi(tot)} dòng vào = {vi(f['dup'])} trùng + {vi(f['rej'])} loại ({f['rej'] / tot * 100:.2f}%) + {vi(f['clean'])} đạt. "
                 "Một dòng có thể vi phạm nhiều luật nên các số trong ô LOẠI cộng lại lớn hơn số dòng bị loại.", ha="center", va="bottom", fontsize=8.3, color=MUTED)
    save(fig, out, "diagram02_clean_policy")


def save(fig, out, name):
    out.mkdir(parents=True, exist_ok=True)
    fig.savefig(out / f"{name}.png", dpi=200, facecolor=SURF)
    fig.savefig(out / f"{name}.svg", facecolor=SURF)
    plt.close(fig)
    print(f"[ok] {name}.png / .svg")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results-root", default=str(ROOT / "results"))
    ap.add_argument("--out", default=str(ROOT / "docs" / "diagrams"))
    a = ap.parse_args()
    rr, out = Path(a.results_root), Path(a.out)
    diagram_flow(rr, out)
    diagram_policy(rr, out)


if __name__ == "__main__":
    main()

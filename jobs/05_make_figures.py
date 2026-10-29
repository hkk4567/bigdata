#!/usr/bin/env python3
"""Vẽ biểu đồ cho báo cáo (mục 6 Thực nghiệm, 7 Demo/KPI, 5 Cài đặt) từ các file kết quả trong results/.

Đầu vào (chỉ đọc):  results/04_benchmark/{bench_summary.csv, spark_ui/*/stages_summary.csv}, results/03_aggregate/kpi_*.csv,
                    results/02_clean/rule_counts.csv, results/02_clean_v2/rule_counts.csv
Đầu ra: results/05_make_figures/fig01..fig11 *.png (200 dpi, nền sáng, dùng cho Word/PDF) + figures_index.csv
Mọi số trên biểu đồ và trong tiêu đề/phụ đề đều TÍNH từ file kết quả, không gõ tay. Biểu đồ nào thiếu dữ liệu thì bỏ qua và báo.

Quy ước hình (theo skill dataviz): màu phân loại theo thứ tự cố định xanh dương -> cam -> xanh ngọc (đã kiểm bằng validate_palette:
đạt cả 3 cổng, riêng tương phản với nền < 3:1 ở xanh ngọc nên luôn có nhãn trực tiếp + bảng số trong CSV); nhấn mạnh = 1 màu + xám;
một trục, không trục kép; thanh <= 24 px, đầu thanh bo 4 px; đường 2 px; chữ luôn màu mực (không tô màu chữ theo chuỗi).
Hình tĩnh nên không có tooltip; số liệu đầy đủ nằm trong các CSV nguồn.

Chạy:  python jobs/05_make_figures.py
       python jobs/05_make_figures.py --results-root results --only fig01 fig05
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, results_dir  # noqa: E402

SURF, INK, INK2, MUTED = "#fcfcfb", "#0b0b0b", "#52514e", "#898781"
GRID, AXIS, DEEMPH = "#e1e0d9", "#c3c2b7", "#c3c2b7"
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
SEQ = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
DOW_VI = {2: "T2", 3: "T3", 4: "T4", 5: "T5", 6: "T6", 7: "T7", 1: "CN"}
DOW_ORDER = [2, 3, 4, 5, 6, 7, 1]
SRC_BENCH = "results/04_benchmark/bench_summary.csv"


def vi(x, d=0):
    """Định dạng số kiểu Việt Nam: 1.234.567,8"""
    s = f"{x:,.{d}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def compact(x, _=None):
    if abs(x) >= 1e6:
        return f"{x / 1e6:g}M"
    if abs(x) >= 1e3:
        return f"{x / 1e3:g}K"
    return f"{x:g}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results-root", default=str(ROOT / "results"))
    ap.add_argument("--out", default=None, help="mặc định: results/05_make_figures")
    ap.add_argument("--only", nargs="*", help="chỉ vẽ các hình này, vd fig01 fig05")
    ap.add_argument("--dpi", type=int, default=200)
    a = ap.parse_args()

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    from matplotlib.colors import LinearSegmentedColormap, to_rgb
    from matplotlib.patches import PathPatch, Rectangle
    from matplotlib.path import Path as MPath
    from matplotlib.ticker import FuncFormatter

    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.sans-serif": ["Segoe UI", "Helvetica Neue", "Arial", "DejaVu Sans"],
        "font.size": 10, "axes.edgecolor": AXIS, "text.color": INK, "axes.labelcolor": INK2,
        "xtick.color": INK2, "ytick.color": INK2, "figure.facecolor": SURF, "axes.facecolor": SURF,
        "savefig.facecolor": SURF, "axes.unicode_minus": False,
    })
    rr, out = Path(a.results_root), results_dir(__file__, a.out)
    index = []

    # ---------- tiện ích vẽ ----------
    def new_fig(w=8.0, h=4.6, ncols=1, left=0.09, right=0.97, top=0.80, bottom=0.14, wspace=0.28, widths=None):
        fig = plt.figure(figsize=(w, h), dpi=100)
        axs = fig.subplots(1, ncols, squeeze=False, gridspec_kw=dict(
            left=left, right=right, top=top, bottom=bottom, wspace=wspace, **({"width_ratios": widths} if widths else {})))[0]
        return fig, list(axs)

    def header(fig, title, sub):
        fig.text(0.02, 0.955, title, fontsize=13.5, fontweight="bold", color=INK, ha="left", va="top")
        import textwrap
        sub = textwrap.fill(sub, width=int(fig.get_size_inches()[0] * 11.5))
        fig.text(0.02, 0.905, sub, fontsize=9.5, color=INK2, ha="left", va="top", linespacing=1.3)

    def footer(fig, src):
        import textwrap
        w = int(fig.get_size_inches()[0] * 15.5)
        fig.text(0.02, 0.012, textwrap.fill(src, width=w), fontsize=7.5, color=MUTED, ha="left", va="bottom", linespacing=1.3)

    def style(ax, grid="y", ticks_y=True):
        for s in ("top", "right", "left"):
            ax.spines[s].set_visible(False)
        ax.spines["bottom"].set_color(AXIS)
        ax.tick_params(axis="both", length=0, labelsize=9)
        if grid == "y":
            ax.yaxis.grid(True, color=GRID, lw=0.72, ls="-")
        elif grid == "x":
            ax.xaxis.grid(True, color=GRID, lw=0.72, ls="-")
        ax.set_axisbelow(True)
        if not ticks_y:
            ax.tick_params(axis="y", labelleft=False)

    def px(ax):
        """1 px (ở dpi 100) tính theo đơn vị dữ liệu của trục hiện tại (gọi SAU khi đặt xlim/ylim)."""
        bb = ax.get_position()
        fw, fh = ax.figure.get_size_inches() * 100
        (x0, x1), (y0, y1) = ax.get_xlim(), ax.get_ylim()
        return abs(x1 - x0) / (bb.width * fw), abs(y1 - y0) / (bb.height * fh)

    def vbar(ax, x, h, w, color, r=4):
        dx, dy = px(ax)
        rx, ry = min(r * dx, w / 2), min(r * dy, abs(h))
        x0, x1, y1 = x - w / 2, x + w / 2, h
        v = [(x0, 0), (x0, y1 - ry), (x0, y1), (x0 + rx, y1), (x1 - rx, y1), (x1, y1), (x1, y1 - ry), (x1, 0), (x0, 0)]
        c = [1, 1, 3, 1, 1, 3, 1, 1, 79]
        c = [MPath.MOVETO, MPath.LINETO, MPath.CURVE3, MPath.CURVE3, MPath.LINETO, MPath.CURVE3, MPath.CURVE3, MPath.LINETO, MPath.CLOSEPOLY]
        ax.add_patch(PathPatch(MPath(v, c), facecolor=color, edgecolor="none", zorder=3))

    def hbar(ax, y, wd, h, color, r=4):
        dx, dy = px(ax)
        rx, ry = min(r * dx, abs(wd)), min(r * dy, h / 2)
        y0, y1, x1 = y - h / 2, y + h / 2, wd
        v = [(0, y0), (x1 - rx, y0), (x1, y0), (x1, y0 + ry), (x1, y1 - ry), (x1, y1), (x1 - rx, y1), (0, y1), (0, y0)]
        c = [MPath.MOVETO, MPath.LINETO, MPath.CURVE3, MPath.CURVE3, MPath.LINETO, MPath.CURVE3, MPath.CURVE3, MPath.LINETO, MPath.CLOSEPOLY]
        ax.add_patch(PathPatch(MPath(v, c), facecolor=color, edgecolor="none", zorder=3))

    def whisker(ax, x, lo, hi, cap=0.06):
        ax.plot([x, x], [lo, hi], color=INK2, lw=1, zorder=4, solid_capstyle="butt")
        for yy in (lo, hi):
            ax.plot([x - cap, x + cap], [yy, yy], color=INK2, lw=1, zorder=4)

    def dot(ax, x, y, color):
        ax.plot([x], [y], marker="o", ms=6, mfc=color, mec=SURF, mew=1.5, ls="none", zorder=5, clip_on=False)

    def ink_on(fill):
        def lum(c):
            r, g, b = [(v / 12.92 if v <= 0.03928 else ((v + 0.055) / 1.055) ** 2.4) for v in to_rgb(c)]
            return 0.2126 * r + 0.7152 * g + 0.0722 * b
        lf, lw_, lk = lum(fill), lum("#ffffff"), lum(INK)
        return "#ffffff" if (lw_ + 0.05) / (lf + 0.05) >= (lf + 0.05) / (lk + 0.05) else INK

    def read(rel):
        p = rr / rel
        if not p.exists():
            raise FileNotFoundError(rel)
        return pd.read_csv(p)

    def save(fig, name, title, sources):
        path = out / f"{name}.png"
        fig.savefig(path, dpi=a.dpi)
        plt.close(fig)
        index.append({"figure": path.name, "title": title, "sources": "; ".join(sources)})
        print(f"[ok] {path.name}")

    def bench():
        b = read("04_benchmark/bench_summary.csv").set_index(["experiment", "config"])
        return b

    def stage_bytes(folder, records):
        """Các giá trị inputBytes (byte đọc vào) của stage có đúng `records` dòng đầu vào trong Spark UI."""
        s = read(f"04_benchmark/spark_ui/{folder}/stages_summary.csv")
        v = s[(s["inputRecords"] == records) & (s["inputBytes"] > 0) & s["name"].str.startswith("save")]["inputBytes"]
        return sorted(set(v.astype(int)))

    figs = {}

    # ---------- fig01: quy mô dữ liệu, Spark so với Pandas ----------
    def fig01():
        b = bench()
        sc = b.loc["scale"].reset_index()
        sp = sc[sc.config.str.startswith("spark_")].assign(m=lambda d: d.config.str.extract(r"_(\d+)m")[0].astype(int)).sort_values("m")
        pdn = sc[sc.config.str.startswith("pandas_")].assign(m=lambda d: d.config.str.extract(r"_(\d+)m")[0].astype(int)).sort_values("m")
        fig, (a1, a2) = new_fig(8.6, 4.4, 2, left=0.08, right=0.97, wspace=0.26)
        for ax, col, ylab, fmt in ((a1, "median_s", "Thời gian xử lý (giây)", None), (a2, "rows_per_s_median", "Thông lượng (triệu dòng/giây)", None)):
            style(ax)
            for dfx, color, name in ((sp, BLUE, "Spark local[2]"), (pdn, ORANGE, "Pandas")):
                y = dfx[col] / (1e6 if col != "median_s" else 1)
                ax.plot(dfx.m, y, color=color, lw=1.5, solid_capstyle="round", solid_joinstyle="round", label=name, zorder=3)
                for xx, yy in zip(dfx.m, y):
                    dot(ax, xx, yy, color)
            ax.set_xticks(sp.m)
            ax.set_xlabel("Số tháng dữ liệu (Yellow Taxi 2024)", fontsize=9)
            ax.set_ylabel(ylab, fontsize=9)
            ax.set_ylim(0, None)
            ax.set_xlim(0, 12.8)
        # nhãn trực tiếp ở điểm cuối
        a1.text(12.15, sp.median_s.iloc[-1], f"{vi(sp.median_s.iloc[-1], 2)} s", fontsize=9, va="center", color=INK2)
        a1.text(6.15, pdn.median_s.iloc[-1] + 0.12, f"{vi(pdn.median_s.iloc[-1], 2)} s", fontsize=9, va="bottom", color=INK2)
        a2.text(12.15, sp.rows_per_s_median.iloc[-1] / 1e6, f"{vi(sp.rows_per_s_median.iloc[-1] / 1e6, 1)}", fontsize=9, va="center", color=INK2)
        a2.text(6.15, pdn.rows_per_s_median.iloc[-1] / 1e6 - 0.35, f"{vi(pdn.rows_per_s_median.iloc[-1] / 1e6, 1)}", fontsize=9, va="top", color=INK2)
        a1.legend(loc="upper left", frameon=False, fontsize=9, handlelength=1.6)
        r12 = int(sp.rows.iloc[-1])
        header(fig, "Khi dữ liệu còn vừa RAM, Spark và Pandas có tốc độ cùng cỡ",
               f"Cùng phép gom (ngày × khu vực đón). Spark 12 tháng: {vi(r12)} dòng trong {vi(sp.median_s.iloc[-1], 2)} giây; Pandas chỉ đo đến 6 tháng.")
        footer(fig, f"Nguồn: {SRC_BENCH}. Median của 3 lượt đo (sau 1 lượt khởi động, Spark đã nóng); đọc 4 cột. Pandas 12 tháng và RAM: xem fig12.")
        return fig, "Quy mô dữ liệu: Spark so với Pandas", [SRC_BENCH]

    # ---------- fig02: số partition shuffle ----------
    def fig02():
        b = bench().loc["shuffle"].reset_index()
        b["order"] = b.config.map(lambda c: 9999 if "AQE on" in c else int(c.split("=")[1].split()[0]))
        b = b.sort_values("order")
        fig, (ax,) = new_fig(8.2, 4.4, 1, left=0.09, right=0.97, bottom=0.2)
        style(ax)
        xs = [0, 1, 2, 3, 4, 5.4]
        best = b.median_s.min()
        labels = []
        ax.set_xlim(-0.6, 6.0)
        ax.set_ylim(0, b.max_s.max() * 1.18)
        for x, (_, r) in zip(xs, b.iterrows()):
            is_best = r.median_s <= best * 1.03
            vbar(ax, x, r.median_s, 0.5, BLUE if is_best else DEEMPH)
            whisker(ax, x, r.min_s, r.max_s)
            ax.text(x, r.max_s + b.max_s.max() * 0.025, f"{vi(r.median_s, 2)} s", ha="center", va="bottom", fontsize=9, color=INK)
            labels.append(f"{int(r.order)}" if r.order != 9999 else "200\n(AQE bật)")
        ax.set_xticks(xs)
        ax.set_xticklabels(labels, fontsize=9)
        ax.set_xlabel("spark.sql.shuffle.partitions (AQE tắt, trừ cột cuối)", fontsize=9)
        ax.set_ylabel("Thời gian xử lý (giây)", fontsize=9)
        p200 = float(b[b.order == 200].median_s.iloc[0])
        p16 = float(b[b.order == 16].median_s.iloc[0])
        header(fig, "Quá nhiều partition làm chậm tác vụ gom",
               f"12 tháng, 2 nhân. 200 partition chậm hơn 16 partition {vi((p200 / p16 - 1) * 100)}%; bật AQE giảm nhưng chưa bằng chỉnh đúng.")
        footer(fig, f"Nguồn: {SRC_BENCH}. Cột xanh = trong 3% của cấu hình nhanh nhất; thanh dọc = min–max của 3 lượt đo.")
        return fig, "Số partition shuffle", [SRC_BENCH]

    # ---------- fig03: Parquet so với CSV ----------
    def fig03():
        b = bench().loc["format"].reset_index().set_index("config")
        pq_key = [k for k in b.index if k.startswith("parquet_")][0]
        csv_key = [k for k in b.index if k.startswith("csv_")][0]
        runs = read("04_benchmark/bench_runs.csv")
        d = runs[(runs.experiment == "format") & (runs.config == "disk_bytes")]["note"].iloc[0]
        kv = dict(t.split("=") for t in d.split() if "=" in t)
        pq_disk, csv_disk = float(kv["parquet"]), float(kv["csv"])
        rows = int(b.loc[pq_key, "rows"])
        panels = [("Thời gian truy vấn (giây)", [b.loc[pq_key, "median_s"], b.loc[csv_key, "median_s"]],
                   lambda v: f"{vi(v, 1)} s", [(b.loc[pq_key, "min_s"], b.loc[pq_key, "max_s"]), (b.loc[csv_key, "min_s"], b.loc[csv_key, "max_s"])]),
                  ("Dung lượng trên đĩa (MB)", [pq_disk / 1e6, csv_disk / 1e6], lambda v: f"{vi(v)} MB", None)]
        try:
            by = stage_bytes("format", rows)
            if len(by) >= 2:
                panels.append(("Byte Spark đọc vào (MB)", [min(by) / 1e6, max(by) / 1e6], lambda v: f"{vi(v)} MB", None))
        except FileNotFoundError:
            pass
        n = len(panels)
        fig, axs = new_fig(2.9 * n + 0.6, 4.4, n, left=0.07, right=0.98, wspace=0.42, bottom=0.17)
        for ax, (ttl, vals, f, rng) in zip(axs, panels):
            style(ax)
            ax.set_xlim(-0.7, 1.7)
            ax.set_ylim(0, max(vals) * 1.22)
            for i, v in enumerate(vals):
                vbar(ax, i, v, 0.55, BLUE if i == 0 else DEEMPH)
                if rng:
                    whisker(ax, i, *rng[i], cap=0.05)
                top = rng[i][1] if rng else v
                ax.text(i, top + max(vals) * 0.02, f(v), ha="center", va="bottom", fontsize=9)
            ax.set_xticks([0, 1])
            ax.set_xticklabels(["Parquet", "CSV"], fontsize=9)
            ax.set_title(ttl, fontsize=9.5, color=INK2, loc="left", pad=8)
            ax.text(0.5, max(vals) * 1.12, f"×{vi(vals[1] / vals[0], 1)}", ha="center", fontsize=9.5, fontweight="bold", color=INK)
        header(fig, "Parquet nhanh hơn CSV và gọn hơn nhiều",
               f"Cùng {vi(rows)} dòng (3 tháng), cùng phép gom; CSV đọc với schema khai báo sẵn (không suy luận kiểu).")
        footer(fig, f"Nguồn: {SRC_BENCH}, results/04_benchmark/bench_runs.csv (dung lượng), spark_ui/format/stages_summary.csv (byte đọc). Cột xanh = Parquet.")
        return fig, "Parquet so với CSV", [SRC_BENCH, "results/04_benchmark/bench_runs.csv", "results/04_benchmark/spark_ui/format/stages_summary.csv"]

    # ---------- fig04: partition pruning ----------
    def fig04():
        b = bench().loc["pruning"].reset_index().set_index("config")
        k_part = [k for k in b.index if k.startswith("partition_filter")][0]
        k_func = [k for k in b.index if k.startswith("function_filter")][0]
        t = [b.loc[k_part, "median_s"], b.loc[k_func, "median_s"]]
        rng = [(b.loc[k, "min_s"], b.loc[k, "max_s"]) for k in (k_part, k_func)]
        panels = [("Thời gian truy vấn (giây)", t, lambda v: f"{vi(v, 2)} s", rng)]
        srcs = [SRC_BENCH]
        try:
            s = read("04_benchmark/spark_ui/pruning/stages_summary.csv")
            s = s[(s["inputBytes"] > 0) & (s["status"] == "COMPLETE") & s["name"].str.startswith("first")]
            rows_m = int(b.loc[k_part, "rows"])
            part_stages = s[s.inputRecords == rows_m]
            first_id = int(part_stages.stageId.min())
            by_part = part_stages["inputBytes"].median()
            by_func = s[(s.stageId > part_stages.stageId.max()) & (s.inputRecords > rows_m * 3)]["inputBytes"].median()
            panels.append(("Byte Spark đọc vào (MB)", [by_part / 1e6, by_func / 1e6], lambda v: f"{vi(v, 1)} MB", None))
            srcs.append("results/04_benchmark/spark_ui/pruning/stages_summary.csv")
        except (FileNotFoundError, KeyError, ValueError):
            pass
        n = len(panels)
        fig, axs = new_fig(3.2 * n + 1.0, 4.4, n, left=0.07, right=0.98, wspace=0.42, bottom=0.2)
        for ax, (ttl, vals, f, rg) in zip(axs, panels):
            style(ax)
            ax.set_xlim(-0.7, 1.7)
            ax.set_ylim(0, max(vals) * 1.22)
            for i, v in enumerate(vals):
                vbar(ax, i, v, 0.55, BLUE if i == 0 else DEEMPH)
                if rg:
                    whisker(ax, i, *rg[i], cap=0.05)
                ax.text(i, (rg[i][1] if rg else v) + max(vals) * 0.02, f(v), ha="center", va="bottom", fontsize=9)
            ax.set_xticks([0, 1])
            ax.set_xticklabels(["Lọc theo\ncột phân vùng", "Lọc bằng hàm\ntrên cột giờ"], fontsize=9)
            ax.set_title(ttl, fontsize=9.5, color=INK2, loc="left", pad=8)
            ax.text(0.5, max(vals) * 1.12, f"×{vi(vals[1] / vals[0], 1)}", ha="center", fontsize=9.5, fontweight="bold", color=INK)
        header(fig, "Lọc theo cột phân vùng giúp bỏ qua cả thư mục dữ liệu",
               "Cùng kết quả (1 tháng), nhưng lọc bằng hàm trên cột giờ buộc Spark đọc toàn bộ 12 tháng.")
        footer(fig, f"Nguồn: {SRC_BENCH}, spark_ui/pruning/stages_summary.csv. Cột xanh = lọc theo month (cột phân vùng).")
        return fig, "Partition pruning", srcs

    # ---------- fig05: số nhân ----------
    def fig05():
        b = bench().loc["cores"].reset_index()
        b["c"] = b.config.str.extract(r"\[(\d+)\]")[0].astype(int)
        b = b.sort_values("c")
        t1 = float(b.median_s.iloc[0])
        b["speed"] = t1 / b.median_s
        fig, (ax,) = new_fig(7.4, 4.5, 1, left=0.10, right=0.93, bottom=0.17)
        style(ax)
        mx = float(b.c.max())
        ax.set_xlim(0.5, mx + 0.8)
        ax.set_ylim(0, mx + 0.6)
        ax.plot([1, mx], [1, mx], color=MUTED, lw=1.2, ls=(0, (4, 3)), zorder=2)
        ax.text(mx + 0.1, mx, "Lý tưởng", fontsize=9, color=INK2, va="center")
        ax.plot(b.c, b.speed, color=BLUE, lw=1.5, solid_capstyle="round", zorder=3)
        for _, r in b.iterrows():
            dot(ax, r.c, r.speed, BLUE)
            if r.c > 1:
                last = r.c == mx
                ax.text(r.c, r.speed - 0.3, f"×{vi(r.speed, 2)}\n({vi(r.speed / r.c * 100)}% hiệu suất)", fontsize=9,
                        va="top", ha="center", color=INK, linespacing=1.2)
        ax.set_xticks(b.c)
        ax.set_xlabel("Số nhân Spark (local[N])", fontsize=9)
        ax.set_ylabel("Tăng tốc so với 1 nhân (lần)", fontsize=9)
        ntask = ""
        try:
            sc = read(f"04_benchmark/spark_ui/cores_{int(b.c.iloc[-1])}/stages_summary.csv")
            nt = sc[(sc.inputRecords == int(b["rows"].iloc[0])) & sc["name"].str.startswith("save")]["numTasks"]
            ntask = f" Mỗi lượt chỉ có {int(nt.max())} task nên khó chia đều cho nhiều nhân (giả thuyết)."
        except (FileNotFoundError, ValueError, KeyError):
            pass
        header(fig, "Tăng nhân có lợi nhưng hiệu suất giảm nhanh sau 4 nhân",
               f"12 tháng: {vi(t1, 2)} s với 1 nhân còn {vi(float(b.median_s.iloc[-1]), 2)} s với {int(b.c.iloc[-1])} nhân.{ntask}")
        footer(fig, f"Nguồn: {SRC_BENCH}. Median của 3 lượt đo. Đường nét đứt = tăng tốc tuyến tính lý tưởng.")
        return fig, "Tăng tốc theo số nhân", [SRC_BENCH]

    # ---------- fig06: chuyến theo ngày ----------
    def fig06():
        d = read("03_aggregate/kpi_daily.csv")
        d["pickup_date"] = pd.to_datetime(d["pickup_date"])
        d = d.sort_values("pickup_date")
        d["ma7"] = d.trips.rolling(7, center=True, min_periods=4).mean()
        fig, (ax,) = new_fig(9.2, 4.5, 1, left=0.085, right=0.97, bottom=0.12, top=0.82)
        style(ax)
        ax.plot(d.pickup_date, d.trips, color=DEEMPH, lw=1.0, zorder=2, label="Theo ngày")
        ax.plot(d.pickup_date, d.ma7, color=BLUE, lw=1.5, zorder=3, solid_capstyle="round", label="Trung bình trượt 7 ngày")
        ax.set_ylim(0, d.trips.max() * 1.1)
        ax.yaxis.set_major_formatter(FuncFormatter(compact))
        ax.set_ylabel("Số chuyến mỗi ngày", fontsize=9)
        import matplotlib.dates as mdates
        ax.set_xlim(d.pickup_date.min() - pd.Timedelta(days=3), d.pickup_date.max() + pd.Timedelta(days=3))
        ax.set_xticks([pd.Timestamp(year=int(d.pickup_date.dt.year.iloc[0]), month=m, day=15) for m in range(1, 13)])
        ax.set_xticklabels([f"T{m}" for m in range(1, 13)], fontsize=9)
        marks = [("2024-05-27", "Memorial Day", (0, -0.5)), ("2024-07-04", "4/7", (0, -0.5)), ("2024-11-28", "Lễ Tạ ơn", (0, -0.5)), ("2024-12-25", "Giáng sinh", (0, -0.5))]
        for ds, lab, _ in marks:
            r = d[d.pickup_date == pd.Timestamp(ds)]
            if r.empty:
                continue
            x, y = r.pickup_date.iloc[0], int(r.trips.iloc[0])
            dot(ax, x, y, BLUE)
            ax.annotate(f"{lab}\n{vi(y)}", (x, y), xytext=(0, -16), textcoords="offset points", ha="center", va="top", fontsize=8.5, color=INK2)
        ax.legend(loc="lower left", frameon=False, fontsize=9, ncol=2, bbox_to_anchor=(0.0, 1.0))
        tot = int(d.trips.sum())
        header(fig, "Nhu cầu ổn định quanh năm, giảm sâu vào ngày lễ",
               f"{vi(tot)} chuyến sạch trong {len(d)} ngày; trung bình {vi(tot / len(d))} chuyến/ngày; ngày thấp nhất {d.loc[d.trips.idxmin(), 'pickup_date']:%d/%m} ({vi(int(d.trips.min()))}).")
        ax.legend(loc="upper left", frameon=False, fontsize=9, ncol=2)
        footer(fig, "Nguồn: results/03_aggregate/kpi_daily.csv (dữ liệu sau làm sạch bản 2).")
        return fig, "Số chuyến theo ngày", ["results/03_aggregate/kpi_daily.csv"]

    # ---------- fig07: nhiệt độ giờ x thứ ----------
    def fig07():
        h = read("03_aggregate/kpi_hourly_dow.csv")
        piv = h.pivot(index="pickup_dow", columns="pickup_hour", values="trips").loc[DOW_ORDER] / 1e3
        cmap = LinearSegmentedColormap.from_list("seq", SEQ)
        fig, (ax, cax) = new_fig(9.0, 4.2, 2, left=0.07, right=0.90, widths=[40, 1], wspace=0.04, bottom=0.17, top=0.80)
        mesh = ax.pcolormesh(np.arange(25), np.arange(8), piv.values, cmap=cmap, edgecolors=SURF, linewidth=1.44)
        ax.set_ylim(7, 0)
        ax.set_xticks(np.arange(0, 24, 2) + 0.5)
        ax.set_xticklabels([f"{x}h" for x in range(0, 24, 2)], fontsize=9)
        ax.set_yticks(np.arange(7) + 0.5)
        ax.set_yticklabels([DOW_VI[d] for d in DOW_ORDER], fontsize=9)
        ax.tick_params(length=0)
        for s in ax.spines.values():
            s.set_visible(False)
        ax.set_xlabel("Giờ đón khách", fontsize=9)
        cb = fig.colorbar(mesh, cax=cax)
        cb.outline.set_visible(False)
        cb.ax.tick_params(length=0, labelsize=8.5)
        cb.set_label("Số chuyến (nghìn)", fontsize=9)
        iy, ix = np.unravel_index(np.argmax(piv.values), piv.shape)
        header(fig, "Đỉnh nhu cầu: chiều tối ngày thường; khuya cuối tuần cũng đông",
               f"Đỉnh cả năm: {DOW_VI[DOW_ORDER[iy]]} lúc {ix}h ({vi(piv.values[iy, ix] * 1e3)} chuyến). Tổng 12 tháng, theo giờ đón và thứ.")
        footer(fig, "Nguồn: results/03_aggregate/kpi_hourly_dow.csv. Màu càng đậm càng nhiều chuyến.")
        return fig, "Nhiệt độ giờ × thứ", ["results/03_aggregate/kpi_hourly_dow.csv"]

    # ---------- fig08: 15 khu vực đón nhiều nhất ----------
    def fig08():
        z = read("03_aggregate/kpi_zone_pickup.csv")
        top = z.nlargest(15, "trips").reset_index(drop=True)
        air = top["zone"].str.contains("Airport")
        fig, (ax,) = new_fig(8.4, 5.3, 1, left=0.27, right=0.93, bottom=0.14, top=0.82)
        style(ax, grid="x", ticks_y=True)
        ax.set_ylim(len(top) - 0.4, -0.7)
        ax.set_xlim(0, top.trips.max() * 1.18)
        ax.xaxis.set_major_formatter(FuncFormatter(compact))
        for i, r in top.iterrows():
            hbar(ax, i, r.trips, 0.62, BLUE if air.iloc[i] else DEEMPH)
            ax.text(r.trips + top.trips.max() * 0.012, i, f"{vi(r.trips / 1e6, 2)} tr", va="center", fontsize=9, color=INK)
        ax.set_yticks(range(len(top)))
        ax.set_yticklabels(list(top.zone), fontsize=9)
        ax.set_xlabel("Số chuyến đón trong năm", fontsize=9)
        ax.spines["bottom"].set_color(AXIS)
        jfk = z[z.zone.str.contains("JFK")].iloc[0]
        share_t = jfk.trips / z.trips.sum() * 100
        share_r = jfk.revenue / z.revenue.sum() * 100
        from matplotlib.lines import Line2D
        ax.legend([Line2D([0], [0], color=BLUE, lw=6), Line2D([0], [0], color=DEEMPH, lw=6)], ["Sân bay", "Khu khác"], loc="lower right", frameon=False, fontsize=9)
        header(fig, "JFK dẫn đầu số chuyến, doanh thu vượt xa tỉ lệ chuyến",
               f"JFK = {vi(share_t, 1)}% số chuyến nhưng {vi(share_r, 1)}% doanh thu (theo total_amount). Trừ JFK và LaGuardia (Queens), các khu còn lại đều ở Manhattan.")
        footer(fig, "Nguồn: results/03_aggregate/kpi_zone_pickup.csv. 'tr' = triệu chuyến.")
        return fig, "15 khu vực đón nhiều nhất", ["results/03_aggregate/kpi_zone_pickup.csv"]

    # ---------- fig09: cơ cấu thanh toán ----------
    def fig09():
        p = read("03_aggregate/kpi_payment_month.csv")
        piv = p.pivot(index="month", columns="payment_label", values="pct_of_month_trips").fillna(0)
        piv["Khác (Dispute, No charge)"] = piv.get("Dispute", 0) + piv.get("No charge", 0)
        series = [("Credit card", "Thẻ tín dụng", BLUE), ("Cash", "Tiền mặt", ORANGE), ("Flex Fare", "Flex Fare", AQUA), ("Khác (Dispute, No charge)", "Khác", DEEMPH)]
        fig, (ax,) = new_fig(8.6, 4.6, 1, left=0.08, right=0.97, bottom=0.14, top=0.80)
        style(ax)
        ax.set_ylim(0, 100)
        ax.set_xlim(0.4, 12.6)
        months = list(piv.index)
        bottom = np.zeros(len(months))
        for key, lab, color in series:
            vals = piv[key].values
            ax.bar(months, vals, bottom=bottom, width=0.62, color=color, edgecolor=SURF, linewidth=1.44, zorder=3, label=lab)
            for m, v, b0 in zip(months, vals, bottom):
                if m in (months[0], months[-1], int(piv["Flex Fare"].idxmax())) and v >= 4.5:
                    ax.text(m, b0 + v / 2, f"{vi(v, 0)}%", ha="center", va="center", fontsize=8.5, color=ink_on(color), zorder=4)
            bottom += vals
        ax.set_xticks(months)
        ax.set_xticklabels([f"T{m}" for m in months], fontsize=9)
        ax.set_ylabel("Tỉ lệ số chuyến trong tháng (%)", fontsize=9)
        ax.legend(loc="lower left", bbox_to_anchor=(0.0, 1.0), ncol=4, frameon=False, fontsize=9, handlelength=1.0)
        f_min, f_max = piv["Flex Fare"].min(), piv["Flex Fare"].max()
        header(fig, "Thẻ tín dụng chiếm ~3/4 số chuyến; Flex Fare tăng mạnh trong năm",
               f"Flex Fare từ {vi(f_min, 1)}% (T{int(piv['Flex Fare'].idxmin())}) lên {vi(f_max, 1)}% (T{int(piv['Flex Fare'].idxmax())}). Chỉ gắn nhãn vài tháng; số đầy đủ ở CSV nguồn.")
        footer(fig, "Nguồn: results/03_aggregate/kpi_payment_month.csv. 'Khác' = Dispute + No charge (cộng lại < 2%).")
        return fig, "Cơ cấu thanh toán theo tháng", ["results/03_aggregate/kpi_payment_month.csv"]

    # ---------- fig10: chất lượng theo nhà cung cấp ----------
    def fig10():
        v = read("03_aggregate/kpi_vendor_dq.csv")
        v = v[v.vendorid.isin([1, 2])]
        fig, axs = new_fig(9.4, 4.9, 2, left=0.07, right=0.90, wspace=0.34, bottom=0.13, top=0.74)  # top thấp hơn: phụ đề 2 dòng không đè tiêu đề panel
        for ax, col, ttl in ((axs[0], "zero_distance_pct", "Dòng có quãng đường bằng 0 (%)"), (axs[1], "total_mismatch_pct", "total_amount lệch tổng các khoản (%)")):
            style(ax)
            for vid, color, name in ((1, BLUE, "Creative Mobile"), (2, ORANGE, "Curb Mobility")):
                s = v[v.vendorid == vid].sort_values("month")
                ax.plot(s.month, s[col], color=color, lw=1.5, solid_capstyle="round", zorder=3, label=name)
                dot(ax, s.month.iloc[-1], s[col].iloc[-1], color)
                ax.text(s.month.iloc[-1] + 0.25, s[col].iloc[-1], name.split()[0], fontsize=8.5, va="center", color=INK2)
            ax.set_xticks(range(1, 13))
            ax.set_xticklabels([f"{m}" for m in range(1, 13)], fontsize=8.5)
            ax.set_xlim(0.7, 13.4)
            ax.set_ylim(0, None)
            ax.set_xlabel("Tháng", fontsize=9)
            ax.set_title(ttl, fontsize=9.5, color=INK2, loc="left", pad=8)
        axs[0].legend(loc="upper right", frameon=False, fontsize=9)
        c1 = v[(v.vendorid == 1) & (v.month <= 3)].zero_distance_pct.mean()
        c1b = v[(v.vendorid == 1) & (v.month >= 4)].zero_distance_pct.mean()
        m1 = v[v.vendorid == 1].total_mismatch_pct
        m2 = v[v.vendorid == 2].total_mismatch_pct
        header(fig, "Hai nhà cung cấp lớn ghi dữ liệu theo hai cách khác nhau",
               f"Creative Mobile: quãng đường 0 trung bình {vi(c1, 1)}% ở T1–T3, {vi(c1b, 1)}% từ T4. total_amount lệch {vi(m1.min())}–{vi(m1.max())}% dòng (Curb Mobility {vi(m2.min())}–{vi(m2.max())}%).")
        footer(fig, "Nguồn: results/03_aggregate/kpi_vendor_dq.csv. Bỏ Myle và Helix (vài trăm dòng/năm). Lệch = |total − tổng các khoản| > 0,01 USD.")
        return fig, "Chất lượng dữ liệu theo nhà cung cấp", ["results/03_aggregate/kpi_vendor_dq.csv"]

    # ---------- fig11: luật làm sạch bản 1 và bản 2 ----------
    def fig11():
        r1 = read("02_clean/rule_counts.csv")[["rule", "rows"]].rename(columns={"rows": "v1"})
        r2 = read("02_clean_v2/rule_counts.csv")[["rule", "rows"]].rename(columns={"rows": "v2"})
        m = r1.merge(r2, on="rule", how="outer").fillna(0)
        m = m[~m.rule.isin(["PAX_REPAIRED_TO_NULL", "ZERO_DISTANCE_FLAGGED", "DUPLICATES_REMOVED", "TIME_NULL", "ZONE_INVALID"])]
        m["mx"] = m[["v1", "v2"]].max(axis=1)
        m = m[m.mx >= 1000].sort_values("mx", ascending=False).reset_index(drop=True)
        fig, (ax,) = new_fig(9.0, 5.4, 1, left=0.26, right=0.93, bottom=0.12, top=0.82)
        style(ax, grid="x")
        ax.set_xlim(0, m.mx.max() * 1.2)
        ax.set_ylim(len(m) - 0.3, -0.7)
        ax.xaxis.set_major_formatter(FuncFormatter(compact))
        for i, r in m.iterrows():
            for off, val, color in ((-0.2, r.v1, DEEMPH), (0.2, r.v2, BLUE)):
                if val > 0:
                    hbar(ax, i + off, val, 0.34, color)
                ax.text(val + m.mx.max() * 0.012, i + off, vi(val), va="center", fontsize=8.5, color=INK)
        ax.set_yticks(range(len(m)))
        ax.set_yticklabels(m.rule, fontsize=8.5)
        ax.set_xlabel("Số dòng vi phạm luật (một dòng có thể vi phạm nhiều luật)", fontsize=9)
        from matplotlib.lines import Line2D
        ax.legend([Line2D([0], [0], color=DEEMPH, lw=6), Line2D([0], [0], color=BLUE, lw=6)], ["Bản 1 (02_clean.py)", "Bản 2 (02_clean_v2.py)"], loc="lower right", frameon=False, fontsize=9)
        t1 = int(pd.read_csv(rr / "02_clean/clean_summary_by_month.csv").rows_rejected.sum())
        t2 = int(pd.read_csv(rr / "02_clean_v2/clean_summary_by_month.csv").rows_rejected.sum())
        tin = int(pd.read_csv(rr / "02_clean_v2/clean_summary_by_month.csv").rows_in.sum())
        header(fig, "Bản 2 giữ lại các dòng quãng đường 0 và loại thêm dòng bất thường rõ ràng",
               f"Dòng bị loại: {vi(t1)} ({vi(t1 / tin * 100, 2)}%) → {vi(t2)} ({vi(t2 / tin * 100, 2)}%) trên {vi(tin)} dòng. Quãng đường 0 nay chỉ gắn cờ is_zero_distance.")
        footer(fig, "Nguồn: results/02_clean/ và results/02_clean_v2/ (rule_counts.csv, clean_summary_by_month.csv). Chỉ hiện luật có ≥ 1.000 dòng.")
        return fig, "Số dòng vi phạm theo luật: bản 1 và bản 2", ["results/02_clean/rule_counts.csv", "results/02_clean_v2/rule_counts.csv"]


    # ---------- fig12: RAM đỉnh và thời gian, Pandas so với Spark (04b) ----------
    def fig12():
        r = read("04b_pandas_memory/mem_runs.csv")
        r = r[r.status == "OK"]
        g = r.groupby(["engine", "months"]).agg(t=("query_seconds", "median"), ram=("peak_rss_gb", "median"), rows=("rows", "first")).reset_index()
        spec = [("spark", "Spark local[2] (4 cột)", BLUE), ("pandas_narrow", "Pandas, đọc 4 cột", ORANGE), ("pandas_wide", "Pandas, đọc mọi cột", AQUA)]
        fig, (a1, a2) = new_fig(9.0, 4.5, 2, left=0.07, right=0.97, wspace=0.24, bottom=0.17, top=0.78)
        ms = sorted(g.months.unique())
        for ax, col, ylab, fmt in ((a1, "ram", "RAM đỉnh của tiến trình (GB)", 1), (a2, "t", "Thời gian truy vấn (giây)", 1)):
            style(ax)
            for eng, name, color in spec:
                d = g[g.engine == eng].sort_values("months")
                if d.empty:
                    continue
                ax.plot(d.months, d[col], color=color, lw=1.5, solid_capstyle="round", label=name, zorder=3)
                for xx, yy in zip(d.months, d[col]):
                    dot(ax, xx, yy, color)
                ax.text(d.months.iloc[-1] + 0.2, d[col].iloc[-1], f"{vi(d[col].iloc[-1], fmt)}", fontsize=9, va="center", color=INK2)
            ax.set_xticks(ms)
            ax.set_xlim(0, max(ms) + 1.6)
            ax.set_ylim(0, None)
            ax.set_xlabel("Số tháng dữ liệu (Yellow Taxi 2024)", fontsize=9)
            ax.set_ylabel(ylab, fontsize=9)
        a2.legend(loc="upper left", frameon=False, fontsize=9, handlelength=1.6)
        m = max(ms)
        gm = g[g.months == m].set_index("engine")
        header(fig, "Spark giữ RAM gần như phẳng; Pandas đọc mọi cột cần ~" + vi(gm.loc["pandas_wide", "ram"], 0) + " GB",
               f"{m} tháng ({vi(int(gm.loc['spark', 'rows']))} dòng): Spark {vi(gm.loc['spark', 'ram'], 1)} GB, Pandas 4 cột {vi(gm.loc['pandas_narrow', 'ram'], 1)} GB, "
               f"Pandas mọi cột {vi(gm.loc['pandas_wide', 'ram'], 1)} GB. Pandas 4 cột lại nhanh hơn Spark ({vi(gm.loc['pandas_narrow', 't'], 1)} s so với {vi(gm.loc['spark', 't'], 1)} s).")
        footer(fig, "Nguồn: results/04b_pandas_memory/mem_runs.csv. Median của 3 lượt, mỗi lượt một tiến trình mới (Spark không khởi động trước nên "
                    "thời gian khác 04_benchmark). RAM = RSS đỉnh cả cây tiến trình, kể cả JVM.")
        return fig, "RAM đỉnh và thời gian: Pandas so với Spark", ["results/04b_pandas_memory/mem_runs.csv"]


    # ---------- fig13: RAM đỉnh theo số dòng, Yellow và HVFHV ----------
    def fig13():
        ry = read("04b_pandas_memory/mem_runs.csv")
        rh = read("04b_pandas_memory/fhvhv/mem_runs.csv")
        spec = [("spark", "Spark local[2]", BLUE), ("pandas_narrow", "Pandas, 4 cột", ORANGE), ("pandas_wide", "Pandas, mọi cột", AQUA)]
        fig, (a1, a2) = new_fig(9.0, 4.6, 2, left=0.07, right=0.97, wspace=0.14, bottom=0.17, top=0.74)
        gs = []
        for ax, r, ttl in ((a1, ry, "Yellow Taxi (20 cột)"), (a2, rh, "HVFHV Uber/Lyft (24 cột)")):
            style(ax)
            r = r[r.status == "OK"]
            g = r.groupby(["engine", "months"]).agg(ram=("peak_rss_gb", "median"), rows=("rows", "first")).reset_index()
            gs.append(g)
            for eng, name, color in spec:
                d = g[g.engine == eng].sort_values("rows")
                ax.plot(d.rows / 1e6, d.ram, color=color, lw=1.5, solid_capstyle="round", label=name, zorder=3)
                for xx, yy in zip(d.rows / 1e6, d.ram):
                    dot(ax, xx, yy, color)
                ax.text(d.rows.iloc[-1] / 1e6 + 0.8, d.ram.iloc[-1], f"{vi(d.ram.iloc[-1], 1)}", fontsize=9, va="center", color=INK2)
            ax.set_title(ttl, fontsize=9.5, color=INK2, loc="left")
            ax.set_xlim(0, 48)
            ax.set_ylim(0, 12.5)
            ax.set_xlabel("Số dòng đọc (triệu)", fontsize=9)
        a1.set_ylabel("RAM đỉnh của tiến trình (GB)", fontsize=9)
        a2.tick_params(axis="y", labelleft=False)
        a1.legend(loc="upper left", frameon=False, fontsize=9, handlelength=1.6)
        ny = gs[0][gs[0].months == gs[0].months.max()].set_index("engine")
        nh = gs[1][gs[1].months == gs[1].months.max()].set_index("engine")
        header(fig, "RAM của Pandas gần như chỉ phụ thuộc số dòng và số cột đọc",
               f"Cùng cỡ ~40 triệu dòng ({vi(int(ny.loc['spark', 'rows']) / 1e6, 1)} triệu Yellow, {vi(int(nh.loc['spark', 'rows']) / 1e6, 1)} triệu HVFHV): "
               f"Pandas 4 cột {vi(ny.loc['pandas_narrow', 'ram'], 1)} và {vi(nh.loc['pandas_narrow', 'ram'], 1)} GB; Spark {vi(ny.loc['spark', 'ram'], 1)} và {vi(nh.loc['spark', 'ram'], 1)} GB.")
        footer(fig, "Nguồn: results/04b_pandas_memory/mem_runs.csv và results/04b_pandas_memory/fhvhv/mem_runs.csv. Median 3 lượt. RAM đỉnh của Pandas đọc mọi cột "
                    "có thể bị đánh thấp: máy chỉ còn ~9 GB trống nên Windows nén/phân trang bộ nhớ.")
        return fig, "RAM đỉnh theo số dòng: Yellow và HVFHV", ["results/04b_pandas_memory/mem_runs.csv", "results/04b_pandas_memory/fhvhv/mem_runs.csv"]

    # ---------- fig14: bất thường trong HVFHV ----------
    def fig14():
        d = read("06_profile_hvfhv/hv_anomalies_by_month.csv")
        total = int(d.rows.sum())
        names = {"pickup_before_request": "Đón trước giờ gọi xe", "driver_pay_gt_1_5x_fare": "Tiền tài xế > 1,5 × giá cước",
                 "miles_le_0": "Quãng đường ≤ 0", "fare_le_0": "Giá cước ≤ 0", "miles_gt_100": "Quãng đường > 100 dặm",
                 "wait_gt_1h": "Chờ xe > 1 giờ", "driver_pay_lt_0": "Tiền tài xế âm", "time_gt_6h": "Chuyến dài hơn 6 giờ",
                 "speed_gt_80mph": "Tốc độ TB > 80 mph", "time_le_0": "Thời gian ≤ 0"}
        zero = [n for n in ("dropoff_before_pickup", "pickup_outside_month", "tips_lt_0", "trip_time_vs_timestamps_gt_60s") if d[n].sum() == 0]
        v = pd.Series({names[k]: int(d[k].sum()) for k in names}).sort_values(ascending=False)
        fig, (ax,) = new_fig(8.6, 4.8, 1, left=0.30, right=0.93, bottom=0.14, top=0.78)
        style(ax, grid="x")
        ax.set_ylim(len(v) - 0.4, -0.7)
        ax.set_xlim(0, v.max() * 1.18)
        ax.xaxis.set_major_formatter(FuncFormatter(compact))
        for i, (k, x) in enumerate(v.items()):
            hbar(ax, i, x, 0.62, BLUE if i < 2 else DEEMPH)
            ax.text(x + v.max() * 0.012, i, f"{vi(x)}  ({vi(x / total * 100, 2)}%)", va="center", fontsize=9, color=INK)
        ax.set_yticks(range(len(v)))
        ax.set_yticklabels(list(v.index), fontsize=9)
        ax.set_xlabel("Số dòng (2 tháng 2024)", fontsize=9)
        header(fig, "Lỗi trong HVFHV hiếm; chỉ hai nhóm lệch khoảng 1% số dòng",
               f"{vi(total)} dòng, hai tháng đầu 2024. Hai nhóm đầu mỗi nhóm khoảng 1% số dòng; mọi lỗi còn lại dưới 0,05%."
               + (" Không có dòng nào: trả khách trước khi đón, đón ngoài tháng, tip âm, thời gian lệch mốc giờ." if len(zero) == 4 else ""))
        footer(fig, "Nguồn: results/06_profile_hvfhv/hv_anomalies_by_month.csv. Một dòng có thể vi phạm nhiều mục. Chỉ đo, chưa làm sạch.")
        return fig, "Bất thường trong HVFHV", ["results/06_profile_hvfhv/hv_anomalies_by_month.csv"]


    # ---------- fig15: tiền tài xế > 1,5 × giá cước theo ngày, Uber so với Lyft ----------
    def fig15():
        import matplotlib.dates as mdates
        d = read("06b_diagnose_hvfhv/h3_ratio_daily.csv")
        d["day"] = pd.to_datetime(d["day"])
        u = d[d.company == "Uber"].sort_values("day").reset_index(drop=True)
        l = d[d.company == "Lyft"].sort_values("day")
        base = float(u.ratio_gt_1_5_pct.median())
        hot = u[u.ratio_gt_1_5_pct > 5 * base]
        fig, (ax,) = new_fig(9.0, 4.4, 1, left=0.08, right=0.97, bottom=0.15, top=0.76)
        style(ax)
        for dfx, color, name in ((u, BLUE, "Uber"), (l, ORANGE, "Lyft")):
            ax.plot(dfx.day, dfx.ratio_gt_1_5_pct, color=color, lw=1.5, solid_capstyle="round", solid_joinstyle="round", label=name, zorder=3)
        if len(hot):
            d0, d1 = hot.day.min(), hot.day.max()
            ax.axvspan(d0 - pd.Timedelta(hours=12), d1 + pd.Timedelta(hours=12), color=GRID, alpha=0.6, zorder=1, lw=0)
            tot = int(u[u.day.dt.month == d0.month].ratio_gt_1_5.sum())
            wk = int(hot.ratio_gt_1_5.sum())
            ax.text(d0 - pd.Timedelta(days=1), hot.ratio_gt_1_5_pct.max(), f"{d0.day}/{d0.month}–{d1.day}/{d1.month}: Uber {vi(hot.ratio_gt_1_5.sum() / hot.rows.sum() * 100, 1)}%", ha="right", va="center", fontsize=9, color=INK)
        ax.set_ylim(0, None)
        ax.set_ylabel("% chuyến có driver_pay > 1,5 × giá cước", fontsize=9)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
        ax.xaxis.set_major_locator(mdates.DayLocator(bymonthday=[1, 8, 15, 22]))
        ax.legend(loc="upper left", frameon=False, fontsize=9, handlelength=1.6)
        header(fig, "Cú nhảy của tháng 2 là một tuần bất thường của riêng Uber",
               (f"Uber {vi(hot.ratio_gt_1_5_pct.mean(), 1)}% trong {len(hot)} ngày ({hot.day.min().day}–{hot.day.max().day}/{hot.day.min().month}), "
                f"chiếm {vi(wk / tot * 100)}% số dòng vượt của cả tháng; các ngày còn lại của tháng chỉ {vi(float(u[(u.day.dt.month == d0.month) & ~u.day.isin(hot.day)].ratio_gt_1_5.sum()) / float(u[(u.day.dt.month == d0.month) & ~u.day.isin(hot.day)].rows.sum()) * 100, 2)}%. Lyft đi ngang."
                if len(hot) else "Không thấy tuần bất thường."))
        footer(fig, "Nguồn: results/06b_diagnose_hvfhv/h3_ratio_daily.csv. Vùng xám = các ngày Uber vượt 5 lần mức trung vị. Nguyên nhân chưa biết (xem NOTES).")
        return fig, "driver_pay > 1,5 × giá cước theo ngày", ["results/06b_diagnose_hvfhv/h3_ratio_daily.csv"]

    figs = {"fig01_bench_scale": fig01, "fig02_bench_shuffle": fig02, "fig03_bench_format": fig03, "fig04_bench_pruning": fig04,
            "fig05_bench_cores": fig05, "fig06_kpi_daily_trips": fig06, "fig07_kpi_hour_dow_heatmap": fig07,
            "fig08_kpi_top_zones": fig08, "fig09_kpi_payment_mix": fig09, "fig10_dq_vendor": fig10, "fig11_clean_rules_v1_v2": fig11,
            "fig12_mem_pandas_vs_spark": fig12,
            "fig13_mem_by_rows_two_datasets": fig13, "fig14_hvfhv_anomalies": fig14,
            "fig15_hvfhv_pay_ratio_daily": fig15}
    failed = 0
    for name, fn in figs.items():
        if a.only and not any(name.startswith(o) for o in a.only):
            continue
        try:
            fig, title, srcs = fn()
            save(fig, name, title, srcs)
        except FileNotFoundError as e:
            print(f"[bỏ qua] {name}: thiếu {e}")
            failed += 1
        except Exception as e:  # noqa: BLE001
            print(f"[LỖI] {name}: {type(e).__name__}: {e}")
            failed += 1
    pd.DataFrame(index).to_csv(out / "figures_index.csv", index=False, encoding="utf-8")
    print(f"\nXong: {len(index)} hình -> {out}" + (f" ({failed} hình bị bỏ qua/lỗi)" if failed else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

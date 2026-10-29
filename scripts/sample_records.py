"""Lay ban ghi mau cho bao cao muc 3.7 (chay: python scripts/sample_records.py).

Ghi results/sample_records/sample_records.txt, gom:
  1) 1 dong Yellow tho (data/raw/yellow_tripdata_<ym>.parquet, mac dinh 2024-10)
  2) cung chuyen do trong data/clean_v2/yellow (tim theo PU/DO/fare/total/distance/dropoff) -> doi chieu truc tiep
  3) 1 dong HVFHV tho (neu co data/raw/fhvhv_tripdata_2024-01.parquet)
Neu gio don trong ban sach lech so voi ban tho, in canh bao (nghi ngo mui gio, xem NOTES).
Tuy chon: --ym 2024-10
"""
import argparse, glob, os
import pyarrow.parquet as pq
import pyarrow.dataset as ds

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ap = argparse.ArgumentParser()
ap.add_argument("--ym", default="2024-10")
a = ap.parse_args()
OUT = os.path.join(ROOT, "results", "sample_records")
os.makedirs(OUT, exist_ok=True)
L = []

def rows(d):
    return [f"{k}: {v}" for k, v in d.items()]

raw_path = os.path.join(ROOT, "data", "raw", f"yellow_tripdata_{a.ym}.parquet")
if not os.path.exists(raw_path):
    c = glob.glob(os.path.join(ROOT, "data", "raw", "**", f"yellow_tripdata_{a.ym}*.parquet"), recursive=True)
    raw_path = c[0] if c else None
if raw_path:
    r = pq.ParquetFile(raw_path).read_row_group(0).slice(0, 1).to_pylist()[0]
    L += [f"== YELLOW RAW: {os.path.relpath(raw_path, ROOT)}"] + rows(r)
    clean_dir = os.path.join(ROOT, "data", "clean_v2", "yellow")
    if os.path.isdir(clean_dir):
        d = ds.dataset(clean_dir, format="parquet", partitioning="hive")
        f = ((ds.field("pulocationid") == r["PULocationID"]) & (ds.field("dolocationid") == r["DOLocationID"])
             & (ds.field("fare_amount") == r["fare_amount"]) & (ds.field("total_amount") == r["total_amount"])
             & (ds.field("trip_distance") == r["trip_distance"]))
        t = d.to_table(filter=f).slice(0, 3).to_pylist()
        L.append("")
        if t:
            # uu tien dong co cung gio tra khach neu co nhieu dong
            m = t[0]
            L += ["== YELLOW CLEAN (cung chuyen, tim theo PU/DO/fare/total/distance)"] + rows(m)
            L.append("")
            L.append(f"so_khop_toi_da_3: {len(t)}")
            if m.get("tpep_pickup_datetime") != r["tpep_pickup_datetime"]:
                L.append("CANH BAO: tpep_pickup_datetime ban sach khac ban tho (xem ghi chu mui gio trong NOTES).")
        else:
            L.append("Khong tim thay dong tuong ung trong clean_v2 (co the bi loai: xem yellow_rejected).")
    else:
        L.append("Khong co data/clean_v2/yellow")
else:
    L.append(f"Khong tim thay data/raw/yellow_tripdata_{a.ym}.parquet")

hv = os.path.join(ROOT, "data", "raw", "fhvhv_tripdata_2024-01.parquet")
if os.path.exists(hv):
    h = pq.ParquetFile(hv).read_row_group(0).slice(0, 1).to_pylist()[0]
    L += ["", f"== HVFHV RAW: {os.path.relpath(hv, ROOT)}"] + rows(h)

txt = "\n".join(L)
open(os.path.join(OUT, "sample_records.txt"), "w", encoding="utf-8").write(txt)
print(txt)

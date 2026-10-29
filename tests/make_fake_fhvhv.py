#!/usr/bin/env python3
"""Sinh dữ liệu GIẢ có cột giống TLC High Volume FHV (fhvhv) để thử code khi chưa tải dữ liệu thật.
KHÔNG dùng cho báo cáo. Chạy:  python tests/make_fake_fhvhv.py --out data/fake_raw --months 2024-01 2024-02 --rows 300000
Tên file: fhvhv_tripdata_YYYY-MM.parquet (đúng quy ước TLC) để 01_ingest_to_lake.py --type fhvhv --raw data/fake_raw đọc được."""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def make_month(ym, n, rng):
    y, m = map(int, ym.split("-"))
    start = pd.Timestamp(y, m, 1)
    end = start + pd.offsets.MonthBegin(1)
    req = start + pd.to_timedelta(rng.integers(0, int((end - start).total_seconds()), n), unit="s")
    pick = req + pd.to_timedelta(rng.integers(60, 900, n), unit="s")
    dur = rng.integers(120, 3600, n)
    miles = np.round(rng.gamma(2.0, 1.8, n), 2)
    fare = np.round(3 + miles * 2.2 + rng.normal(0, 1.5, n), 2)
    lic = rng.choice(["HV0003", "HV0005", "HV0004", "HV0002"], n, p=[0.74, 0.25, 0.005, 0.005])
    df = pd.DataFrame({
        "hvfhs_license_num": lic,
        "dispatching_base_num": rng.choice(["B03404", "B03406"], n),
        "originating_base_num": rng.choice(["B03404", "B03406", None], n),
        "request_datetime": req, "on_scene_datetime": req + pd.to_timedelta(60, unit="s"),
        "pickup_datetime": pick, "dropoff_datetime": pick + pd.to_timedelta(dur, unit="s"),
        "PULocationID": rng.integers(1, 264, n).astype("int32"), "DOLocationID": rng.integers(1, 264, n).astype("int32"),
        "trip_miles": miles, "trip_time": dur.astype("int64"),
        "base_passenger_fare": fare, "tolls": np.where(rng.random(n) < 0.05, 6.94, 0.0),
        "bcf": np.round(fare * 0.0275, 2), "sales_tax": np.round(fare * 0.08875, 2),
        "congestion_surcharge": np.where(rng.random(n) < 0.6, 2.75, 0.0), "airport_fee": np.where(rng.random(n) < 0.03, 2.5, 0.0),
        "tips": np.where(rng.random(n) < 0.2, np.round(fare * 0.15, 2), 0.0), "driver_pay": np.round(fare * 0.75, 2),
        "shared_request_flag": rng.choice(["Y", "N"], n, p=[0.03, 0.97]), "shared_match_flag": rng.choice(["Y", "N"], n, p=[0.02, 0.98]),
        "access_a_ride_flag": rng.choice(["Y", "N", " "], n, p=[0.01, 0.9, 0.09]), "wav_request_flag": "N", "wav_match_flag": rng.choice(["Y", "N"], n, p=[0.04, 0.96]),
    })
    bad = rng.random(n) < 0.002  # một ít dòng bất thường: quãng đường/thời gian 0, tiền âm
    df.loc[bad, "trip_miles"] = 0.0
    df.loc[rng.random(n) < 0.001, "trip_time"] = 0
    df.loc[rng.random(n) < 0.001, "driver_pay"] = -5.0
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/fake_raw")
    ap.add_argument("--months", nargs="+", default=["2024-01", "2024-02"])
    ap.add_argument("--rows", type=int, default=300000)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(a.seed)
    for ym in a.months:
        p = out / f"fhvhv_tripdata_{ym}.parquet"
        pq.write_table(pa.Table.from_pandas(make_month(ym, a.rows, rng), preserve_index=False), p)
        print(f"ghi {p} ({a.rows:,} dòng)")


if __name__ == "__main__":
    main()

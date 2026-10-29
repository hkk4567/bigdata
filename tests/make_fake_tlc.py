#!/usr/bin/env python3
"""Sinh dữ liệu GIẢ có hình dạng giống TLC Yellow Taxi để thử pipeline khi chưa có dữ liệu thật
(và làm dữ liệu mẫu cho phần "khả năng tái lập"). Cố ý tạo các khác biệt/lỗi thường gặp ở dữ liệu thật:
  - 2024: cột "Airport_fee", passenger_count kiểu double ở tháng lẻ; từ 2025: "airport_fee" + "cbd_congestion_fee"
  - dòng lỗi: giá âm, trả khách trước đón khách, ngày đón nằm ngoài tháng, passenger_count null/0/9, dòng trùng hệt
  - tuỳ chọn --ns-month: một tháng dùng timestamp nanosecond để thử đường dự phòng pyarrow

Chạy:  python tests/make_fake_tlc.py --out data/fake_raw --months 2024-01 2024-02 2025-01 --rows 200000
Nhớ:   dữ liệu giả KHÔNG dùng cho báo cáo; chỉ để kiểm thử mã.
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def make_month(ym: str, n: int, rng: np.random.Generator, ns: bool):
    y, m = map(int, ym.split("-"))
    start = pd.Timestamp(y, m, 1)
    end = start + pd.offsets.MonthBegin(1)
    secs = rng.integers(0, int((end - start).total_seconds()), n)
    pickup = start + pd.to_timedelta(secs, unit="s")
    dur = rng.integers(60, 3600, n)
    dropoff = pickup + pd.to_timedelta(dur, unit="s")
    dist = np.round(rng.gamma(2.0, 1.6, n), 2)
    fare = np.round(3 + dist * 2.8 + rng.normal(0, 1, n), 2)
    tip = np.round(np.where(rng.random(n) < 0.6, fare * rng.uniform(0.1, 0.25, n), 0), 2)
    df = pd.DataFrame({
        "VendorID": rng.choice([1, 2], n).astype("int32"),
        "tpep_pickup_datetime": pickup, "tpep_dropoff_datetime": dropoff,
        "passenger_count": rng.choice([1, 1, 1, 2, 3, 4, 5, 6], n).astype("float64"),
        "trip_distance": dist, "RatecodeID": rng.choice([1, 1, 1, 2, 5], n).astype("float64"),
        "store_and_fwd_flag": rng.choice(["N", "N", "N", "Y"], n),
        "PULocationID": rng.integers(1, 266, n).astype("int32"), "DOLocationID": rng.integers(1, 266, n).astype("int32"),
        "payment_type": rng.choice([1, 2, 3, 4], n, p=[.7, .25, .03, .02]).astype("int64"),
        "fare_amount": fare, "extra": rng.choice([0.0, 0.5, 1.0], n), "mta_tax": 0.5, "tip_amount": tip,
        "tolls_amount": np.where(rng.random(n) < 0.05, 6.94, 0.0), "improvement_surcharge": 1.0,
        "total_amount": np.round(fare + tip + 1.5, 2), "congestion_surcharge": rng.choice([0.0, 2.5], n),
    })
    df["Airport_fee" if y < 2025 else "airport_fee"] = rng.choice([0.0, 1.75], n)
    if y >= 2025:
        df["cbd_congestion_fee"] = rng.choice([0.0, 0.75], n)
    # chèn lỗi
    k = max(1, n // 100)
    idx = rng.choice(n, 4 * k, replace=False)
    df.loc[idx[:k], "fare_amount"] = -abs(df.loc[idx[:k], "fare_amount"])
    df.loc[idx[k:2 * k], "tpep_dropoff_datetime"] = df.loc[idx[k:2 * k], "tpep_pickup_datetime"] - pd.Timedelta(minutes=5)
    df.loc[idx[2 * k:3 * k], "tpep_pickup_datetime"] = pd.Timestamp(2002, 12, 31, 12, 0)
    df.loc[idx[3 * k:], "passenger_count"] = np.nan
    idx2 = rng.choice(n, 2 * k, replace=False)  # passenger_count bất thường (0 và 9) để thử luật SỬA
    df.loc[idx2[:k], "passenger_count"] = 0.0
    df.loc[idx2[k:], "passenger_count"] = 9.0
    df = pd.concat([df, df.iloc[:max(1, k // 2)]], ignore_index=True)  # dòng trùng hệt để thử bước loại trùng
    if m % 2 == 1 and y < 2025:  # tháng lẻ: passenger_count kiểu int64 (có null) thay vì double
        df["passenger_count"] = df["passenger_count"].astype("Int64")
    unit = "ns" if ns else "us"
    for c in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        df[c] = df[c].astype(f"datetime64[{unit}]")
    return pa.Table.from_pandas(df, preserve_index=False)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="data/fake_raw")
    ap.add_argument("--months", nargs="+", default=["2024-01", "2024-02", "2025-01"])
    ap.add_argument("--rows", type=int, default=200_000)
    ap.add_argument("--ns-month", help="tháng dùng timestamp nanosecond")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(a.seed)
    for ym in a.months:
        t = make_month(ym, a.rows, rng, ns=(ym == a.ns_month))
        pq.write_table(t, out / f"yellow_tripdata_{ym}.parquet")
        print(f"yellow_tripdata_{ym}.parquet  {t.num_rows:,} dòng")
    zones = pd.DataFrame({"LocationID": range(1, 266), "Borough": "Manhattan", "Zone": [f"Zone {i}" for i in range(1, 266)],
                          "service_zone": "Yellow Zone"})
    zones.to_csv(out / "taxi_zone_lookup.csv", index=False)
    print("taxi_zone_lookup.csv  265 dòng")


if __name__ == "__main__":
    main()

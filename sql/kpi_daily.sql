-- KPI theo ngày đón. Doanh thu = tổng total_amount như TLC báo cáo (lệch ~1% so với tổng các khoản, xem NOTES 02c).
-- avg_distance_mi bỏ qua dòng quãng đường 0 (đã chuyển thành null ở 02_clean_v2); tip_pct_card = tổng tip / tổng fare của chuyến thẻ (bền với ngoại lệ);
-- mean_tip_ratio_card_pct = trung bình tỉ lệ từng chuyến (nhạy ngoại lệ, giữ để đối chiếu); card_tip_gt_fare_rows = số chuyến thẻ có tip > fare.
SELECT to_date(tpep_pickup_datetime)                                         AS pickup_date,
       COUNT(*)                                                              AS trips,
       ROUND(SUM(total_amount), 2)                                           AS revenue,
       ROUND(AVG(fare_amount), 3)                                            AS avg_fare,
       ROUND(AVG(trip_distance), 3)                                          AS avg_distance_mi,
       ROUND(AVG(trip_duration_s) / 60.0, 2)                                 AS avg_duration_min,
       ROUND(SUM(CASE WHEN payment_type = 1 THEN tip_amount END)
             / SUM(CASE WHEN payment_type = 1 THEN fare_amount END) * 100, 2)  AS tip_pct_card,
       ROUND(AVG(CASE WHEN payment_type = 1 THEN tip_amount / fare_amount END) * 100, 2) AS mean_tip_ratio_card_pct,
       SUM(CASE WHEN payment_type = 1 AND tip_amount > fare_amount THEN 1 ELSE 0 END)    AS card_tip_gt_fare_rows,
       ROUND(AVG(CASE WHEN is_flex_fare THEN 1.0 ELSE 0.0 END) * 100, 2)     AS flex_share_pct,
       ROUND(AVG(CASE WHEN is_zero_distance THEN 1.0 ELSE 0.0 END) * 100, 2) AS zero_distance_pct
FROM trips
GROUP BY to_date(tpep_pickup_datetime)
ORDER BY pickup_date

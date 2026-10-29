-- Giám sát chất lượng theo nhà cung cấp x tháng: tỉ lệ quãng đường 0, Flex, và total_amount lệch tổng các khoản (> 0,01 USD).
SELECT year, month, vendorid,
       CASE vendorid WHEN 1 THEN 'Creative Mobile' WHEN 2 THEN 'Curb Mobility' WHEN 6 THEN 'Myle' WHEN 7 THEN 'Helix' ELSE 'Other' END AS vendor_label,
       COUNT(*)                                                                  AS trips,
       ROUND(AVG(CASE WHEN is_zero_distance THEN 1.0 ELSE 0.0 END) * 100, 3)     AS zero_distance_pct,
       ROUND(AVG(CASE WHEN is_flex_fare THEN 1.0 ELSE 0.0 END) * 100, 3)         AS flex_share_pct,
       ROUND(AVG(CASE WHEN ABS(total_amount - (fare_amount + extra + mta_tax + tip_amount + tolls_amount
             + improvement_surcharge + COALESCE(congestion_surcharge, 0) + COALESCE(airport_fee, 0)
             + COALESCE(cbd_congestion_fee, 0))) > 0.011 THEN 1.0 ELSE 0.0 END) * 100, 3) AS total_mismatch_pct,
       ROUND(AVG(fare_amount), 3)                                                AS avg_fare
FROM trips
GROUP BY year, month, vendorid
ORDER BY year, month, vendorid

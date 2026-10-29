-- Cơ cấu thanh toán theo tháng. Nhãn payment_type theo data dictionary TLC.
SELECT year, month, payment_type,
       CASE payment_type WHEN 0 THEN 'Flex Fare' WHEN 1 THEN 'Credit card' WHEN 2 THEN 'Cash' WHEN 3 THEN 'No charge'
                         WHEN 4 THEN 'Dispute' WHEN 5 THEN 'Unknown' WHEN 6 THEN 'Voided' ELSE 'Other' END AS payment_label,
       COUNT(*)                                                       AS trips,
       ROUND(SUM(total_amount), 2)                                    AS revenue,
       ROUND(AVG(fare_amount), 3)                                     AS avg_fare,
       ROUND(AVG(tip_amount), 3)                                      AS avg_tip,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY year, month), 3) AS pct_of_month_trips
FROM trips
GROUP BY year, month, payment_type
ORDER BY year, month, payment_type

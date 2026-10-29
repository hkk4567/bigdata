-- Nhu cầu theo giờ trong ngày x thứ trong tuần (pickup_dow: 1 = Chủ nhật ... 7 = Thứ bảy).
SELECT pickup_dow,
       CASE pickup_dow WHEN 1 THEN 'Sun' WHEN 2 THEN 'Mon' WHEN 3 THEN 'Tue' WHEN 4 THEN 'Wed'
                       WHEN 5 THEN 'Thu' WHEN 6 THEN 'Fri' ELSE 'Sat' END    AS dow_name,
       pickup_hour,
       COUNT(*)                                                              AS trips,
       ROUND(AVG(fare_amount), 3)                                            AS avg_fare,
       ROUND(AVG(trip_distance), 3)                                          AS avg_distance_mi,
       ROUND(AVG(avg_speed_mph), 2)                                          AS avg_speed_mph
FROM trips
GROUP BY pickup_dow, pickup_hour
ORDER BY pickup_dow, pickup_hour

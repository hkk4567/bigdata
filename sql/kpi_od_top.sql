-- 100 cặp (khu đón -> khu trả) đông nhất; bỏ zone Unknown 264/265 vì không định vị được.
SELECT t.pulocationid, zp.borough AS pu_borough, zp.zone AS pu_zone,
       t.dolocationid, zd.borough AS do_borough, zd.zone AS do_zone,
       t.trips, t.avg_fare, t.avg_distance_mi, t.avg_duration_min
FROM (
  SELECT pulocationid, dolocationid, COUNT(*) AS trips,
         ROUND(AVG(fare_amount), 3)            AS avg_fare,
         ROUND(AVG(trip_distance), 3)          AS avg_distance_mi,
         ROUND(AVG(trip_duration_s) / 60.0, 2) AS avg_duration_min
  FROM trips
  WHERE pulocationid NOT IN (264, 265) AND dolocationid NOT IN (264, 265)
  GROUP BY pulocationid, dolocationid
  ORDER BY trips DESC
  LIMIT 100
) t
LEFT JOIN zones zp ON t.pulocationid = zp.locationid
LEFT JOIN zones zd ON t.dolocationid = zd.locationid
ORDER BY t.trips DESC

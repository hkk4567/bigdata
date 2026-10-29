-- Khu vực đón khách (JOIN dim_zone). Zone 264/265 = Unknown trong dim_zone: GIỮ lại và gắn nhãn để tổng khớp.
WITH agg AS (
  SELECT pulocationid,
         COUNT(*)                          AS trips,
         ROUND(SUM(total_amount), 2)       AS revenue,
         ROUND(AVG(fare_amount), 3)        AS avg_fare,
         ROUND(AVG(trip_distance), 3)      AS avg_distance_mi
  FROM trips
  GROUP BY pulocationid
)
SELECT a.pulocationid,
       COALESCE(z.borough, 'Unknown')      AS borough,
       COALESCE(z.zone, 'Unknown')         AS zone,
       a.trips, a.revenue, a.avg_fare, a.avg_distance_mi,
       ROUND(a.trips * 100.0 / SUM(a.trips) OVER (), 3) AS pct_of_trips,
       RANK() OVER (ORDER BY a.trips DESC)               AS pickup_rank
FROM agg a LEFT JOIN zones z ON a.pulocationid = z.locationid
ORDER BY pickup_rank

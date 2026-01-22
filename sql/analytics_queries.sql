-- Top delayed routes
SELECT origin, destination, AVG(delay_minutes) AS avg_delay
FROM transport_delay
GROUP BY origin, destination
ORDER BY avg_delay DESC;

-- Delay by hour
SELECT EXTRACT(HOUR FROM scheduled_arrival) AS hour,
       AVG(delay_minutes) AS avg_delay
FROM transport_delay
GROUP BY hour
ORDER BY hour;
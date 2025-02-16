
INSERT INTO slaterc3.web_events_olap_cube 

WITH mapped AS (
SELECT 
CASE
  WHEN host LIKE '%techcreator%' THEN 'TechCreator.io'
  WHEN host LIKE '%eczachly%' THEN 'EcZachly.com'
  WHEN host LIKE '%zachwilson%' THEN 'ZachWilson.tech'
  WHEN host LIKE '%dataengineer%' THEN 'DataEngineer.io'
  ELSE host
END AS host,
COALESCE(BROWSER_TYPE, 'unknown') as browser_type, 
CASE WHEN DEVICE_TYPE LIKE '%amsung%' THEN 'Samsung'
ELSE COALESCE(device_type, 'unknown') 
END AS device_type, 
COALESCE(OS_TYPe, 'unknown') AS os_type,
url
FROM bootcamp.web_events w
JOIN bootcamp.devices d 
  ON d.device_id = w.device_id
WHERE w.event_time BETWEEN DATE('2023-01-01') AND DATE('2023-12-31')

-- GROUP BY 1, 2, 3, 4,5
),
agged AS (
SELECT 

  COALESCE(host, '(overall)') AS host, 
  COALESCE(browser_type, '(overall)') AS browser_type, 
  COALESCE(device_type, '(overall)') AS device_type, 
  COALESCE(os_type, '(overall)') AS os_type, 
  COUNT(1) as page_hits,
  DATE('2023-12-31') as ds,
  ARRAY_JOIN(FILTER(ARRAY [
CASE WHEN GROUPING(host) = 0 THEN 'host' END,
CASE WHEN GROUPING(browser_type) = 0 THEN 'browser_type' END,
CASE WHEN GROUPING(device_type) = 0 THEN 'device_type' END,
CASE WHEN GROUPING(os_type) = 0 THEN 'os_type' END
], x-> x IS NOT NULL), '__') as agg_level
FROM mapped
GROUP BY CUBE(
      host, browser_type, device_type, os_type
      )
    ORDER BY COUNT(1) DESC
)
SELECT * 
from agged
--COUNT(1) as combo_count FROM agged
 
 --select * from bootcamp.web_events limit 5

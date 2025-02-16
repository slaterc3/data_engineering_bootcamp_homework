WITH mapped AS (
SELECT 
CASE
  WHEN host LIKE '%techcreator%' THEN 'TechCreator.io'
  WHEN host LIKE '%eczachly%' THEN 'EcZachly.com'
  WHEN host LIKE '%zachwilson%' THEN 'ZachWilson.tech'
  WHEN host LIKE '%dataengineer%' THEN 'DataEngineer.io'
  ELSE host
END AS host,
BROWSER_TYPE, 
CASE WHEN DEVICE_TYPE LIKE '%amsung%' THEN 'Samsung'
ELSE COALESCE(device_type, 'unknown') 
END AS device_type, 
COALESCE(OS_TYPe, 'unknown') AS os_type,
url,
COUNT(1)
FROM bootcamp.web_events e
JOIN bootcamp.devices d 
  ON d.device_id = e.device_id
GROUP BY 1, 2, 3, 4,5
),
agged AS (
SELECT 
  COALESCE(host, '(overall)') AS host, 
  COALESCE(browser_type, '(overall)') AS browser_type, 
  COALESCE(device_type, '(overall)') AS device_type, 
  COALESCE(os_type, '(overall)') AS os_type, 
  COUNT(1) as page_hits
FROM mapped
GROUP BY ROLLUP(
      host, browser_type, device_type, os_type
      )
    ORDER BY COUNT(1) DESC
)
SELECT * 
from agged
--COUNT(1) as combo_count FROM agged
 

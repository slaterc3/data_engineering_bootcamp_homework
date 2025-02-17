
-- INSERT INTO slaterc3.web_events_olap_cube 

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
DATE(event_time) as event_date,
event_time,
user_id,
url
FROM bootcamp.web_events w
JOIN bootcamp.devices d 
  ON d.device_id = w.device_id
WHERE w.event_time BETWEEN DATE('2023-01-01') AND DATE('2023-12-31')

),
windowed AS (

SELECT 
user_id,
event_time,
  --FIRST_VALUE(event_time) OVER (PARTITION BY user_id, event_date ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as first_action_on_date,
  FIRST_VALUE(event_time) OVER (PARTITION BY user_id, event_date ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_action_on_date,
  --LAST_VALUE(event_time) OVER (PARTITION BY user_id, event_date ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_action_on_date,
  LAST_VALUE(event_time) OVER (PARTITION BY user_id, event_date ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_action_on_date,
  --COUNT(event_time) OVER (PARTITION BY user_id, event_date ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as total_event_today
  COUNT(event_time) OVER (PARTITION BY user_id, event_date ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as total_event_today
FROM mapped
--QUALIFY COUNT(event_time) OVER (PARTITION BY user_id, event_date ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) > 1
)
SELECT  *
FROM windowed
WHERE total_event_today > 1

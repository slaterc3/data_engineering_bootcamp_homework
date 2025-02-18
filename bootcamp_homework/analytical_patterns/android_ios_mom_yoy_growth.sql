-- SELECT * FROM bootcamp.devices 

-- SELECT ARRAY_JOIN(ARRAY['DEVICE_ID', 'BROWSER_TYPE', 'OS_TYPE', 'DEVICE_TYPE'], ',')

-- DEVICE_ID,BROWSER_TYPE,OS_TYPE,DEVICE_TYPE

--SELECT * FROM bootcamp.web_events
-- USER_ID,DEVICE_ID,REFERRER,HOST,URL,EVENT_TIME,

WITH monthly_os_type_usage AS (
SELECT 
  DATE_TRUNC('month', event_time) AS activity_month,
  CASE 
    WHEN d.os_type LIKE '%ndroid%' THEN 'Android'
    WHEN d.os_type LIKE '%ios%' OR d.os_type LIKE '%iOS' THEN 'iOS'
  -- including all other os types as 'other' 
  ELSE 'other'
  END AS os,
  COUNT(DISTINCT w.user_id) AS monthly_users_active
FROM bootcamp.web_events w
JOIN bootcamp.devices d 
  ON w.device_id = d.device_id
GROUP BY 1, 2
),
monthly_yearly_users AS (
SELECT 
  activity_month,
  os,
  monthly_users_active,
  -- lag by 1 to get prev month
  LAG(monthly_users_active, 1) OVER (PARTITION BY os ORDER BY activity_month) as prior_month_users,
  -- lag by 12 to get prev 12 mo's (prev year) 
  LAG(monthly_users_active, 12) OVER (PARTITION BY os ORDER BY activity_month) as prior_year_users
FROM monthly_os_type_usage
)
SELECT 
  YEAR(activity_month) as activity_year,
  MONTH(activity_month) as activity_month,
  os,
  monthly_users_active,
  prior_month_users,
  prior_year_users,
  CASE
    WHEN prior_month_users IS NULL OR prior_month_users = 0 THEN NULL
    ELSE 100.0 * (monthly_users_active - prior_month_users) / prior_month_users
  END AS mom_growth_percentage,
  CASE
    WHEN prior_year_users IS NULL OR prior_year_users = 0 THEN NULL
    ELSE 100.0 * (monthly_users_active - prior_year_users) / prior_year_users
  END AS yoy_growth_percentage
FROM monthly_yearly_users
ORDER BY activity_year DESC, activity_month DESC, os

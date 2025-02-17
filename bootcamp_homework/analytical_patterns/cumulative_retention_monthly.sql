
WITH user_first_activity AS (
SELECT 
  user_id,
  -- DATE_TRUNC('month', MIN(event_time)) AS first_active_month,
  MIN(DATE(event_time)) AS first_active_date
FROM bootcamp.web_events
GROUP BY user_id
),
after_activity AS (
SELECT 
  user_id,
  DATE(event_time) AS event_date
FROM bootcamp.web_events
)
SELECT 
-- this gives us the year and month of when users started
   YEAR(f.first_active_date) AS first_year, -- the group by column, this gives us more detailed cohorts of users by date
   MONTH(f.first_active_date) AS first_month,
   COUNT(DISTINCT f.user_id) AS total_started,
   COUNT(DISTINCT CASE WHEN two_weeks_joined.user_id IS NOT NULL THEN f.user_id END) as two_weeks_active, 
    COUNT(DISTINCT CASE WHEN one_month_joined.user_id IS NOT NULL THEN f.user_id END) as one_month_active,
    COUNT(DISTINCT CASE WHEN three_months_joined.user_id IS NOT NULL THEN f.user_id END) as three_months_active
FROM user_first_activity f -- `f` for `first`

LEFT JOIN (
  SELECT 
    aa.user_id
  FROM after_activity aa 
  JOIN user_first_activity ufa
    ON aa.user_id = ufa.user_id
  WHERE aa.event_date >= DATE_ADD('day', 14, ufa.first_active_date)
  GROUP BY aa.user_id
) two_weeks_joined
  ON f.user_id = two_weeks_joined.user_id
-- one month active
LEFT JOIN (
SELECT 
  aa.user_id
FROM after_activity aa 
JOIN user_first_activity ufa
  ON aa.user_id = ufa.user_id
WHERE aa.event_date >= DATE_ADD('month', 1, ufa.first_active_date)
GROUP BY aa.user_id
) one_month_joined
  ON f.user_id = one_month_joined.user_id
-- three months active
LEFT JOIN (
SELECT 
  aa.user_id
FROM after_activity aa 
JOIN user_first_activity ufa
  ON aa.user_id = ufa.user_id
WHERE aa.event_date >= DATE_ADD('month', 3, ufa.first_active_date)
GROUP BY aa.user_id
) three_months_joined
  ON f.user_id = three_months_joined.user_id
GROUP BY YEAR(f.first_active_date), MONTH(f.first_active_date)
-- more recent data first
ORDER BY YEAR(f.first_active_date) DESC, MONTH(f.first_active_date) DESC

-- note, values of zero on the top rows is due to lack of data, not lack of retention

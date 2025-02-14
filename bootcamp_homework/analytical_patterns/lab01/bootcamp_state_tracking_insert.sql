INSERT INTO slaterc3.bootcamp_state_tracking 
 WITH yesterday AS (
   SELECT * 
   FROM slaterc3.bootcamp_state_tracking
     WHERE ds = DATE('2022-12-31')
 ),
 today AS (
 SELECT * 
 FROM slaterc3.bootcamp_web_events_daily
   WHERE ds = DATE('2023-01-01')
 ), 
 combined AS (
   SELECT 
      COALESCE(t.user_id, y.user_id) AS user_id,
   COALESCE(y.first_active_date, t.ds) AS first_active_date,
   COALESCE(t.ds, y.last_active_date) AS last_active_date,
   CASE 
     WHEN y.user_id IS NULL THEN 'new'
     ELSE 'unknown'
   END AS current_state,
   COALESCE(lifetime_page_visits, 0) + COALESCE(t.total_page_visits, 0) AS lifetime_page_visits,
   DATE('2023-01-01') as ds 
   FROM today t 
   FULL OUTER JOIN yesterday y 
     ON t.user_id = y.user_id
 )
 SELECT * FROM combined

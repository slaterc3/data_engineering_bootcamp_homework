CREATE TABLE slaterc3.bootcamp_state_tracking (
 user_id BIGINT,
 first_active_date DATE,
 last_active_date DATE,
 current_state VARCHAR,
 lifetime_page_visits BIGINT,
 ds DATE
 )
 WITH (
 format = 'PARQUET',
 partitioning = ARRAY['ds']
 )

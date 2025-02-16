CREATE TABLE slaterc3.web_events_olap_cube (
  host VARCHAR,
  browser_type VARCHAR,
  device_type VARCHAR,
  os_type VARCHAR,
  page_hits BIGINT,
  ds DATE,
  agg_level VARCHAR
) WITH (
  format='PARQUET',
  partitioning= ARRAY['ds', 'agg_level']
)

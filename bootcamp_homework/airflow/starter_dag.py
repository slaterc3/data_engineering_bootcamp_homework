from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from include.eczachly.trino_queries import execute_trino_query, run_trino_query_dq_check
from airflow.models import Variable

import requests 
from datetime import datetime as dt, timedelta as td, timezone

tabular_credential = Variable.get("TABULAR_CREDENTIAL")
polygon_api_key = Variable.get('POLYGON_CREDENTIALS', deserialize_json=True)['AWS_SECRET_ACCESS_KEY']

# TODO make sure to rename this if you're testing this dag out!
schema = 'slaterc3'

@dag(
    description="A dag for your homework, it takes polygon data in and cumulates it",
    default_args={
        "owner": schema,
        "start_date": datetime(2025, 1, 9),
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2025, 1, 9),
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=True,
    tags=["community"],
)
def starter_dag_slaterc3():
    maang_stocks = ['AAPL', 'AMZN', 'NFLX', 'GOOGL', 'META']
    production_table = f'{schema}.user_web_events_cumulated'
    staging_table = production_table + '_stg_{{ ds_nodash }}'
    cumulative_table = f'{schema}.your_table_name'
    yesterday_ds = '{{ yesterday_ds }}'
    ds = '{{ ds }}'

    # todo figure out how to load data from polygon into Iceberg
    def load_data_from_polygon(table):
        """helper function to load data from polygon into iceberg table"""
        load_date = (dt.now(timezone.utc) - td(days=1)).strftime('%Y-%m-%d')
        rows = []
        for ticker in maang_stocks:
            url = (
                f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
                f"{load_date}/{load_date}?adjusted=true&sort=asc&apiKey={polygon_api_key}"
            )
            response = requests.get(url, timeout=10)
            data = response.json()
            if data.get("resultsCount", 0) == 0:
                continue
            for item in data.get("results", []):
                rows.append({
                    "stock_symbol": ticker,
                    "created_ts": item["t"],
                    "opening_price": item["o"],
                    "high_price": item["h"],
                    "low_price": item["l"],
                    "closing_price": item["c"],
                    "trade_volume": item["v"],
                    "trade_count": item["n"],
                    "ds": load_date
                })
                # rows.append(row)
        if rows:
            values = ", ".join([
                f"('{r['stock_symbol']}', FROM_UNIXTIME({int(r['created_ts']/1000)}), {r['opening_price']}, "
                f"{r['high_price']}, {r['low_price']}, {r['closing_price']}, {r['trade_volume']}, "
                f"{r['trade_count']}, DATE '{r['ds']}')"
                for r in rows
            ])
            insert_query = f"INSERT INTO {table} VALUES {values}"
            execute_trino_query(insert_query)
        return len(rows)
    
    
    # TODO create schema for daily stock price summary table
    create_daily_step = PythonOperator(
        task_id="create_daily_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
            CREATE TABLE IF NOT EXISTS {production_table} (
                stock_symbol VARCHAR,
                created_ts TIMESTAMP,
                opening_price FLOAT,
                high_price FLOAT,
                low_price FLOAT,
                closing_price FLOAT,
                trade_volume BIGINT,
                trade_count BIGINT,
                ds DATE
            )
            WITH (
                format='iceberg',
                partitioning=ARRAY['ds']
            )
            """
        }
    )

    # TODO create the schema for your staging table
    create_staging_step = PythonOperator(
        task_id="create_staging_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
            CREATE TABLE IF NOT EXISTS {staging_table} (
                stock_symbol VARCHAR,
                created_ts TIMESTAMP,
                opening_price FLOAT,
                high_price FLOAT,
                low_price FLOAT,
                closing_price FLOAT,
                trade_volume BIGINT,
                trade_count BIGINT,
                ds DATE
            )
            WITH (
                format='iceberg',
                partitioning=ARRAY['ds']
            )
            """
        }
    )


    # todo make sure you load into the staging table not production
    load_to_staging_step = PythonOperator(
        task_id="load_to_staging_step",
        python_callable=load_data_from_polygon,
        op_kwargs={
            'table': staging_table
        }
    )

    # TODO figure out some nice data quality checks
    run_dq_check = PythonOperator(
        task_id="run_dq_check",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""
            SELECT 
                CASE WHEN COUNT(*) = 0 THEN 'fail'
                    ELSE 'pass'
                END AS dq_check
            FROM {staging_table}
            WHERE ds = DATE('{ds}')
            """
        }
    )


    # todo make sure you clear out production to make things idempotent
    clear_step = PythonOperator(
        task_id="clear_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
            DELETE FROM {production_table}
            WHERE ds = DATE('{ds}')
            """
        }
    )

    exchange_data_from_staging = PythonOperator(
        task_id="exchange_data_from_staging",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': """
                          INSERT INTO {production_table}
                          SELECT * FROM {staging_table} 
                          WHERE ds = DATE('{ds}')
                      """.format(production_table=production_table,
                                 staging_table=staging_table,
                                 ds='{{ ds }}')
        }
    )

    # TODO do not forget to clean up
    drop_staging_table = PythonOperator(
        task_id="drop_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
            DROP TABLE IF EXISTS {staging_table}
            """
        }
    )

    # TODO create the schema for your cumulative table
    create_cumulative_step = PythonOperator(
        task_id="create_cumulative_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
            CREATE TABLE IF NOT EXISTS {cumulative_table} (
                stock_symbol VARCHAR,
                last_7_prices ARRAY(FLOAT),
                last_update TIMESTAMP
            )
            WITH (
                format='iceberg'
            )
            """
        }
    )

    clear_cumulative_step = PythonOperator(
        task_id="clear_cumulative_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
            DELETE FROM {cumulative_table}
            WHERE DATE(last_update) = DATE('{ds}')
            """
        }
    )

    # TODO make sure you create array metrics for the last 7 days of stock prices
    cumulate_step = PythonOperator(
        task_id="cumulate_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': """
            INSERT INTO {cumulative_table}
            WITH new_data AS (
                SELECT 
                    stock_symbol,
                    closing_price,
                    created_ts
                FROM {production_table}
                WHERE ds <= DATE('{ds}')
            ),
            combined AS (
                SELECT 
                    COALESCE(n.stock_symbol, c.stock_symbol) AS stock_symbol,
                    n.closing_price AS new_closing,   
                    n.created_ts AS new_ts,
                    c.last_7_prices AS old_prices,
                    c.last_update AS old_update
                FROM new_data n
                FULL OUTER JOIN {cumulative_table} c
                    ON n.stock_symbol = c.stock_symbol
            ),
            updated AS (
                SELECT 
                    stock_symbol,
                    CASE 
                        WHEN old_prices IS NULL THEN ARRAY[new_closing]
                        WHEN new_closing IS NULL THEN old_prices
                        ELSE array_concat(old_prices, ARRAY[new_closing])
                    END AS combined_prices,
                    GREATEST(
                        COALESCE(new_ts, TIMESTAMP '1970-01-01 00:00:00'),
                        COALESCE(old_update, TIMESTAMP '1970-01-01 00:00:00')
                    ) AS last_update
            )
            SELECT 
                stock_symbol,
                array_slice(
                    combined_prices,
                    greatest(cardinality(combined_prices) -6, 1),
                    7
                ) as last_7_prices,
                last_update
            FROM updated
            """
        }
    )

    # TODO figure out the right dependency chain
    create_daily_step >> create_staging_step >> load_to_staging_step >> run_dq_check >> clear_step >> exchange_data_from_staging  
    # branching points
    exchange_data_from_staging  >> drop_staging_table
    exchange_data_from_staging >> create_cumulative_step >> clear_cumulative_step >> cumulate_step

starter_dag_slaterc3()

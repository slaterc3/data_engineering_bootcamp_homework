import logging
from ast import literal_eval
from datetime import datetime, timezone, date

import pyarrow as pa
import requests
from aws_secret_manager import get_secret
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import TableAlreadyExistsError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import FloatType, LongType, NestedField, StringType, TimestampType

logger = logging.getLogger(__name__)
load_dotenv()


def read_data_from_polygon(stock_tickers: list[str]):
    from datetime import timedelta

    #grabbing yesterday's data
    # today's was being DELAYED
    yesterday = (date.today() - timedelta(days=1)).isoformat()  # Use yesterday's date
    polygon_url = f"https://api.polygon.io/v2/aggs/ticker/{{ticker}}/range/1/day/{yesterday}/{yesterday}?adjusted=true&sort=asc&apiKey="
    polygon_api_key = literal_eval(get_secret("POLYGON_CREDENTIALS"))[
        "AWS_SECRET_ACCESS_KEY"
    ]
    # `rows` will store final structured data for tickers
    rows = []
    for ticker in stock_tickers:
        response = requests.get(
            polygon_url.format(ticker=ticker) + polygon_api_key, timeout=10
        )
        data = response.json()

        # Debugging the response
        print(f"Response for {ticker}: {data}")

        # Handle delayed status or missing data
        if data.get("status") == "DELAYED" or data.get("resultsCount", 0) == 0:
            logger.warning(f"No data available for {ticker}. Status: {data.get('status')}")
            continue

        results = data.get("results", [])
        for item in results:
            event_time = datetime.fromtimestamp(item["t"] / 1000.0, timezone.utc)
            rows.append(
                {
                    "stock_symbol": ticker,
                    "created_ts": event_time,
                    "opening_price": item["o"],
                    "high_price": item["h"],
                    "low_price": item["l"],
                    "closing_price": item["c"],
                    "trade_volume": item["v"],
                    "trade_count": item["n"],
                }
            )
    return rows


def create_table_snapshot(table, stock_schema):
    null_row = {
        "stock_symbol": None,
        "created_ts": datetime.now(timezone.utc),
        "opening_price": None,
        "high_price": None,
        "low_price": None,
        "closing_price": None,
        "trade_volume": None,
        "trade_count": None,
    }
    arrow_table = pa.Table.from_pylist([null_row], schema=stock_schema.as_arrow())
    table.overwrite(arrow_table)


def create_or_get_table(catalog, table_identifier, stock_schema, timestamp_col_name):
    try:
        # Define partition spec
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=stock_schema.find_field(timestamp_col_name).field_id,
                field_id=1000,
                transform=DayTransform(),
                name="created_at_day",
            )
        )
        # Attempt to create the table
        table = catalog.create_table(
            identifier=table_identifier,
            schema=stock_schema,
            partition_spec=partition_spec,
        )
        logger.info(f"Created new Iceberg table {table_identifier}")

    except TableAlreadyExistsError:
        # If the table already exists, load it
        logger.info(f"Table {table_identifier} already exists. Loading the table...")
        table = catalog.load_table(table_identifier)

    return table


def homework_script():
    maang_stocks = ["AAPL", "AMZN", "NFLX", "GOOGL", "META"]
    schema_name = "slaterc3"
    table_identifier = f"{schema_name}.stock_prices_table"
    timestamp_col_name = "created_ts"

    catalog = load_catalog(
        "academy",
        type="rest",
        uri="https://api.tabular.io/ws",
        warehouse=get_secret("CATALOG_NAME"),
        credential=get_secret("TABULAR_CREDENTIAL"),
    )

    stock_schema = Schema(
        NestedField(field_id=1, name="stock_symbol", field_type=StringType(), required=True),
        NestedField(field_id=2, name=timestamp_col_name, field_type=TimestampType(), required=True),
        NestedField(field_id=3, name="opening_price", field_type=FloatType(), required=False),
        NestedField(field_id=4, name="high_price", field_type=FloatType(), required=False),
        NestedField(field_id=5, name="low_price", field_type=FloatType(), required=False),
        NestedField(field_id=6, name="closing_price", field_type=FloatType(), required=False),
        NestedField(field_id=7, name="trade_volume", field_type=LongType(), required=False),
        NestedField(field_id=8, name="trade_count", field_type=LongType(), required=False),
    )

    table = create_or_get_table(
        catalog, table_identifier, stock_schema, timestamp_col_name
    )

    if not table.snapshots():
        create_table_snapshot(table, stock_schema)

    try:
    # Try to create the branch
    
        branch_name = "audit_branch"
        current_snapshot_id = table.snapshots()[-1].snapshot_id
        with table.manage_snapshots() as ms:
            ms.create_branch(snapshot_id=current_snapshot_id, branch_name=branch_name)
        logger.info(f"Created branch: {branch_name}")
    except Exception as e:
        logger.warning(f"Branch {branch_name} might already exist or could not be created: {e}")


    rows = read_data_from_polygon(maang_stocks)
    arrow_table = pa.Table.from_pylist(rows, schema=stock_schema.as_arrow())
    table.overwrite(arrow_table)


if __name__ == "__main__":
    homework_script()
    # maang_stocks = ["AAPL", "AMZN", "NFLX", "GOOGL", "META"]
    # print(read_data_from_polygon(maang_stocks))

from dagster import Definitions, load_assets_from_modules

from .assets import bronze, silver, gold, warehouse
from .resources import snowflakeResource, bigQueryResource


bronze_assets = load_assets_from_modules([bronze])
silver_assets = load_assets_from_modules([silver])
gold_assets = load_assets_from_modules([gold])
warehouse_assets = load_assets_from_modules([warehouse])


defs = Definitions(
    assets=[*bronze_assets, *silver_assets, *gold_assets, *warehouse_assets],
    resources={
        "snowflake": snowflakeResource,
        "bigquery": bigQueryResource,
    }
)

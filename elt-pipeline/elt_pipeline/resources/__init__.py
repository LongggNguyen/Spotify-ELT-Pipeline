from dagster import EnvVar
from dagster_gcp import BigQueryResource
from dagster_snowflake import SnowflakeResource


bigQueryResource = BigQueryResource(
    project=EnvVar("PROJECT_GCP"),
    location=EnvVar("LOCATION_GCP"),
    gcp_credentials=EnvVar("GCP_CREDS"),
    # timeout=EnvVar("TIMEOUT_GCP"),
)

snowflakeResource = SnowflakeResource(
    account=EnvVar("ACCOUNT_SNOWFLAKE"),
    user=EnvVar("USER_SNOWFLAKE"),
    password=EnvVar("PASSWORD_SNOWFLAKE"),
    database=EnvVar("DATABASE_SNOWFLAKE"),
    schema=EnvVar("SCHEMA_SNOWFLAKE"),
    role=EnvVar("ROLE_SNOWFLAKE"),
    warehouse=EnvVar("WAREHOUSE_SNOWFLAKE")
)
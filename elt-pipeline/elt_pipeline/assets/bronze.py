import polars as pl

from dagster import asset, AssetExecutionContext
from dagster_snowflake import SnowflakeResource
from dagster_gcp import BigQueryResource
from dagster import Output, MetadataValue, AssetIn

from . import contants


COMPUTE_KIND = ["SQL", "polars"]
LAYER = "bronze"

@asset(
    compute_kind=COMPUTE_KIND[0],
    group_name=LAYER
)
def spotify_albums(context: AssetExecutionContext, 
                   bigquery: BigQueryResource) -> Output[pl.DataFrame]:
    """ Extract table spotify_albums from BigQuery and load to MinIO """
    
    sql_query = f"SELECT * FROM {contants.DATASET_NAME}.spotify_albums;"
    
    with bigquery.get_client() as client:
        df_data: pl.DataFrame = client.query(sql_query).to_dataframe()
        df_data = pl.DataFrame(df_data)
        context.log.info(f"Extracted table spotify_albums with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "Dataset name": MetadataValue.text(contants.DATASET_NAME),
            "Table name": MetadataValue.text("spotify_albums"),
            "Number of records": MetadataValue.int(df_data.shape[0]),
            "Number of columns": MetadataValue.int(df_data.shape[1])
        }
    )


@asset(
    compute_kind=COMPUTE_KIND[0],
    group_name=LAYER
)
def spotify_artists(context: AssetExecutionContext, 
                   bigquery: BigQueryResource) -> Output[pl.DataFrame]:
    """ Extract table spotify_artists from BigQuery and load to MinIO """
    
    sql_query = f"SELECT * FROM {contants.DATASET_NAME}.spotify_artists;"
    
    with bigquery.get_client() as client:
        df_data: pl.DataFrame = client.query(sql_query).to_dataframe()
        df_data = pl.DataFrame(df_data)
        context.log.info(f"Extracted table spotify_artists with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "Dataset name": MetadataValue.text(contants.DATASET_NAME),
            "Table name": MetadataValue.text("spotify_artists"),
            "Number of records": MetadataValue.int(df_data.shape[0]),
            "Number of columns": MetadataValue.int(df_data.shape[1])
        }
    )
    

@asset(
    compute_kind=COMPUTE_KIND[0],
    group_name=LAYER
)
def spotify_tracks(context: AssetExecutionContext,
                   snowflake: SnowflakeResource) -> Output[pl.DataFrame]:
    """ Extract table spotify_tracks from BigQuery and load to MinIO """
    
    sql_query = f"SELECT * FROM {contants.SCHEMA_NAME}.spotify_tracks;"
    
    with snowflake.get_connection() as connect:
        df_data = connect.cursor().execute(sql_query).fetch_pandas_all()
        df_data = pl.DataFrame(df_data)
        context.log.info(f"Extracted table spotify_tracks with shape: {df_data.shape}")
    
    return Output(
        value=df_data,
        metadata={
            "Schema name": MetadataValue.text(contants.SCHEMA_NAME),
            "Table name": MetadataValue.text("spotify_tracks"),
            "Number of records": MetadataValue.int(df_data.shape[0]),
            "Number of columns": MetadataValue.int(df_data.shape[1])
        }
    )
    

@asset(
    ins={
        "spotify_tracks": AssetIn(key="spotify_tracks")
    },
    compute_kind=COMPUTE_KIND[1],
    group_name=LAYER
)
def spotify_track(context: AssetExecutionContext,
                  spotify_tracks: pl.DataFrame) -> Output[pl.DataFrame]:
    """ Get Table spotify_track from table spotify_tracks in SnowFlake and load to MinIO """
    
    df_data = spotify_tracks[contants.SPOTIFY_TRACK_COLUMNS]
    context.log.info(f"Get Dataframe spotify_track success with: {df_data.shape}")
    
    return Output(
        value=df_data,
        metadata={
            "Schema name": MetadataValue.text(contants.SCHEMA_NAME),
            "Table name": MetadataValue.text("spotify_track"),
            "Number of records": MetadataValue.int(df_data.shape[0]),
            "Number of columns": MetadataValue.int(df_data.shape[1])
        }
    )
    

@asset(
    ins={
        "spotify_tracks": AssetIn(key="spotify_tracks")
    },
    compute_kind=COMPUTE_KIND[1],
    group_name=LAYER
)
def spotify_track_metrics(context: AssetExecutionContext,
                    spotify_tracks: pl.DataFrame) -> Output[pl.DataFrame]:
    """ Get Table spotify_track_metrics from table spotify_tracks in SnowFlake and load to MinIO """
    
    df_data = spotify_tracks[contants.SPOTIFY_TRACK_METRICS_COLUMNS]
    context.log.info(f"Get Dataframe spotify_track_metrics success with: {df_data.shape}")
    
    return Output(
        value=df_data,
        metadata={
            "Dataset name": MetadataValue.text(contants.SCHEMA_NAME),
            "Table name": MetadataValue.text("spotify_track_metrics"),
            "Number of records": MetadataValue.int(df_data.shape[0]),
            "Number of columns": MetadataValue.int(df_data.shape[1])
        }
    )
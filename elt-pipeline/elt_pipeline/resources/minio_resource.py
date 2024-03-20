import polars as pl
from minio import Minio

from dagster import ConfigurableResource



class MinIOResource(ConfigurableResource):
    
    def connect_minio(self):
        pass
    
    
    def make_bucket(self):
        pass
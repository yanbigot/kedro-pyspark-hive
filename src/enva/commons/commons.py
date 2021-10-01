import datetime
from itertools import chain

from pyspark.sql import (
    DataFrame,
    SparkSession,
    functions as f)


def to_source_view(df: DataFrame) -> DataFrame:
    columns = df.columns
    source_view_df = df.withColumn(
        "data_map",
        f.create_map(list(chain(*(
            (f.lit(name), f.col(name)) for name in df.columns
        ))))
    ).withColumn(
        "execution_id",
        f.lit(str(datetime.datetime.now()))
    ).drop(*columns)

    source_view_df.printSchema

    return source_view_df


def create_source_view_if_not_exists():
    spark = SparkSession.builder.getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS dhr")
    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS dhr.source_views(data_map map<string, string>)
        PARTITIONED BY (execution_id string) 
        STORED AS PARQUET 
        LOCATION 'source_views'
    """)


def insert_into_source_view(df: DataFrame):
    df.show
    df.select("data_map", "execution_id")\
        .write.format("parquet")\
        .insertInto(tableName="dhr.source_views")

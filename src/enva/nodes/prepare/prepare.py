from pyspark.sql import (
    DataFrame,
    SparkSession)
from enva.commons.commons import (
    insert_into_source_view,
    to_source_view,
    create_source_view_if_not_exists
)


def create_dummy_df() -> DataFrame:
    columns = ["a", "b"]
    data = [("a", "a"), ("b", "b")]
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(data).toDF(*columns)
    df.show(truncate=False)
    return df


def ensure_prerequisites() -> bool:
    create_source_view_if_not_exists()
    return True


def write_to_source_view(df: DataFrame):
    df.show(truncate=False)
    source_views_df = to_source_view(df)
    source_views_df.show(truncate=False)
    insert_into_source_view(df=source_views_df)


def control_insertion() -> bool :
    spark = SparkSession.builder.getOrCreate()
    dhr_tables = spark.catalog.listTables("dhr")
    for t in dhr_tables:
        print(t.name)
    spark.sql("SELECT * FROM dhr.source_views").show(truncate=False)
    return True

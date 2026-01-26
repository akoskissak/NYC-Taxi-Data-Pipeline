from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="transportation.silver.taxi_zone",
    comment="Cleaned and standardized products dimension with business transformations",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def taxi_zone_silver():
    df_bronze = spark.read.table("transportation.bronze.taxi_zone")
    df_silver = df_bronze.select(
        F.col("LocationID"),
        F.col("Borough").alias("borough_name"),
        F.col("Zone").alias("zone"),
        F.col("service_zone"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp")
    )

    df_silver = df_silver.withColumn(
        "silver_processed_timestamp", F.current_timestamp()
    )

    return df_silver
from pyspark import pipelines as dp
import pyspark.sql.functions as F

SOURCE_PATH = "s3://nyctaxi-akos/data-store/trips"

@dp.table(
    name="transportation.bronze.trips",
    comment="Streaming ingestion of raw orderd data with Auto Loader",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "parquet",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    }
)
def trips_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 5)
        .load(SOURCE_PATH)
    )

    df = df.withColumnRenamed("trip_distance", "distance_travelled_mi")

    df = df.withColumn("file_name", F.col("_metadata.file_path")).withColumn("ingest_datetime", F.current_timestamp())

    return df


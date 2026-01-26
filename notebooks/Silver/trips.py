from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import md5, concat_ws

@dp.view(
    name="trips_silver_staging",
    comment="Transformed trips data ready for CDC upsert"
)
@dp.expect_or_drop("valid_date", "year(pickup_ts) >= 2025")
def trips_silver():
    df_bronze = spark.readStream.table("transportation.bronze.trips")

    # creating unique ID
    df_silver = df_bronze.withColumn("trip_id", md5(concat_ws("||",
                                    F.col("VendorID"),
                                    F.col("tpep_pickup_datetime"),
                                    F.col("PULocationID"),
                                    F.col("DOLocationID"),
                                    F.col("total_amount"))))
    
    # metric conversion
    df_silver = df_silver.withColumn("distance_travelled_km", F.round(F.col("distance_travelled_mi") * 1.60934, 2))

    # duration enrichment
    df_silver = df_silver.withColumn("duration_minutes", F.round((F.col("tpep_dropoff_datetime") - F.col("tpep_pickup_datetime")).cast("long") / 60))

    df_silver = df_silver.withColumn("pickup_hour", F.hour("tpep_pickup_datetime")).withColumn("pickup_day", F.date_format("tpep_pickup_datetime", "EEEE"))

    df_silver = df_silver.filter(
        (F.col("distance_travelled_mi") > 0) &
        (F.col("total_amount") > 0) &
        (F.col("duration_minutes") > 0) &
        (F.col("duration_minutes") < 240) # less than 4 hours
    )

    # clean up nulls
    df_silver = df_silver.fillna({"passenger_count": 1, "RatecodeID": 1})

    df_silver = df_silver.select(
        F.col("trip_id").alias("id"),
        F.col("VendorID"),
        F.col("tpep_pickup_datetime").cast("date").alias("business_date"),
        F.col("tpep_pickup_datetime").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").alias("dropoff_ts"),
        F.col("pickup_hour"),
        F.col("pickup_day"),
        F.col("duration_minutes"),
        F.col("PULocationID").alias("start_location_id"),
        F.col("DOLocationID").alias("end_location_id"),
        F.col("passenger_count"),
        F.col("distance_travelled_mi"),
        F.col("distance_travelled_km"),
        F.col("RateCodeID"),
        F.col("total_amount").alias("sales_amount"),
        F.col("tip_amount"),
        F.col("tolls_amount"),
        F.col("payment_type"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp")
    )
    
    df_silver = df_silver.withColumn("silver_processed_timestamp", F.current_timestamp())

    return df_silver

dp.create_streaming_table(
    name="transportation.silver.trips",
    comment="Cleaned and validated trips with CDC upsert capability",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.feature.timestampNtz": "supported", # because timestampNtz is not supported by default
        "delta.autoOptimize.autoCompact": "true"
    }
)

dp.create_auto_cdc_flow(
    target="transportation.silver.trips",
    source="trips_silver_staging",
    keys=["id"],
    sequence_by=F.col("silver_processed_timestamp"),
    stored_as_scd_type=1,
    except_column_list=[],
)
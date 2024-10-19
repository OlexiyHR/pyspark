from pyspark.sql import SparkSession
import pyspark.sql.types as t


def read_trip_data_df(
        spark_session: SparkSession,
        dataframe_path: str
):
    trip_data_schema = t.StructType([
        t.StructField("medallion", t.StringType(), nullable=False),
        t.StructField("hack_license", t.StringType(), nullable=False),
        t.StructField("vendor_id", t.StringType(), nullable=False),
        t.StructField("rate_code", t.IntegerType(), nullable=False),
        t.StructField("store_and_fwd_flag", t.StringType(), nullable=False),
        t.StructField("pickup_datetime", t.TimestampType(), nullable=False),
        t.StructField("dropoff_datetime", t.TimestampType(), nullable=False),
        t.StructField("passenger_count", t.IntegerType(), nullable=False),
        t.StructField("trip_time_in_secs", t.IntegerType(), nullable=False),
        t.StructField("trip_distance", t.DoubleType(), nullable=False),
        t.StructField("pickup_longitude", t.DoubleType(), nullable=False),
        t.StructField("pickup_latitude", t.DoubleType(), nullable=False),
        t.StructField("dropoff_longitude", t.DoubleType(), nullable=False),
        t.StructField("dropoff_latitude", t.DoubleType(), nullable=False)
    ])

    return (spark_session
            .read
            .option("header", True)
            .option("sep", ",")
            .option("mode", "FAILFAST")
            .option("multiLine", False)
            .schema(trip_data_schema)
            .csv(dataframe_path))

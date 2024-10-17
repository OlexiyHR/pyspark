from pyspark.sql import SparkSession

def read_trip_data_df(
        spark_session: SparkSession,
        dataframe_path: str
):
    return spark_session.read.csv(dataframe_path, header=True, inferSchema=True)

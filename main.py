from pyspark import SparkConf
from pyspark.sql import SparkSession

from basic_dfs import basic_df_Krasovskyy as basic_df_k
from basic_dfs.basic_df_mykytyshyn import basic_test_df as basic_test_df_myk
from basic_dfs.basic_df_Hromiak import basic_test_df as basic_test_df_Hromiak
from read_write import read_fare_data_df, write_fare_data_df_to_csv, read_trip_data_df, write_trip_data_df_to_csv
import fare_data_postprocessing as fare_proc
import settings as s
import columns as c


def create_spark_session():
    """
    Creates and returns Spark session

    Returns: SparkSession object
    """
    spark_conf = SparkConf()
    spark = (SparkSession.builder
             .master("local")
             .appName("pyspark project")
             .config(conf=spark_conf)
             .getOrCreate())
    return spark


def display_demo_dataframe_mykytyshyn():
    df = basic_test_df_myk(spark_session)
    df.show()


def display_demo_dataframe_krasovskyy():
    basic_df_k.basic_test_df(spark_session=spark_session).show()


def display_demo_dataframe_Hromiak():
    df = basic_test_df_Hromiak(spark_session)
    df.show()


if __name__ == "__main__":
    spark_session = create_spark_session()

    display_demo_dataframe_krasovskyy()
    display_demo_dataframe_mykytyshyn()
    display_demo_dataframe_Hromiak()

    fare_data_df = read_fare_data_df(
        spark_session=spark_session,
        dataframe_path=s.TRIP_FARE_READ_DIRECTORY_PATH,
        header=True,
        sep=",",
        null_value="NULL",
        mode="FAILFAST",
        multi_line=True
    )

    fare_data_df = fare_proc.replace_unknown_values_with_null(
        df=fare_data_df,
        column_name=c.payment_type,
        unknown_value="UNK"
    )

    fare_num_columns = [c.fare_amount, c.surcharge, c.mta_tax, c.tip_amount, c.tolls_amount, c.total_amount]
    for column in fare_num_columns:
        fare_data_df = fare_proc.filter_negative_values(df=fare_data_df, column=column)

    fare_data_df = fare_proc.filter_zero_fare_rows(fare_data_df)

    fare_data_df = fare_proc.filter_invalid_mta_tax(fare_data_df)

    write_fare_data_df_to_csv(
        df=fare_data_df,
        write_folder_path=s.TRIP_FARE_WRITE_DIRECTORY_PATH,
        num_files=s.WRITE_PARTITION,
        header=True,
        sep=","
    )

    trip_data_df = read_trip_data_df(
        spark_session=spark_session,
        dataframe_path=s.TRIP_DATA_READ_DIRECTORY_PATH,
        header=True,
        sep=",",
        null_value="NULL",
        mode="FAILFAST",
        multi_line=True
    )

    write_trip_data_df_to_csv(
        df=trip_data_df,
        write_folder_path=s.TRIP_DATA_WRITE_DIRECTORY_PATH,
        num_files=s.WRITE_PARTITION,
        header=True,
        sep=","
    )

    spark_session.stop()

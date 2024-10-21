from pyspark import SparkConf
from pyspark.sql import SparkSession

from basic_dfs import basic_df_Krasovskyy as basic_df_k
from basic_dfs.basic_df_mykytyshyn import basic_test_df as basic_test_df_myk
from basic_dfs.basic_df_Hromiak import basic_test_df as basic_test_df_Hromiak
from fare_data_postprocessing import replace_unknown_values_with_null
from trip_data_postprocessing import transform_store_and_fwd_flag_to_bool, remove_invalid_rows
from read_write import read_fare_data_df, write_fare_data_df_to_csv, read_trip_data_df, write_trip_data_df_to_csv
from settings import TRIP_FARE_READ_DIRECTORY_PATH, TRIP_FARE_WRITE_DIRECTORY_PATH, TRIP_DATA_READ_DIRECTORY_PATH, TRIP_DATA_WRITE_DIRECTORY_PATH, WRITE_PARTITION
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


def display_demo_dataframe_krasovskyy():
    basic_df_k.basic_test_df(spark_session=spark_session).show()


def display_demo_dataframe_Hromiak():
    df = basic_test_df_Hromiak(spark_session)



if __name__ == "__main__":
    spark_session = create_spark_session()

    display_demo_dataframe_krasovskyy()
    display_demo_dataframe_mykytyshyn()
    display_demo_dataframe_Hromiak()

    fare_data_df = read_fare_data_df(
        spark_session=spark_session,
        dataframe_path=TRIP_FARE_READ_DIRECTORY_PATH,
        header=True,
        sep=",",
        null_value="NULL",
        mode="FAILFAST",
        multi_line=True
    )

    fare_data_df = replace_unknown_values_with_null(
        df=fare_data_df,
        column_name=c.payment_type,
        unknown_value="UNK"
    )

    write_fare_data_df_to_csv(
        df=fare_data_df,
        write_folder_path=TRIP_FARE_WRITE_DIRECTORY_PATH,
        num_files=WRITE_PARTITION,
        header=True,
        sep=","
    )


    trip_data_df = read_trip_data_df(
        spark_session=spark_session,
        dataframe_path=TRIP_DATA_READ_DIRECTORY_PATH,
        header=False,
        sep=",",
        null_value="NULL",
        mode="FAILFAST",
        multi_line=True
    )

    trip_data_df = transform_store_and_fwd_flag_to_bool(trip_data_df)
    trip_data_df = remove_invalid_rows(trip_data_df)


    write_trip_data_df_to_csv(
        df=trip_data_df,
        write_folder_path=TRIP_DATA_WRITE_DIRECTORY_PATH,
        num_files=WRITE_PARTITION,
        header=True,
        sep=","
    )


    spark_session.stop()

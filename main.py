import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

from basic_dfs import basic_df_Krasovskyy as basic_df_k
from basic_dfs.basic_df_mykytyshyn import basic_test_df as basic_test_df_myk
from basic_dfs.basic_df_Hromiak import basic_test_df as basic_test_df_Hromiak
from read_write import read_fare_data_df, write_fare_data_df_to_csv, read_trip_data_df, write_trip_data_df_to_csv
from data_postprocessing import fare_data_postprocessing as fare_proc, trip_data_postprocessing as trip_proc
from data_cleaning.remove_duplicates import remove_duplicates
from data_cleaning.clean_trip_data import fill_null_trip_data
import data_analysis.fare_data_analysis as fda
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


def display_demo_dataframe_mykytyshyn(spark_session):
    df = basic_test_df_myk(spark_session)
    df.show()


def display_demo_dataframe_krasovskyy(spark_session):
    basic_df_k.basic_test_df(spark_session=spark_session).show()


def display_demo_dataframe_Hromiak(spark_session):
    df = basic_test_df_Hromiak(spark_session)
    df.show()


def main():
    spark_session = create_spark_session()

    display_demo_dataframe_krasovskyy(spark_session)
    display_demo_dataframe_mykytyshyn(spark_session)
    display_demo_dataframe_Hromiak(spark_session)

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

    fare_data_df = remove_duplicates(fare_data_df)

    fare_num_columns = [c.fare_amount, c.surcharge, c.mta_tax, c.tip_amount, c.tolls_amount, c.total_amount]
    for column in fare_num_columns:
        fare_data_df = fare_proc.filter_negative_values(df=fare_data_df, column=column)

    fare_data_df = fare_proc.filter_zero_fare_rows(fare_data_df)

    fare_data_df = fare_proc.filter_invalid_mta_tax(fare_data_df)

    total_fare_num_columns = [c.fare_amount, c.total_amount]
    for column in total_fare_num_columns:
        fare_data_df = fare_proc.remove_outliers_iqr_in_col(df=fare_data_df, column=column, multiplier=10)

    write_fare_data_df_to_csv(
        df=fare_data_df,
        write_folder_path=s.TRIP_FARE_WRITE_DIRECTORY_PATH,
        num_files=s.WRITE_PARTITION,
        header=True,
        sep=","
    )

    os.makedirs(os.path.dirname(s.QUESTION21_WRITE_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(s.QUESTION22_WRITE_PATH), exist_ok=True)
    os.makedirs(s.QUESTION23_WRITE_PATH, exist_ok=True)

    evening_rides_with_high_total_amount_count = fda.count_evening_rides_with_high_total_amount(fare_data_df)
    with open(s.QUESTION21_WRITE_PATH, "w") as f:
        f.write(str(evening_rides_with_high_total_amount_count))

    cash_tips_above_average_count = fda.count_cash_tips_above_average(fare_data_df)
    with open(s.QUESTION22_WRITE_PATH, "w") as f:
        f.write(str(cash_tips_above_average_count))

    weekday_credit_card_trips_with_high_tips = fda.filter_weekday_credit_card_trips_with_high_tips(fare_data_df)
    write_fare_data_df_to_csv(
        df=weekday_credit_card_trips_with_high_tips,
        write_folder_path=s.QUESTION23_WRITE_PATH,
        num_files=s.WRITE_PARTITION,
        header=True,
        sep=","
    )

    trip_data_df = read_trip_data_df(
        spark_session=spark_session,
        dataframe_path=s.TRIP_DATA_READ_DIRECTORY_PATH,
        header=False,
        sep=",",
        null_value="NULL",
        mode="FAILFAST",
        multi_line=True
    )

    trip_data_df = remove_duplicates(trip_data_df)

    trip_data_df = fill_null_trip_data(trip_data_df)

    trip_data_df = trip_proc.remove_invalid_rows(trip_data_df)

    trip_data_df = trip_proc.transform_store_and_fwd_flag_to_bool(trip_data_df)

    write_trip_data_df_to_csv(
        df=trip_data_df,
        write_folder_path=s.TRIP_DATA_WRITE_DIRECTORY_PATH,
        num_files=s.WRITE_PARTITION,
        header=True,
        sep=","
    )

    spark_session.stop()


if __name__ == "__main__":
    main()

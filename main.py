from pyspark import SparkConf
from pyspark.sql import SparkSession

from basic_dfs import basic_df_Krasovskyy as basic_df_k
from basic_dfs.basic_df_mykytyshyn import basic_test_df as basic_test_df_myk
from basic_dfs.basic_df_Hromiak import basic_test_df as basic_test_df_Hromiak
from read_write import (read_fare_data_df,
                        write_fare_data_df_to_csv,
                        read_trip_data_df,
                        write_trip_data_df_to_csv,
                        setup_directories, write_question_results)
from data_postprocessing import fare_data_postprocessing as fare_proc, trip_data_postprocessing as trip_proc
from data_cleaning.remove_duplicates import remove_duplicates
from data_cleaning.clean_trip_data import fill_null_trip_data
import settings as s
import columns as c
from data_analysis import fare_data_analysis as fda, trip_data_analysis as tda


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

    setup_directories()

    # Artem
    passenger_count_distribution = tda.passenger_count_distribution(trip_data_df)
    write_question_results(passenger_count_distribution, 14)

    vendor_trip_counts_distribution = tda.trip_amounts_distribution_by_vendor(trip_data_df)
    write_question_results(vendor_trip_counts_distribution, 19)

    average_trip_speeds_by_month = tda.average_trip_speed_by_month(trip_data_df)
    write_question_results(average_trip_speeds_by_month, 20)

    short_trips_count = tda.count_short_trips(trip_data_df)
    write_question_results(short_trips_count, 24)

    large_group_trips_count = tda.count_large_group_trips(trip_data_df)
    write_question_results(large_group_trips_count, 25)

    average_trip_price_paid_with_card = fda.average_card_payment_total(fare_data_df)
    write_question_results(average_trip_price_paid_with_card, 26)

    top_10_drivers_by_distance_per_month = tda.top_10_drivers_by_distance_per_month(trip_data_df)
    write_question_results(top_10_drivers_by_distance_per_month, 34)


    # Andrii
    evening_rides_with_high_total_amount_count = fda.count_evening_rides_with_high_total_amount(fare_data_df)
    write_question_results(evening_rides_with_high_total_amount_count, 21)

    cash_tips_above_average_count = fda.count_cash_tips_above_average(fare_data_df)
    write_question_results(cash_tips_above_average_count, 22)

    weekday_credit_card_trips_with_high_tips = fda.filter_weekday_credit_card_trips_with_high_tips(fare_data_df)
    write_question_results(weekday_credit_card_trips_with_high_tips, 23)

    top_10_by_trips_df, top_10_by_trip_income_df, top_10_by_tips_df = fda.top_10_successful_drivers(fare_data_df)
    write_question_results(top_10_by_trips_df, 1, part=1)
    write_question_results(top_10_by_trip_income_df, 1, part=2)
    write_question_results(top_10_by_tips_df, 1, part=3)

    month_profit_df, day_of_week_profit_df = fda.most_profitable_months_and_days(fare_data_df)
    write_question_results(month_profit_df, 2, part=1)
    write_question_results(day_of_week_profit_df, 2, part=2)

    mta_taxes_df = fda.monthly_mta_tax_by_vendor(fare_data_df)
    write_question_results(mta_taxes_df, 12)

    top_10_drivers_df = tda.top_10_drivers_by_between_ride_distance(trip_data_df)
    write_question_results(top_10_drivers_df, 30)

    ranked_trip_counts_df = tda.get_driver_peak_load_days_in_december(trip_data_df)
    write_question_results(ranked_trip_counts_df, 33)

    # Oleksii
    expensive_trips = fda.count_expensive_trips(fare_data_df)
    write_question_results(expensive_trips, 27)

    medium_duration_trips = tda.count_medium_duration_trips(trip_data_df)
    write_question_results(medium_duration_trips, 28)

    jfk_airport_trips_with_four_passengers = tda.jfk_airport_trips_with_four_passengers(trip_data_df)
    write_question_results(jfk_airport_trips_with_four_passengers, 29)

    average_tip_by_payment_type = fda.average_tip_by_payment_type(fare_data_df)
    write_question_results(average_tip_by_payment_type, 8)

    vendor_with_highest_fare = fda.vendor_with_highest_fare(fare_data_df)
    write_question_results(vendor_with_highest_fare, 11)

    passenger_count_by_time_of_day = tda.passenger_count_by_time_of_day(trip_data_df)
    write_question_results(passenger_count_by_time_of_day, 16)

    cumulative_total_fare_on_july_4 = fda.cumulative_total_fare_on_july_4(fare_data_df)
    write_question_results(cumulative_total_fare_on_july_4, 31)

    top_5_drivers_by_trip_count_on_july_4 = fda.top_5_drivers_by_trip_count_on_july_4(fare_data_df)
    write_question_results(top_5_drivers_by_trip_count_on_july_4, 32)

    spark_session.stop()


if __name__ == "__main__":
    main()

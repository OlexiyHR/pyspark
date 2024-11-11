from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window

import columns as c



def column_tip_correlation(trip_data_df: DataFrame, trip_fare_df: DataFrame, column_name: str) -> float:
    """
    Calculates the correlation between trip distance and tipping amount for NYC taxi data.

    Args:
        trip_data_df (DataFrame): Spark DataFrame containing trip data.
        trip_fare_df (DataFrame): Spark DataFrame containing fare data.

    Returns:
        float: Correlation value between trip distance and tipping amount.

    Examples:
        >>> distance_tip_correlation = column_tip_correlation(trip_data_df, trip_fare_df, column_name=c.trip_distance)
    """
    joined_df = trip_data_df.join(
        trip_fare_df,
        on=[c.medallion, c.hack_license, c.pickup_datetime],
        how="inner"
    )

    return joined_df.stat.corr(column_name, c.tip_amount)


def count_trips_missing_fare(trip_data_df: DataFrame, trip_fare_df: DataFrame) -> int:
    """
    Counts the number of trips with missing fare information.

    Args:
        trip_data_df (DataFrame): Spark DataFrame containing trip data.
        trip_fare_df (DataFrame): Spark DataFrame containing fare data.

    Returns:
        int: Number of trips with missing fare information.

    Examples:
        >>> missing_fare_count = count_trips_missing_fare(trip_data_df, trip_fare_df)
    """
    joined_df = trip_data_df.join(
        trip_fare_df,
        on=[c.medallion, c.hack_license, c.pickup_datetime],
        how="left"
    )

    return joined_df.filter(joined_df[c.total_amount].isNull()).count()


def passenger_count_vs_trip_price(trip_data: DataFrame, fare_data: DataFrame) -> DataFrame:
    """
    Calculates the average trip price for each number of passengers.

    Args:
        trip_data (DataFrame): Spark DataFrame containing trip data.
        fare_data (DataFrame): Spark DataFrame containing fare data.

    Returns:
        DataFrame: A Spark DataFrame with columns for the passenger count and the average trip price.
                   The DataFrame is ordered by passenger count.

    Examples:
        >>> avg_trip_price_df = passenger_count_vs_trip_price(trip_data, fare_data)
    """
    joined_df = trip_data.join(
        fare_data,
        on=["medallion", "hack_license", "pickup_datetime"],
        how="inner"
    )

    result_df = joined_df.groupBy("passenger_count").agg(
        f.avg("total_amount").alias("average_trip_price")
    ).orderBy("passenger_count")

    return result_df


def most_popular_rate_code_by_payment_type(trip_data:DataFrame, fare_data: DataFrame) -> DataFrame:
    """
    Finds the most popular rate_code for each payment_type.

    Args:
        trip_data (DataFrame): Spark DataFrame containing trip data.
        fare_data (DataFrame): Spark DataFrame containing fare data.

    Returns:
        DataFrame: A Spark DataFrame with columns `payment_type` and `most_popular_rate_code`,
                   representing each payment type and its most frequently occurring rate code.

    Examples:
        >>> most_popular_rate_code_by_payment_type_df = most_popular_rate_code_by_payment_type(trip_data, fare_data)
    """
    joined_df = trip_data.join(
        fare_data,
        on=["medallion", "hack_license", "pickup_datetime"],
        how="left"
    )

    rate_code_counts = (joined_df.groupBy("payment_type", "rate_code")
                                 .agg(f.count("*")
                                 .alias("usage_count")))

    window_spec = Window.partitionBy("payment_type").orderBy(f.desc("usage_count"))

    ranked_rate_codes = rate_code_counts.withColumn(
        "rank", f.row_number().over(window_spec)
    )

    most_popular_rate_codes = ranked_rate_codes.filter(f.col("rank") == 1).select(
        "payment_type", "rate_code", "usage_count"
    )

    return most_popular_rate_codes

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


def get_most_profitable_rate_codes(trip_data_df: DataFrame, fare_data_df: DataFrame) -> DataFrame:
    """
    Get most profitable rate codes by calculating total revenue for each code.

    Notes:
        Question 10.

    Args:
        trip_data_df (DataFrame): Trip data DataFrame for processing.
        fare_data_df (DataFrame): Fare data DataFrame for processing.

    Returns:
        DataFrame: DataFrame containing rate codes with the highest total revenue ordered by total revenue
                   in descending order.

    Examples:
        >>> most_profitable_rate_codes_df = get_most_profitable_rate_codes(trip_data_df, fare_data_df)
    """
    total_revenue_col = "total_revenue"

    join_condition = (
            (trip_data_df[c.medallion] == fare_data_df[c.medallion])
            & (trip_data_df[c.hack_license] == fare_data_df[c.hack_license])
            & (trip_data_df[c.pickup_datetime] == fare_data_df[c.pickup_datetime])
    )

    trip_fare_data = trip_data_df.join(
        fare_data_df,
        on=join_condition,
        how='inner'
    )

    profit_from_rate_codes = (
        trip_fare_data
        .groupBy(trip_fare_data[c.rate_code])
        .agg(f.sum(fare_data_df[c.total_amount]).alias(total_revenue_col))
        .orderBy(f.desc(total_revenue_col))
    )

    return profit_from_rate_codes


def get_rate_codes_with_tolls_percentage(trip_data_df: DataFrame, fare_data_df: DataFrame) -> DataFrame:
    """
    Get the rate codes for trips and calculate the percentage of trips that had tolls (tolls_amount > 0).

    Notes:
        Question 36.

    Args:
        trip_data_df (DataFrame): Trip data DataFrame for processing.
        fare_data_df (DataFrame): Fare data DataFrame for processing.

    Returns:
        DataFrame: DataFrame containing rate codes and the percentage of trips with tolls,
                   ordered by the percentage of tolls in descending order.

    Examples:
        >>> rate_codes_with_tolls_percentage_df = get_rate_codes_with_tolls_percentage(trip_data_df, fare_data_df)
    """
    total_trips_col = "total_trips"
    tolls_count_col = "tolls_count"
    tolls_percent_col = "tolls_percent"

    join_condition = (
            (trip_data_df[c.medallion] == fare_data_df[c.medallion])
            & (trip_data_df[c.hack_license] == fare_data_df[c.hack_license])
            & (trip_data_df[c.pickup_datetime] == fare_data_df[c.pickup_datetime])
    )

    trip_fare_data = trip_data_df.join(
        fare_data_df,
        on=join_condition,
        how='left'
    )

    rate_codes_with_counts = (
        trip_fare_data
        .groupBy(trip_fare_data[c.rate_code])
        .agg(
            f.count("*").alias(total_trips_col),
            f.count(f.when(f.col(c.tolls_amount) > 0, 1)).alias(tolls_count_col)
        )
    )

    rate_codes_with_counts = (
        rate_codes_with_counts
        .withColumn(
            tolls_percent_col,
            f.when(f.col(total_trips_col) == 0, 0)
            .otherwise(f.col(tolls_count_col) / f.col(total_trips_col) * 100)
        )
        .orderBy(f.desc(tolls_percent_col))
    )

    return rate_codes_with_counts

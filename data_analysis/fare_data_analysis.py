"""
Module for functions that implement analysis of trip fare data.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window
import columns as c


def count_evening_rides_with_high_total_amount(df):
    """
    Count trips that took place in the evening (between 18:00 and 23:59) and
    had a total_amount > mean(total_amount).

    Notes:
        Question 21.

    Args:
        df (DataFrame): Fare data DataFrame to process.

    Returns:
        int: Count of evening trips with a total_amount above mean value .

    Example:
        >>> evening_rides_with_high_total_amount_count = count_evening_rides_with_high_total_amount(df)
    """
    total_amount_mean = df.select(f.mean(c.total_amount)).first()[0]

    evening_trips_with_high_total_amount_count = df.where(
        (f.hour(f.col(c.pickup_datetime)).between(18, 23))
        & (f.col(c.total_amount) > total_amount_mean)
    ).count()

    return evening_trips_with_high_total_amount_count


def count_cash_tips_above_average(df):
    """
    Count cash-paid trips that have tip amount above mean value of tip amount.

    Notes:
        Question 22.

    Args:
        df (DataFrame): Fare data DataFrame to process.

    Returns:
        int: Count of cash-paid trips with `tip_amount` above the average.

    Examples:
        >>> cash_tips_above_average_count = count_cash_tips_above_average(df)
    """
    tip_amount_mean = df.select(f.avg(f.col(c.tip_amount))).first()[0]

    cash_tip_count = df.where(
        (f.col(c.payment_type) == "CSH")
        & (f.col(c.tip_amount) > tip_amount_mean)
    ).count()

    return cash_tip_count


def filter_weekday_credit_card_trips_with_high_tips(df):
    """
    Select credit_card-paid trips on weekdays (from Monday to Friday) with tip amount that is bigger
    than fare amount.

    Notes:
        Question 23.

    Args:
        df (DataFrame): Fare data DataFrame to process.

    Returns:
        DataFrame: Filtered DataFrame with credit_card-paid weekdays trips with tip amount that is bigger than fare amount
        (payment_type column is dropped because it becomes redundant
    Examples:
        >>> weekday_credit_card_trips_with_high_tips = filter_weekday_credit_card_trips_with_high_tips(df)
    """
    weekday_trips_with_high_tips = df.where(
        (f.col(c.payment_type) == "CRD")
        & (f.col(c.tip_amount) > f.col(c.fare_amount))
        & (f.dayofweek(f.col(c.pickup_datetime)).between(2, 6))
    )

    weekday_trips_with_high_tips = weekday_trips_with_high_tips.drop(c.payment_type)

    return weekday_trips_with_high_tips


def average_card_payment_total(df) -> float:
    """
    Calculates the average total amount of trips paid for with a card.

    Args:
        df (DataFrame): Spark DataFrame containing trip data.

    Returns:
        DataFrame: A new DataFrame containing the average total amount of card-paid trips.

    Examples:
        >>> average_total_amount_by_card = average_card_payment_total(taxi_df)
    """
    card_payment_type_code = 'CRD'
    return (df
            .filter(f.col(c.payment_type) == card_payment_type_code)
            .select(f.mean(c.total_amount).alias("avg_total_amount"))
            .fillna(0.0, subset=["avg_total_amount"])
            .first()[0])


def count_expensive_trips(df: DataFrame) -> int:
    """
    Filters trips with a total fare greater than 50 dollars and returns the count of these trips.

    Args:
        df (DataFrame): Spark DataFrame containing trip fare data.

    Returns:
        int: The number of trips with a total fare greater than 50 dollars.

    Examples:
        >>> expensive_trips_count = count_expensive_trips(fare_data_df)
    """
    return df.where(f.col(c.total_amount) >= 50).count()


def average_tip_by_payment_type(df: DataFrame) -> DataFrame:
    """
    Groups data by payment type and calculates the average tip amount.

    Args:
        df (DataFrame): Spark DataFrame containing fare data.

    Returns:
        DataFrame: A new DataFrame with each payment type and the average tip amount.

    Examples:
        >>> avg_tips_by_payment = average_tip_by_payment_type(fare_data_df)
    """
    return df.groupBy(c.payment_type).agg(f.avg(c.tip_amount).alias("average_tip"))


def vendor_with_highest_fare(df: DataFrame) -> DataFrame:
    """
    Groups data by vendor ID and calculates the total fare amount for each vendor,
    returning the vendor with the highest total fare.

    Args:
        df (DataFrame): Spark DataFrame containing fare data.

    Returns:
            DataFrame: A new DataFrame with vendor IDs and their total fare amounts, sorted in descending order.

    Examples:
        >>> top_vendor = vendor_with_highest_fare(fare_data_df)
    """
    return df.groupBy(c.vendor_id).agg(f.sum(c.fare_amount).alias("total_fare")).orderBy("total_fare", ascending=False)


def cumulative_total_fare_on_july_4(df: DataFrame) -> DataFrame:
    """
    Calculates cumulative total fare for each driver on July 4.

    Args:
        df (DataFrame): Spark DataFrame containing fare data.

    Returns:
        DataFrame: A new DataFrame with each driver's cumulative total fare on July 4.

    Examples:
        >>> cumulative_fares = cumulative_total_fare_on_july_4(fare_data_df)
    """
    july_4_data = df.filter(
        (f.col(c.pickup_datetime) >= "2023-07-04 00:00:00") &
        (f.col(c.pickup_datetime) <= "2023-07-04 23:59:59")
    )

    window_spec = Window.partitionBy(c.hack_license).orderBy(c.pickup_datetime)

    return july_4_data.withColumn("cumulative_fare", f.sum(c.total_amount).over(window_spec))


def top_5_drivers_by_trip_count_on_july_4(df: DataFrame) -> DataFrame:
    """
        Finds the top 5 drivers by trip count on July 4, with additional sorting by total fare amount.

        Args:
            df (DataFrame): Spark DataFrame containing trip data.

        Returns:
             DataFrame: A new DataFrame with the top 5 drivers by trip count and total fare on July 4.

        Examples:
            >>> top_drivers = top_5_drivers_by_trip_count_on_july_4(fare_data_df)
    """
    july_4_data = df.filter(
        (f.col(c.pickup_datetime) >= "2023-07-04 00:00:00") &
        (f.col(c.pickup_datetime) <= "2023-07-04 23:59:59")
    )

    aggregated_data = july_4_data.groupBy(c.hack_license).agg(
        f.count("*").alias("trip_count"),
        f.sum(c.total_amount).alias("total_fare")
    )

    window_spec = Window.orderBy(f.col("trip_count").desc(), f.col("total_fare").desc())

    ranked_data = aggregated_data.withColumn("rank", f.row_number().over(window_spec))

    return ranked_data.filter(f.col("rank") <= 5)

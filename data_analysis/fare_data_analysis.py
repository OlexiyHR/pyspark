"""
Module for functions that implement analysis of trip fare data.
"""


from pyspark.sql import DataFrame
from pyspark.sql import functions as f
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
            .agg(f.avg(c.total_amount))
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


def top_10_successful_drivers(df):
    """
    Identify the 10 most successful taxi drivers by his:
    1. Total number of trips.
    2. Total cost of trips.
    3. Total tips received.

    Args:
        df (DataFrame): Fare data DataFrame to process.

    Returns:
        DataFrame: DataFrame of top 10 drivers based on total trips, trip cost, and tips.
    """
    drivers_summary_df = (
        df.groupBy("medallion", "hack_license")
        .agg(
            f.count("*").alias("total_trips"),
            f.sum("total_amount").alias("total_trip_cost"),
            f.sum("tip_amount").alias("total_tips")
        )
    )

    top_10_by_trips = drivers_summary_df.orderBy(f.desc("total_trips")).select("medallion", "hack_license", "total_trips").limit(10)
    top_10_by_trip_income = drivers_summary_df.orderBy(f.desc("total_trip_cost")).select("medallion", "hack_license", "total_trip_cost").limit(10)
    top_10_by_tips = drivers_summary_df.orderBy(f.desc("total_tips")).select("medallion", "hack_license", "total_tips").limit(10)

    return top_10_by_trips, top_10_by_trip_income, top_10_by_tips



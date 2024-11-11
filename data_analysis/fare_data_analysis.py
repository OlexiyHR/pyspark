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
    count_expensive = df.where(f.col(c.total_amount) >= 50).count()
    return count_expensive


def top_10_successful_drivers(df):
    """
    Identify the 10 most successful taxi drivers by their:
    1. Total number of trips.
    2. Total income of trips.
    3. Total tips received.

    Notes:
        Question 1.

    Args:
        df (DataFrame): Fare data DataFrame to process.

    Returns:
        Tuple[DataFrame, DataFrame, DataFrame]: DataFrames of top 10 drivers based on total trips, trip income, and tips.

    Examples:
        >>> top_10_by_trips_df, top_10_by_trip_income_df, top_10_by_tips_df = top_10_successful_drivers(df)
    """
    total_trips_name = "total_trips"
    total_trip_cost_name = "total_trip_cost"
    total_tips_name = "total_tips"

    drivers_summary_df = (
        df
        .groupBy(c.medallion, c.hack_license)
        .agg(
            f.count("*").alias(total_trips_name),
            f.sum(c.total_amount).alias(total_trip_cost_name),
            f.sum(c.tip_amount).alias(total_tips_name)
        )
    )

    top_10_by_trips = (
        drivers_summary_df
        .orderBy(f.desc(total_trips_name))
        .select(c.medallion, c.hack_license, total_trips_name)
        .limit(10)
    )

    top_10_by_trip_income = (
        drivers_summary_df
        .orderBy(f.desc(total_trip_cost_name))
        .select(c.medallion, c.hack_license, total_trip_cost_name)
        .limit(10)
    )

    top_10_by_tips = (
        drivers_summary_df
        .orderBy(f.desc(total_tips_name))
        .select(c.medallion, c.hack_license, total_tips_name)
        .limit(10)
    )

    return top_10_by_trips, top_10_by_trip_income, top_10_by_tips


def most_profitable_months_and_days(df, month_names_df, day_names_df):
    """
    Calculate the most profitable months and days of the week by total trip income.

    Notes:
        Question 2.

    Args:
        df (DataFrame): Fare data DataFrame to process.
        month_names_df (DataFrame): DataFrame for month names.
        day_names_df (DataFrame): DataFrame for day names.

    Returns:
        Tuple[DataFrame, DataFrame]: DataFrames with trip costs for every month and day of the week.

    Examples:
        >>> month_profit_df, day_of_week_profit_df = most_profitable_months_and_days(df, month_names_df, day_names_df)
    """
    month_col = "month"
    day_of_week_col = "day_of_week"
    month_name_col = "month_name"
    day_name_col = "day_name"
    total_trip_cost_col = "total_trip_cost"

    df = (
        df
        .withColumn(month_col, f.month(c.pickup_datetime))
        .withColumn(day_of_week_col, f.dayofweek(c.pickup_datetime))
    )

    month_join_condition = (df[month_col] == month_names_df[month_col])
    day_join_condition = (df[day_of_week_col] == day_names_df[day_of_week_col])

    df = (
        df
        .join(month_names_df, on=month_join_condition, how="left")
        .join(day_names_df, on=day_join_condition, how="left")
    )

    month_profit = (
        df
        .groupBy(month_name_col)
        .agg(f.sum(c.total_amount).alias(total_trip_cost_col))
        .orderBy(f.desc(total_trip_cost_col))
    )

    day_of_week_profit = (
        df
        .groupBy(day_name_col)
        .agg(f.sum(c.total_amount).alias(total_trip_cost_col))
        .orderBy(f.desc(total_trip_cost_col))
    )

    return month_profit, day_of_week_profit


def monthly_mta_tax_by_vendor(df):
    """
    Get mta tax paid by each vendor monthly.

    Notes:
        Question 12.

    Args:
        df (DataFrame): Fare data DataFrame to process.

    Returns:
        DataFrame: DataFrames with mta_tax total amount from each vendor monthly.

    Examples:
        >>> mta_taxes_df = monthly_mta_tax_by_vendor(df)
    """
    month_name_col = "month_name"
    total_mta_tax = "total_mta_tax"

    df_with_month = df.withColumn(month_name_col, f.date_format(f.col("pickup_datetime"), "MMMM"))

    mta_taxes = (df_with_month
                 .groupBy(c.vendor_id, month_name_col)
                 .agg(f.sum(c.mta_tax).alias(total_mta_tax))
                 .orderBy(f.desc(total_mta_tax))
                )

    return mta_taxes


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
    result_df = df.groupBy(c.payment_type).agg(f.avg(c.tip_amount).alias("average_tip"))
    return result_df


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
    result_df = (df.groupBy(c.vendor_id)
                   .agg(f.sum(c.fare_amount).alias("total_fare"))
                   .orderBy("total_fare", ascending=False))
    return result_df


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

    result_df = july_4_data.withColumn("cumulative_fare", f.sum(c.total_amount).over(window_spec))
    return result_df


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

    result_df = ranked_data.filter(f.col("rank") <= 5)
    return result_df

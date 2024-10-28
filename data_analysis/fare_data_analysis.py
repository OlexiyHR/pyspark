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

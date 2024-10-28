from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg

import columns as c


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
            .filter(col(c.payment_type) == card_payment_type_code)
            .agg(avg(c.total_amount))
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
    return df.filter(col(c.total_amount) >= 50).count()

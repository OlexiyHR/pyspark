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

from pyspark.sql import DataFrame

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

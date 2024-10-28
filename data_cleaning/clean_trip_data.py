"""
Module for preprocessing trip data after reading it in PySpark DataFrame.

This module contains DataFrame preprocessing function for:
1. Filling null values in the `store_and_fwd_flag` column with 'Y':
     - null becomes 'Y'
"""

from pyspark.sql import functions as f
import columns as c


def fill_null_trip_data(df, column_name=c.store_and_fwd_flag):
    """
    This function fills null values in the `store_and_fwd_flag` column with 'Y'

    Args:
        df (DataFrame): Spark DataFrame for preprocessing.
        column_name (str, optional): The name of the column to be filled. Defaults to 'store_and_fwd_flag'.

    Returns:
        DataFrame: New Spark DataFrame after filling nulls in `store_and_fwd_flag` column with 'Y'

    Examples:
        >>> trip_data_df = fill_null_trip_data(trip_data_df)
    """
    df = df.withColumn(
        column_name,
        f.when(f.col(column_name).isNull(), 'Y').otherwise(f.col(column_name))
    )

    return df

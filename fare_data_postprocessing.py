"""
Module for postprocessing trip fare data after reading it in PySpark DataFrame.

This module contains DataFrame postprocessing functions for transforming unknown values to nulls,
deleting invalid MTA tax, fare_amount and total_amount values, deleting negative numbers and removing outliers.
"""


from pyspark.sql import functions as f
import columns as c

def replace_unknown_values_with_null(df, column_name, unknown_value="UNK"):
    """
    Transform specified value to null in specified column of trip fare DataFrame.

    Args:
        df (DataFrame): Spark DataFrame for transformation.
        column_name (str): The name of the column where the value should be replaced.
        unknown_value (str, optional): Value to be recognized as null. Defaults to 'UNK'.

    Returns:
        DataFrame: New Spark DataFrame where passed value was transformed to null in given column.

    Examples:
        >>> fare_data_df = replace_unknown_values_with_null(
        ...     df=fare_data_df,
        ...     column_name="payment_type",
        ...     unknown_value= "UNK"
        ... )
    """
    return df.withColumn(
        column_name,
        f.when(f.col(column_name) == unknown_value, None).otherwise(f.col(column_name))
    )


def filter_negative_values(df, column):
    """
    Remove rows with negative values in a specified column.

    Args:
        df (DataFrame): DataFrame to filter.
        column (str): Column name for filtering.

    Returns:
        DataFrame: New DataFrame without negative values in the specified column.

    Examples:
        >>> fare_data_df = filter_negative_values(
        ...     df=fare_data_df,
        ...     column="fare_amount"
        ... )
    """
    return df.filter(f.col(column) >= 0)

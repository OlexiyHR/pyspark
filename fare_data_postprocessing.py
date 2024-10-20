"""
Module for postprocessing trip fare data after reading it in PySpark DataFrame.

This module contains DataFrame postprocessing functions for transforming unknown values to nulls
and dropping columns with excessive number of empty values.
"""


from pyspark.sql import functions as f


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

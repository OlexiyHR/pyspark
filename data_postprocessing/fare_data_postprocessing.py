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
        ...     column_name=c.payment_type,
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


def filter_zero_fare_rows(df):
    """
    Drop rows where fare_amount or total_amount column value is zero.

    Notes:
        This is done because we cannot have 0 fare charge for ride, so such records
        can be considered as incorrect. We have 3637 zeros in fare_amount and 3015 zeros in total_amount,
        so dropping these rows won't affect data too much.

    Args:
        df (DataFrame): DataFrame to filter.

    Returns:
        DataFrame: New DataFrame where rows with zero fare_amount or total_amount are dropped.

    Examples:
        >>> fare_data_df = filter_zero_fare_rows(fare_data_df)
    """
    return df.filter((f.col(c.fare_amount) != 0.0) | (f.col(c.total_amount) != 0.0))


def filter_invalid_mta_tax(df):
    """
    Remove DataFrame rows where MTA tax is not equal to 0.0 or 0.5.

    Notes:
        MTA tax is foxed to 0.5 if paid, so values that are not 0.0 and 0.5 can be
        considered as errors and deleted. This will not affect data too much because
        there are only 358 incorrect values.

    Args:
        df (DataFrame): DataFrame to filter.

    Returns:
        DataFrame: New DataFrame where MTA tax is 0.0 or 0.5.

    Examples:
        >>> fare_data_df = filter_invalid_mta_tax(fare_data_df)
    """
    return df.filter((f.col(c.mta_tax) == 0.0) | (f.col(c.mta_tax) == 0.5))


def remove_outliers_iqr_in_col(df, column, multiplier=2.22):
    """
    Remove rows with outliers based on a specified column of fare data DataFrame using the IQR method.

    Args:
        df (DataFrame): DataFrame to clean.
        column (str): Name of the column to filter.
        multiplier (float, optional): Multiplier for the IQR to calculate the outlier bounds. Defaults to 2.22.

    Returns:
        DataFrame: New cleaned DataFrame without outliers.

    Examples:
        >>> fare_data_df = remove_outliers_iqr_in_col(
        ...     df=fare_data_df,
        ...     column="fare_amount",
        ...     multiplier=2.22
        ... )
    """
    if df.isEmpty():
        return df

    q1 = df.approxQuantile(column, [0.25], 0.01)[0]
    q3 = df.approxQuantile(column, [0.75], 0.01)[0]
    iqr = q3 - q1

    lower_bound = q1 - multiplier * iqr
    upper_bound = q3 + multiplier * iqr

    df = df.filter((f.col(column) >= lower_bound) & (f.col(column) <= upper_bound))

    return df

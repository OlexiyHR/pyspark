"""
Module for postprocessing trip data after reading it in PySpark DataFrame.

This module contains DataFrame postprocessing functions for:
1. Transforming the `store_and_fwd_flag` column values from 'Y/N' to boolean:
   - 'Y' -> True
   - 'N' -> False

2. Removing rows where the `dropoff_longitude` column contains null values:
   - Rows with null `dropoff_longitude` have `passenger_count`, `trip_time_in_secs`, and `trip_distance` are set to 0,
     meaning the trip data is not useful.
     Such rows are removed to ensure the DataFrame contains only valid trip records.
"""

from pyspark.sql import functions as f
import columns as c


def transform_store_and_fwd_flag_to_bool(df, column_name=c.store_and_fwd_flag):
    """
    Transform 'Y/N' values to boolean in the specified column of trip data DataFrame.

    - 'Y' -> True
    - 'N' -> False

    Args:
        df (DataFrame): Spark DataFrame for transformation.
        column_name (str, optional): The name of the column to be transformed. Defaults to 'store_and_fwd_flag'.

    Returns:
        DataFrame: New Spark DataFrame where 'Y/N' values were transformed to boolean in the specified column.

    Examples:
        >>> trip_data_df = transform_store_and_fwd_flag_to_bool(
        ...     df=trip_data_df,
        ...     column_name=c.store_and_fwd_flag
        ... )
    """
    result_df = df.withColumn(column_name,
                              f.when(f.col(column_name) == "Y", True)
                               .when(f.col(column_name) == "N", False))
    return result_df


def remove_invalid_rows(df, column_name=c.dropoff_longitude):
    """
    Remove rows where the specified column 'dropoff_longitude' has null values.

    Rows with null values in the 'dropoff_longitude' column are invalid, as in these rows
    'passenger_count', 'trip_time_in_secs', and 'trip_distance' are zero,
    So, the data from such rows does not provide useful information about trips and can be removed for cleaner analysis.

    Args:
        df (DataFrame): Spark DataFrame from which invalid rows will be removed.
        column_name (str, optional): The name of the column to check for null values. Defaults to 'dropoff_longitude'.

    Returns:
        DataFrame: New Spark DataFrame without invalid rows.

    Examples:
        >>> trip_data_df = remove_invalid_rows(
        ...     df=trip_data_df,
        ...     column_name=c.dropoff_longitude
        ... )
    """
    result_df = df.filter(f.col(column_name).isNotNull())
    return result_df

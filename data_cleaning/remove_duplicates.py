import columns as c

from pyspark.sql import DataFrame


def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Remove duplicate rows based on key columns in the DataFrame.

    Args:
        df (DataFrame): Spark DataFrame from which duplicates should be removed.

    Returns:
        DataFrame: New Spark DataFrame with duplicates removed, retaining only unique rows
        based on 'medallion' and 'pickup_datetime' columns.

    Examples:
        >>> trip_data_df = remove_duplicates(trip_data_df)
    """
    minimal_key_columns = [c.medallion, c.pickup_datetime]
    return df.dropDuplicates(minimal_key_columns)

from pyspark.sql import functions as f


def replace_unknown_values_with_null(df, column_name, unknown_value="UNK"):
    """
    Transform specified value to null in specified column of trip fare DataFrame.

    Args:
        df (DataFrame): Spark DataFrame for transformation.
        column_name (str): The name of the column where the value should be replaced.
        unknown_value (str, optional): Value to be recognized as null. Defaults to 'UNK'.

    Returns:
        DataFrame: New DataFrame where passed value was transformed to null in given column.

    Example:
        df = replace_unknown_values_with_null(fare_data_df, "payment_type", "UNK")
    """
    return df.withColumn(
        column_name,
        f.when(f.col(column_name) == unknown_value, None).otherwise(f.col(column_name))
    )

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

    Example:
        fare_data_df = replace_unknown_values_with_null(fare_data_df, "payment_type", "UNK")
    """
    return df.withColumn(
        column_name,
        f.when(f.col(column_name) == unknown_value, None).otherwise(f.col(column_name))
    )

def drop_columns_with_nulls(df, threshold):
    """
    Drops columns in dataframe in which null values amount exceeds given threshold.

    Args:
    df (DataFrame): Spark DataFrame to be processed.
    threshold (float): Threshold for the number of null values, exceeding which
                       results in the column being deleted. Changes from 0.0 to 1.0.

    Returns:
        DataFrame: New Spark DataFrame where columns, that do not satisfy threshold, were dropped.

    Example:
        fare_data_df = drop_columns_with_nulls(fare_data_df, threshold=0.7)
    """
    total_rows = df.count()
    columns_to_drop = []

    for col in df.columns:
        null_count = df.filter(df[col].isNull()).count()
        null_part = null_count / total_rows

        # print(f"Column '{col}' has {null_part * 100:.2f}% null values.")

        if null_part > threshold:
            columns_to_drop.append(col)

    df_cleaned = df.drop(*columns_to_drop)

    return df_cleaned

from pyspark.sql import types as t
from pyspark.sql.functions import col

def read_fare_data_df(spark_session,
                      dataframe_path,
                      header=True,
                      sep=",",
                      mode="PERMISSIVE",
                      multi_line=False,
                      null_value=None):
    """
    Reads trip fare dataframe from given directory using defined schema for data and additional options.

    Args:
        spark_session (SparkSession): Spark session to perform operations.
        dataframe_path (str): Path to the directory containing CSV blocks.
        header (bool, optional): Signals if the first row is a header. Defaults to True.
        sep (str, optional): Separator for CSV file reader. Defaults to ",".
        mode (str, optional): Error handling mode ("PERMISSIVE", "DROPMALFORMED", "FAILFAST"). Defaults to "PERMISSIVE".
        multi_line (bool, optional): Support multi-line values in CSV. Defaults to False.
        null_value (str, optional): Value to be classified as null. Defaults to None.

    Returns:
        DataFrame: A Spark DataFrame with columns:
            - medallion (str): Taxi unique medallion.
            - hack_license (str): Driver's license number.
            - vendor_id (str): Taxi vendor's id.
            - pickup_datetime (timestamp): Date and time of the pickup.
            - payment_type (str): Payment type.
            - fare_amount (double): Fare for the trip.
            - surcharge (double): Additional charges for the trip.
            - mta_tax (double): MTA tax.
            - tip_amount (double): Tip given to the driver.
            - tolls_amount (double): Tolls paid during the trip.
            - total_amount (double): Total amount charged for the trip.
    """
    trip_fare_schema = t.StructType([
        t.StructField("medallion", t.StringType(), nullable=False),
        t.StructField(" hack_license", t.StringType(), nullable=False),
        t.StructField(" vendor_id", t.StringType(), nullable=False),
        t.StructField(" pickup_datetime", t.TimestampType(), nullable=False),
        t.StructField(" payment_type", t.StringType(), nullable=False),
        t.StructField(" fare_amount", t.DoubleType(), nullable=False),
        t.StructField(" surcharge", t.DoubleType(), nullable=False),
        t.StructField(" mta_tax", t.DoubleType(), nullable=False),
        t.StructField(" tip_amount", t.DoubleType(), nullable=False),
        t.StructField(" tolls_amount", t.DoubleType(), nullable=False),
        t.StructField(" total_amount", t.DoubleType(), nullable=False)
    ])

    df_reader = (
        spark_session.read
        .option("header", header)
        .option("sep", sep)
        .option("mode", mode)
        .option("multiLine", multi_line)
        .schema(trip_fare_schema)
    )

    if null_value:
        df_reader = df_reader.option("nullValue", null_value)

    df = df_reader.csv(dataframe_path)

    df = df.select([col(c).alias(c.strip()) for c in df.columns])

    return df


def write_fare_data_df_to_csv(df, write_folder_path, num_files=1, header=True, sep=","):
    """
    Writes passed fare data DataFrame to CSV files.

    Args:
        df (DataFrame): The DataFrame for writing.
        write_folder_path (str): Path for writing directory.
        num_files (int, optional): The number of partitions to split the DataFrame into. Defaults to 1.
        header (bool, optional): Specifies whether to write column names at line 1 or not. Defaults to True.
        sep (str, optional): Separator for CSV files. Defaults to ","

    Notes:
        - If the passed directory does not exist, it will be created automatically.
        - If num_files argument is passed with value < 1, it will default to 1.
    """
    if num_files < 1:
        num_files = 1

    df.repartition(num_files).write.csv(write_folder_path, mode='overwrite', header=header, sep=sep)

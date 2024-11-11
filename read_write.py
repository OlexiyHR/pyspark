"""
Module for reading and writing fare and trip data DataFrames.

This module contains read_fare_data_df(), write_fare_data_df_to_csv() functions for r/w fare data and
read_trip_data_df(), write_trip_data_df_to_csv() functions for r/w trip data.
"""

import settings as s
import constants

import os
from pyspark.sql import types as t
from pyspark.sql import functions as f
from pyspark.sql import DataFrame


def get_result_directory_path_for_question(question_number: int, part: int = 0) -> str:
    return os.path.join(s.RESULTS_PATH, f"question_{question_number}", str(part) if part != 0 else "")


def setup_directories():
    for i in range(constants.TOTAL_QUESTIONS_COUNT):
        result_path = get_result_directory_path_for_question(i+1)
        os.makedirs(result_path, exist_ok=True)


def read_fare_data_df(spark_session,
                      dataframe_path,
                      header=True,
                      sep=",",
                      mode="PERMISSIVE",
                      multi_line=False,
                      null_value=None):
    """
    Reads trip fare dataframe from passed directory using defined schema for data and additional options.

    Args:
        spark_session (SparkSession): Spark session to perform operations.
        dataframe_path (str): Path to the directory containing CSV blocks to read.
        header (bool, optional): Signals if the first row is a header. Defaults to True.
        sep (str, optional): Separator for CSV file reader. Defaults to ",".
        mode (str, optional): Error handling mode ("PERMISSIVE", "DROPMALFORMED", "FAILFAST"). Defaults to "PERMISSIVE".
        multi_line (bool, optional): Support multi-line values in CSV. Defaults to False.
        null_value (str, optional): Value that will be interpreted as null. Defaults to None.

    Returns:
        DataFrame: Trip fare data Spark DataFrame with following columns:
            - medallion (str): Unique identifier for taxi (medallion).
            - hack_license (str): Taxi driver's license number.
            - vendor_id (str): Taxi vendor's id.
            - pickup_datetime (timestamp): Date and time of the passenger pickup.
            - payment_type (str): Payment type.
            - fare_amount (double): Fare for the trip.
            - surcharge (double): Additional surcharges during the trip.
            - mta_tax (double): MTA tax.
            - tip_amount (double): Tip given to the driver.
            - tolls_amount (double): Tolls paid during the trip.
            - total_amount (double): Total amount charged for the trip.

    Examples:
        >>> fare_data_df = read_fare_data_df(
        ...     spark_session=spark_session,
        ...     dataframe_path="your/read/path",
        ...     header=True,
        ...     sep=",",
        ...     null_value="NULL",
        ...     mode="FAILFAST",
        ...     multi_line=True
        ... )
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

    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())

    return df


def write_fare_data_df_to_csv(df, write_folder_path, num_files=1, header=True, sep=","):
    """
    Writes passed trip fare data DataFrame to CSV file(s).

    Notes:
        - If the passed directory does not exist, it will be created automatically.
        - If num_files argument is passed with value < 1, it will default to 1.

    Args:
        df (DataFrame): The trip fare data DataFrame for writing.
        write_folder_path (str): Path for writing directory.
        num_files (int, optional): The number of partitions to split the DataFrame into. Defaults to 1.
        header (bool, optional): Specifies whether to write column names at line 1 or not. Defaults to True.
        sep (str, optional): Separator for CSV files. Defaults to ",".

    Examples:
        >>> write_fare_data_df_to_csv(
        ...     df=df,
        ...     write_folder_path="your/write/path",
        ...     num_files=10,
        ...     header=True,
        ...     sep=","
        ... )
    """
    if num_files < 1:
        num_files = 1

    df.repartition(num_files).write.csv(write_folder_path, mode='overwrite', header=header, sep=sep)


def read_trip_data_df(spark_session,
                      dataframe_path,
                      header=False,
                      sep=",",
                      mode="PERMISSIVE",
                      multi_line=False,
                      null_value=None):
    """
    Reads trip data dataframe from passed directory using defined schema for data and additional options.
    Problem:
    - Files in the directory have inconsistent headers:
      * In 1-2 files, the headers do not contain spaces between words.
      * In 3-12 files, the headers contain spaces between words in the column names.

    Args:
        spark_session (SparkSession): Spark session to perform operations.
        dataframe_path (str): Path to the directory containing CSV blocks to read.
        header (bool, optional): Signals if the first row is a header. Defaults to True.
        sep (str, optional): Separator for CSV file reader. Defaults to ",".
        mode (str, optional): Error handling mode ("PERMISSIVE", "DROPMALFORMED", "FAILFAST"). Defaults to "PERMISSIVE".
        multi_line (bool, optional): Support multi-line values in CSV. Defaults to False.
        null_value (str, optional): Value that will be interpreted as null. Defaults to None.

    Returns:
        DataFrame: Trip data Spark DataFrame with the following columns:
            - medallion (str): Unique identifier for taxi (medallion).
            - hack_license (str): Taxi driver's license number.
            - vendor_id (str): Taxi vendor's ID.
            - rate_code (int): Rate code for the trip.
            - store_and_fwd_flag (str): Whether the trip data was stored before forwarding. Should be bool type and contains null values.
            - pickup_datetime (timestamp): Date and time of the passenger pickup.
            - dropoff_datetime (timestamp): Date and time of the passenger dropoff.
            - passenger_count (int): Number of passengers during the trip.
            - trip_time_in_secs (int): Duration of the trip in seconds.
            - trip_distance (double): Distance traveled during the trip.
            - pickup_longitude (double): Longitude of the pickup location.
            - pickup_latitude (double): Latitude of the pickup location.
            - dropoff_longitude (double): Longitude of the dropoff location. Contains null values.
            - dropoff_latitude (double): Latitude of the dropoff location. Contains null values.
            - dropoff_longitude (double): Longitude of the dropoff location.
            - dropoff_latitude (double): Latitude of the dropoff location.

    Examples:
        >>> trip_data_df = read_trip_data_df(
        ...     spark_session=spark_session,
        ...     dataframe_path="your/read/path",
        ...     header=True,
        ...     sep=",",
        ...     null_value="NULL",
        ...     mode="FAILFAST",
        ...     multi_line=True
        ... )
    """
    trip_data_schema = t.StructType([
        t.StructField("medallion", t.StringType(), nullable=False),
        t.StructField("hack_license", t.StringType(), nullable=False),
        t.StructField("vendor_id", t.StringType(), nullable=False),
        t.StructField("rate_code", t.IntegerType(), nullable=False),
        t.StructField("store_and_fwd_flag", t.StringType(), nullable=True),
        t.StructField("pickup_datetime", t.TimestampType(), nullable=False),
        t.StructField("dropoff_datetime", t.TimestampType(), nullable=False),
        t.StructField("passenger_count", t.IntegerType(), nullable=False),
        t.StructField("trip_time_in_secs", t.IntegerType(), nullable=False),
        t.StructField("trip_distance", t.DoubleType(), nullable=False),
        t.StructField("pickup_longitude", t.DoubleType(), nullable=False),
        t.StructField("pickup_latitude", t.DoubleType(), nullable=False),
        t.StructField("dropoff_longitude", t.DoubleType(), nullable=True),
        t.StructField("dropoff_latitude", t.DoubleType(), nullable=True)
    ])

    df_reader = (
        spark_session.read
        .option("header", header)
        .option("sep", sep)
        .option("mode", mode)
        .option("multiLine", multi_line)
        .schema(trip_data_schema)
    )

    if null_value:
        df_reader = df_reader.option("nullValue", null_value)

    df = df_reader.csv(dataframe_path)

    df = df.filter(f.col(df.columns[0]) != "medallion")

    return df


def write_trip_data_df_to_csv(df, write_folder_path, num_files=1, header=True, sep=","):
    """
    Writes passed trip data DataFrame to CSV file(s).

    Args:
        df (DataFrame): The trip data DataFrame for writing.
        write_folder_path (str): Path for writing directory.
        num_files (int, optional): The number of partitions to split the DataFrame into. Defaults to 1.
        header (bool, optional): Specifies whether to write column names at line 1 or not. Defaults to True.
        sep (str, optional): Separator for CSV files. Defaults to ",".

    Notes:
        - If the passed directory does not exist, it will be created automatically.
        - If num_files argument is passed with value < 1, it will default to 1.

    Examples:
        >>> write_trip_data_df_to_csv(
        ...     df=df,
        ...     write_folder_path="your/write/path",
        ...     num_files=10,
        ...     header=True,
        ...     sep=","
        ... )
    """
    if num_files < 1:
        num_files = 1

    df.repartition(num_files).write.csv(write_folder_path, mode='overwrite', header=header, sep=sep)


def write_question_results(results, question_num: int, part: int = 0):
    if isinstance(results, DataFrame):
        write_df_to_csv(results, get_result_directory_path_for_question(question_num, part))
    else:
        write_results_to_txt(results, get_result_directory_path_for_question(question_num, part))


def write_df_to_csv(df, folder_path):
    (df
     .repartition(s.WRITE_PARTITION)
     .write
     .csv(folder_path, mode='overwrite', header=True, sep=','))


def write_results_to_txt(results, folder_path):
    with open(os.path.join(folder_path, "results.txt"), "w") as file:
        file.write(str(results))

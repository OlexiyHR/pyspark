from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def basic_test_df(spark_session):
    """
    Creates and returns a basic DataFrame using PySpark.

    This function generates a DataFrame with the following three columns:
    - name (StringType): The person's name.
    - age (IntegerType): The person's age.
    - city (StringType): The city where the person lives.

    Parameters:
    -----------
    spark_session : pyspark.sql.SparkSession
        An existing Spark session used to create the DataFrame.

    Returns:
    --------
    pyspark.sql.DataFrame
        A DataFrame containing a small set of test data with three columns: name, age, and city.

    Example:
    --------
    +----+---+--------+
    |name|age|    city|
    +----+---+--------+
    |John| 25|New York|
    |Anna| 30|  London|
    |Mike| 35|  Sydney|
    +----+---+--------+
    """

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True)
    ])

    data = [
        ("John", 25, "New York"),
        ("Anna", 30, "London"),
        ("Mike", 35, "Sydney")
    ]

    df = spark_session.createDataFrame(data, schema)

    return df
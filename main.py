from pyspark import SparkConf
from pyspark.sql import SparkSession

from basic_dfs.basic_df_mykytyshyn import basic_test_df


def create_spark_session():
    """
    Creates and returns Spark session

    Returns: SparkSession object
    """
    spark_conf = SparkConf()
    spark = (SparkSession.builder
             .master("local")
             .appName("pyspark project")
             .config(conf=spark_conf)
             .getOrCreate())
    return spark


def display_demo_dataframe():
    df = basic_test_df(spark_session)
    df.show()


if __name__ == "__main__":
    spark_session = create_spark_session()

    display_demo_dataframe()

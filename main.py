from pyspark import SparkConf
from pyspark.sql import SparkSession

from basic_dfs import basic_df_Krasovskyy as basic_df_k
from basic_dfs.basic_df_mykytyshyn import basic_test_df as basic_test_df_myk
from basic_dfs.basic_df_Hromiak import basic_test_df as basic_test_df_Hromiak


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


def display_demo_dataframe_mykytyshyn():
    df = basic_test_df_myk(spark_session)
    df.show()


def display_demo_dataframe_krasovskyy():
    basic_df_k.basic_test_df(spark_session=spark_session).show()


def display_demo_dataframe_Hromiak():
    df = basic_test_df_Hromiak(spark_session)
    df.show()

if __name__ == "__main__":
    spark_session = create_spark_session()

    display_demo_dataframe_krasovskyy()
    display_demo_dataframe_mykytyshyn()
    display_demo_dataframe_Hromiak()

    spark_session.stop()

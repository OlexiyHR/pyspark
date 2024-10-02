from pyspark import SparkConf
from pyspark.sql import SparkSession

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

if __name__ == "__main__":
    spark_session = create_spark_session()

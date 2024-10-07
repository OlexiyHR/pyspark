from pyspark.sql import functions as f
import pyspark.sql.types as t


def basic_test_df(spark_session):
    """
    Creates DataFrame with shop transaction data and adds column for total price.

    Args:
        spark_session (SparkSession): Spark session to create the DataFrame.

    Returns:
        DataFrame: A Spark DataFrame with columns:
            - transaction_id (int): Unique transaction id.
            - item (str): Item name.
            - quantity (int): Purchased item quantity.
            - price_per_item (float): Price per item.
            - purchase_date (date): Transaction date.
            - total_price (float): Transaction total price.

    Note: This function performs the following operations on DataFrame:
        1. Converts the 'purchase_date' column from string format to a DateType.
        2. Calculates new column 'total_price' by multiplying 'quantity' by 'price_per_item'.
    """
    data = [
        (1, "Pen", 5, 2.5, "2024-09-01"),
        (2, "Notebook", 2, 3.8, "2024-09-02"),
        (3, "Pencil", 10, 1.2, "2024-09-03"),
        (4, "Eraser", 3, 0.8, "2024-09-01"),
        (5, "Ruler", 1, 1.5, "2024-09-04"),
        (6, "Marker", 4, 2.0, "2024-09-05"),
        (7, "Scissors", 1, 4.0, "2024-09-06"),
        (8, "Glue", 6, 1.0, "2024-09-07"),
        (9, "Stapler", 1, 5.5, "2024-09-08"),
        (10, "Folder", 3, 2.5, "2024-09-01"),
        (11, "Highlighter", 2, 1.8, "2024-09-09"),
        (12, "Paper Clips", 10, 0.5, "2024-09-10"),
        (13, "Calculator", 1, 9.9, "2024-09-11"),
        (14, "Tape", 3, 1.3, "2024-09-12"),
        (15, "Sharpener", 2, 0.7, "2024-09-13"),
        (16, "Sticky Notes", 4, 1.1, "2024-09-14"),
        (17, "Correction Fluid", 2, 2.2, "2024-09-15"),
        (18, "Binder", 1, 3.5, "2024-09-16"),
        (19, "Index Tabs", 5, 1.9, "2024-09-17"),
        (20, "Hole Punch", 1, 6.0, "2024-09-18")
    ]

    schema = t.StructType([
        t.StructField("transaction_id", t.IntegerType(), False),
        t.StructField("item", t.StringType(), False),
        t.StructField("quantity", t.IntegerType(), False),
        t.StructField("price_per_item", t.FloatType(), False),
        t.StructField("purchase_date", t.StringType(), True)
    ])

    df = spark_session.createDataFrame(data, schema)

    df = df.withColumn("purchase_date", f.to_date(f.col("purchase_date"), "yyyy-MM-dd"))
    df = df.withColumn("total_price", f.col("quantity") * f.col("price_per_item"))

    return df

import pyspark.sql.types as t


def basic_test_df():
    from main import spark_session

    data = [("Concert ticket", 450), ("Ergonomic chair", 153), ("Food delivery", 87)]
    schema = t.StructType([
        t.StructField("expense_name", t.StringType(), False),
        t.StructField("expense_amount", t.IntegerType(), False)
    ])

    return spark_session.createDataFrame(data, schema)

import pyspark.sql.types as t


def basic_test_df(session):
    data = [("Concert ticket", 450), ("Ergonomic chair", 153), ("Food delivery", 87)]
    schema = t.StructType([
        t.StructField("expense_name", t.StringType(), False),
        t.StructField("expense_amount_dollars", t.IntegerType(), False)
    ])

    return session.createDataFrame(data, schema)

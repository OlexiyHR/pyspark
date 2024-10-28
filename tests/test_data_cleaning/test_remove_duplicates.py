import unittest
from pyspark.sql import SparkSession
import columns as c
from data_cleaning.remove_duplicates import remove_duplicates


class RemoveDuplicatesTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_remove_duplicates_single_duplicate(self):
        data = [
            (1, '2023-10-01 10:00:00', 'value1'),
            (1, '2023-10-01 10:00:00', 'value2'),
            (2, '2023-10-01 11:00:00', 'value3')
        ]
        columns = [c.medallion, c.pickup_datetime, 'other_column']
        df = self.spark.createDataFrame(data, columns)
        expected_data = [
            (1, '2023-10-01 10:00:00', 'value1'),
            (2, '2023-10-01 11:00:00', 'value3')
        ]
        expected_df = self.spark.createDataFrame(expected_data, columns)

        result_df = remove_duplicates(df)

        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_remove_duplicates_no_duplicates(self):
        data = [
            (1, '2023-10-01 10:00:00', 'value1'),
            (2, '2023-10-01 11:00:00', 'value2'),
            (3, '2023-10-01 12:00:00', 'value3')
        ]
        columns = [c.medallion, c.pickup_datetime, 'other_column']
        df = self.spark.createDataFrame(data, columns)
        expected_df = self.spark.createDataFrame(data, columns)

        result_df = remove_duplicates(df)

        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_remove_duplicates_multiple_duplicates(self):
        data = [
            (1, '2023-10-01 10:00:00', 'value1'),
            (1, '2023-10-01 10:00:00', 'value2'),
            (1, '2023-10-01 10:00:00', 'value3'),
            (2, '2023-10-01 11:00:00', 'value4'),
            (2, '2023-10-01 11:00:00', 'value5')
        ]
        columns = [c.medallion, c.pickup_datetime, 'other_column']
        df = self.spark.createDataFrame(data, columns)
        expected_data = [
            (1, '2023-10-01 10:00:00', 'value1'),
            (2, '2023-10-01 11:00:00', 'value4')
        ]
        expected_df = self.spark.createDataFrame(expected_data, columns)

        result_df = remove_duplicates(df)

        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_remove_duplicates_empty_dataframe(self):
        columns = [c.medallion, c.pickup_datetime, 'other_column']
        df = self.spark.createDataFrame([], schema=columns)
        expected_df = self.spark.createDataFrame([], schema=columns)

        result_df = remove_duplicates(df)

        self.assertEqual(result_df.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main()

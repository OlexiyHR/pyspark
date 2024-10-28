import unittest
from pyspark.sql import SparkSession

import columns as c
from data_analysis.trip_data_analysis import count_short_trips, count_large_group_trips


class TripDataAnalysisTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_count_short_trips(self):
        data = [
            (0.5, 2),
            (1.2, 4),
            (0.8, 1)
        ]
        columns = [c.trip_distance, c.passenger_count]
        df = self.spark.createDataFrame(data, columns)

        short_trips_count = count_short_trips(df)

        self.assertEqual(short_trips_count, 2)

    def test_count_large_group_trips(self):
        data = [
            (2.0, 6),
            (3.5, 3),
            (4.0, 6),
            (1.0, 7)
        ]
        columns = [c.trip_distance, c.passenger_count]
        df = self.spark.createDataFrame(data, columns)

        large_group_trips_count = count_large_group_trips(df)

        self.assertEqual(large_group_trips_count, 3)

    def test_count_no_short_trips(self):
        data = [
            (1.2, 2),
            (3.0, 4),
            (5.0, 3)
        ]
        columns = [c.trip_distance, c.passenger_count]
        df = self.spark.createDataFrame(data, columns)
        short_trips_count = count_short_trips(df)
        self.assertEqual(short_trips_count, 0)

    def test_count_no_large_group_trips(self):
        data = [
            (1.2, 2),
            (3.0, 3),
            (5.0, 4)
        ]
        columns = [c.trip_distance, c.passenger_count]
        df = self.spark.createDataFrame(data, columns)

        large_group_trips_count = count_large_group_trips(df)

        self.assertEqual(large_group_trips_count, 0)


if __name__ == "__main__":
    unittest.main()

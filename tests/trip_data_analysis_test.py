import unittest
from pyspark.sql import SparkSession

import columns as c
from data_analysis.trip_data_analysis import count_short_trips, count_large_group_trips, count_medium_duration_trips, jfk_airport_trips_with_four_passengers


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


    def test_count_trips_duration_between_30_and_60_minutes(self):
        data = [
            (1800, 2),  # 30 minutes
            (2400, 3),  # 40 minutes
            (3600, 4),  # 60 minutes
            (1200, 1)   # 20 minutes
        ]
        columns = [c.trip_time_in_secs, c.passenger_count]
        df = self.spark.createDataFrame(data, columns)

        trips_duration_count = count_medium_duration_trips(df)

        self.assertEqual(trips_duration_count, 3)  # 1800, 2400, and 3600 are between 30 and 60 minutes


    def test_jfk_airport_trips_with_four_passengers(self):
        data = [
            (4, 2),  # 4 passengers, JFK rate code
            (3, 2),  # Not 4 passengers
            (4, 1),  # Not JFK rate code
            (4, 2)   # 4 passengers, JFK rate code
        ]
        columns = [c.passenger_count, c.rate_code]
        df = self.spark.createDataFrame(data, columns)

        jfk_trips_with_four_passengers_df = jfk_airport_trips_with_four_passengers(df)

        self.assertEqual(jfk_trips_with_four_passengers_df.count(), 2)  # Two trips with 4 passengers and JFK rate code


if __name__ == "__main__":
    unittest.main()

import unittest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

import columns as c
from data_analysis.trip_data_analysis import (count_short_trips,
                                              count_large_group_trips,
                                              count_medium_duration_trips,
                                              jfk_airport_trips_with_four_passengers,
                                              trip_amounts_distribution_by_vendor,
                                              average_trip_speed_by_month,
                                              passenger_count_distribution,
                                              short_trip_distribution_by_day_ranked,
                                              top_10_drivers_by_distance_per_month,
                                              passenger_count_by_time_of_day)


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
            (1200, 1)  # 20 minutes
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
            (4, 2)  # 4 passengers, JFK rate code
        ]
        columns = [c.passenger_count, c.rate_code]
        df = self.spark.createDataFrame(data, columns)

        jfk_trips_with_four_passengers_df = jfk_airport_trips_with_four_passengers(df)

        self.assertEqual(jfk_trips_with_four_passengers_df.count(), 2)  # Two trips with 4 passengers and JFK rate code

    def test_trip_amounts_distribution_by_vendor(self):
        data = [
            ("vendor_1", 10.5),
            ("vendor_2", 15.3),
            ("vendor_1", 7.8),
            ("vendor_2", 12.1),
            ("vendor_1", 9.7)
        ]
        columns = [c.vendor_id, c.total_amount]
        df = self.spark.createDataFrame(data, columns)

        expected_data = [
            ("vendor_1", 3),
            ("vendor_2", 2),
        ]
        result_columns = [c.vendor_id, "trip_count"]
        expected_df = self.spark.createDataFrame(expected_data, result_columns)

        actual_df = trip_amounts_distribution_by_vendor(df)

        assertDataFrameEqual(actual_df, expected_df)

    def test_average_trip_speed_by_month(self):
        data = [
            ("2024-01-15 10:00:00", 10.0, 600),
            ("2024-01-20 14:30:00", 15.0, 900),
            ("2024-02-10 12:00:00", 20.0, 1200)
        ]
        columns = [c.pickup_datetime, c.trip_distance, c.trip_time_in_secs]
        df = self.spark.createDataFrame(data, columns)

        result_df = average_trip_speed_by_month(df)

        result_data = {row["month"]: row["average_trip_speed_in_miles_per_hour"] for row in result_df.collect()}
        self.assertAlmostEqual(result_data.get(1), 60.0, places=1)
        self.assertAlmostEqual(result_data.get(2), 60.0, places=1)

    def test_passenger_count_distribution(self):
        data = [
            (1, 100.01),
            (2, 50.0),
            (1, 24.0),
            (3, 123.34),
            (2, 19.99),
            (1, 5.55)
        ]
        columns = ["passenger_count", "total_amount"]
        df = self.spark.createDataFrame(data, columns)

        expected_data = [
            (1, 3),
            (2, 2),
            (3, 1)
        ]
        result_columns = ["passenger_count", "trip_count"]
        expected_df = self.spark.createDataFrame(expected_data, result_columns)

        actual_df = passenger_count_distribution(df)

        assertDataFrameEqual(actual_df, expected_df)


    def test_short_trip_distribution_by_day_ranked(self):
        data = [
            ("2024-10-28 10:00:00", 1.5),  # Monday, short trip
            ("2024-10-29 11:00:00", 1.8),  # Tuesday, short trip
            ("2024-10-28 12:00:00", 1.0),  # Monday, short trip
            ("2024-10-30 09:00:00", 2.0),  # Wednesday, not a short trip
            ("2024-10-28 15:00:00", 1.2),  # Monday, short trip
            ("2024-10-29 16:00:00", 1.7)  # Tuesday, short trip
        ]
        columns = [c.pickup_datetime, c.trip_distance]
        df = self.spark.createDataFrame(data, columns)

        expected_data = [
            (2, 3, 1),  # Monday (day 2), 3 short trips, rank 1
            (3, 2, 2)  # Tuesday (day 3), 2 short trips, rank 2
        ]
        expected_schema = ["day_of_week", "short_trip_count", "ranking"]
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

        actual_df = short_trip_distribution_by_day_ranked(df)

        assertDataFrameEqual(actual_df, expected_df)

    def test_top_10_drivers_by_distance_per_month(self):
        data = [
            ("2024-01-15 10:00:00", "driver_1", 100.0),
            ("2024-01-20 12:00:00", "driver_2", 150.0),
            ("2024-01-22 14:00:00", "driver_3", 80.0),
            ("2024-01-25 16:00:00", "driver_4", 50.0),
            ("2024-01-28 18:00:00", "driver_5", 120.0),
            ("2024-02-10 10:00:00", "driver_1", 200.0),
            ("2024-02-12 12:00:00", "driver_2", 250.0),
            ("2024-02-14 14:00:00", "driver_3", 180.0),
            ("2024-02-16 16:00:00", "driver_4", 300.0),
            ("2024-02-18 18:00:00", "driver_5", 170.0)
        ]
        columns = [c.pickup_datetime, c.hack_license, c.trip_distance]
        df = self.spark.createDataFrame(data, columns)

        expected_data = [
            (1, "driver_2", 150.0, 1),
            (1, "driver_5", 120.0, 2),
            (1, "driver_1", 100.0, 3),
            (1, "driver_3", 80.0, 4),
            (1, "driver_4", 50.0, 5),
            (2, "driver_4", 300.0, 1),
            (2, "driver_2", 250.0, 2),
            (2, "driver_1", 200.0, 3),
            (2, "driver_3", 180.0, 4),
            (2, "driver_5", 170.0, 5)
        ]
        expected_columns = ["month", "hack_license", "total_distance", "ranking"]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)

        actual_df = top_10_drivers_by_distance_per_month(df)

        assertDataFrameEqual(actual_df, expected_df)


    def test_passenger_count_by_time_of_day(self):
        data = [
            ("2023-12-01 07:00:00", 2),  # Morning
            ("2023-10-02 13:00:00", 3),  # Afternoon
            ("2023-09-03 20:00:00", 1),  # Evening
            ("2023-08-04 23:00:00", 4),  # Evening
        ]
        columns = ["pickup_datetime", "passenger_count"]
        df = self.spark.createDataFrame(data, columns)

        result_df = passenger_count_by_time_of_day(df)
        result = {row.time_of_day: row.average_passenger_count for row in result_df.collect()}

        self.assertAlmostEqual(result["morning"], 2.0, places=2)
        self.assertAlmostEqual(result["afternoon"], 3.0, places=2)
        self.assertAlmostEqual(result["evening"], 2.5, places=2)


if __name__ == "__main__":
    unittest.main()

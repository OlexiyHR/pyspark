import unittest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

import columns as c
import data_analysis.all_data_analysis as ada


class AllDataAnalysisTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_distance_tip_correlation_perfect_correlation(self):
        """
        Tests that the correlation between trip distance and tip amount is 1
        when they have a perfect positive relationship.
        """
        data_trip_data = [
            ("med1", "license1", "vendor1", "2024-11-10 10:00:00", 10.0),
            ("med2", "license2", "vendor1", "2024-11-10 11:00:00", 20.0),
        ]
        data_trip_fare = [
            ("med1", "license1", "vendor1", "2024-11-10 10:00:00", 10.0),
            ("med2", "license2", "vendor1", "2024-11-10 11:00:00", 20.0),
        ]
        trip_data_df = self.spark.createDataFrame(data_trip_data,
                                                  [c.medallion, c.hack_license, c.vendor_id, c.pickup_datetime,
                                                   c.trip_distance])
        trip_fare_df = self.spark.createDataFrame(data_trip_fare,
                                                  [c.medallion, c.hack_license, c.vendor_id, c.pickup_datetime,
                                                   c.tip_amount])

        correlation = ada.column_tip_correlation(trip_data_df, trip_fare_df, column_name=c.trip_distance)

        self.assertEqual(correlation, 1)

    def test_distance_tip_correlation_no_correlation(self):
        """
        Tests that the correlation between trip duration and tip amount is 0
        when there is no relationship between the columns.
        """
        data_trip_data = [
            ("med1", "license1", "vendor1", "2024-11-10 10:00:00", 600),
            ("med2", "license2", "vendor1", "2024-11-10 11:00:00", 1200),
        ]
        data_trip_fare = [
            ("med1", "license1", "vendor1", "2024-11-10 10:00:00", 15.0),
            ("med2", "license2", "vendor1", "2024-11-10 11:00:00", 15.0),
        ]
        trip_data_df = self.spark.createDataFrame(data_trip_data,
                                                  [c.medallion, c.hack_license, c.vendor_id, c.pickup_datetime,
                                                   c.trip_time_in_secs])
        trip_fare_df = self.spark.createDataFrame(data_trip_fare,
                                                  [c.medallion, c.hack_license, c.vendor_id, c.pickup_datetime,
                                                   c.tip_amount])

        correlation = ada.column_tip_correlation(trip_data_df, trip_fare_df, column_name=c.trip_time_in_secs)

        self.assertEqual(correlation, 0)

    def test_distance_tip_correlation_empty_result(self):
        """
        Tests that the correlation function returns None when the join results in an empty DataFrame.
        """
        data_trip_data = [
            ("med1", "license1", "vendor1", "2024-11-10 10:00:00", 10.0),
        ]
        data_trip_fare = [
            ("med2", "license2", "vendor1", "2024-11-10 11:00:00", 15.0),
        ]
        trip_data_df = self.spark.createDataFrame(data_trip_data,
                                                  [c.medallion, c.hack_license, c.vendor_id, c.pickup_datetime,
                                                   c.trip_distance])
        trip_fare_df = self.spark.createDataFrame(data_trip_fare,
                                                  [c.medallion, c.hack_license, c.vendor_id, c.pickup_datetime,
                                                   c.tip_amount])

        correlation = ada.column_tip_correlation(trip_data_df, trip_fare_df, column_name=c.trip_distance)

        self.assertIsNone(correlation)

    def test_count_trips_missing_fare_zero(self):
        """
        Tests that the count of trips with missing fare information is 0
        when all trips have fare data.
        """
        data_trip_data = [
            ("med1", "license1", "vendor1", "2024-11-10 10:00:00", 10.0),
            ("med2", "license2", "vendor1", "2024-11-10 11:00:00", 20.0),
        ]
        data_trip_fare = [
            ("med1", "license1", "vendor1", "2024-11-10 10:00:00", 15.0),
            ("med2", "license2", "vendor1", "2024-11-10 11:00:00", 20.0),
        ]
        trip_data_df = self.spark.createDataFrame(data_trip_data,
                                                  [c.medallion, c.hack_license, c.vendor_id, c.pickup_datetime,
                                                   c.trip_distance])
        trip_fare_df = self.spark.createDataFrame(data_trip_fare,
                                                  [c.medallion, c.hack_license, c.vendor_id, c.pickup_datetime,
                                                   c.total_amount])

        missing_fare_count = ada.count_trips_missing_fare(trip_data_df, trip_fare_df)

        self.assertEqual(missing_fare_count, 0)

    def test_count_trips_missing_fare_positive(self):
        """
        Tests that the count of trips with missing fare information is positive
        when there are trips without corresponding fare data.
        """
        data_trip_data = [
            ("med1", "license1", "vendor1", "2024-11-10 10:00:00", 10.0),
            ("med2", "license2", "vendor1", "2024-11-10 11:00:00", 20.0),
            ("med3", "license3", "vendor1", "2024-11-10 12:00:00", 15.0),
        ]
        data_trip_fare = [
            ("med1", "license1", "vendor1", "2024-11-10 10:00:00", 15.0),
            ("med2", "license2", "vendor1", "2024-11-10 11:00:00", 20.0),
        ]
        trip_data_df = self.spark.createDataFrame(data_trip_data,
                                                  [c.medallion, c.hack_license, c.vendor_id, c.pickup_datetime,
                                                   c.trip_distance])
        trip_fare_df = self.spark.createDataFrame(data_trip_fare,
                                                  [c.medallion, c.hack_license, c.vendor_id, c.pickup_datetime,
                                                   c.total_amount])

        missing_fare_count = ada.count_trips_missing_fare(trip_data_df, trip_fare_df)

        self.assertEqual(missing_fare_count, 1)


if __name__ == "__main__":
    unittest.main()

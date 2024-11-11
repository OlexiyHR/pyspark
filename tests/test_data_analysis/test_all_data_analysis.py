import unittest
from pyspark.sql import SparkSession, Row

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

    def test_passenger_count_vs_trip_price(self):
        trip_data = [
            Row(medallion="1", hack_license="A", pickup_datetime="2022-01-01 10:00:00", passenger_count=1),
            Row(medallion="2", hack_license="B", pickup_datetime="2022-01-01 11:00:00", passenger_count=1),
            Row(medallion="3", hack_license="C", pickup_datetime="2022-01-01 12:00:00", passenger_count=2),
            Row(medallion="4", hack_license="D", pickup_datetime="2022-01-01 13:00:00", passenger_count=2),
            Row(medallion="5", hack_license="E", pickup_datetime="2022-01-01 14:00:00", passenger_count=3)
        ]
        fare_data = [
            Row(medallion="1", hack_license="A", pickup_datetime="2022-01-01 10:00:00", total_amount=10.0),
            Row(medallion="2", hack_license="B", pickup_datetime="2022-01-01 11:00:00", total_amount=20.0),
            Row(medallion="3", hack_license="C", pickup_datetime="2022-01-01 12:00:00", total_amount=30.0),
            Row(medallion="4", hack_license="D", pickup_datetime="2022-01-01 13:00:00", total_amount=40.0),
            Row(medallion="5", hack_license="E", pickup_datetime="2022-01-01 14:00:00", total_amount=50.0)
        ]

        trip_data_df = self.spark.createDataFrame(trip_data)
        fare_data_df = self.spark.createDataFrame(fare_data)

        result_df = ada.passenger_count_vs_trip_price(trip_data_df, fare_data_df)
        result = {row.passenger_count: row.average_trip_price for row in result_df.collect()}

        self.assertEqual(result, {
            1: 15.0,  # (10 + 20) / 2
            2: 35.0,  # (30 + 40) / 2
            3: 50.0  # 50 / 1
        })

    def test_most_popular_rate_code_by_payment_type(self):
        trip_data = [
            Row(medallion="1", hack_license="A", pickup_datetime="2022-01-01 10:00:00", rate_code=1),
            Row(medallion="2", hack_license="B", pickup_datetime="2022-01-01 11:00:00", rate_code=1),
            Row(medallion="3", hack_license="C", pickup_datetime="2022-01-01 12:00:00", rate_code=2),
            Row(medallion="4", hack_license="D", pickup_datetime="2022-01-01 13:00:00", rate_code=2),
            Row(medallion="5", hack_license="E", pickup_datetime="2022-01-01 14:00:00", rate_code=2)
        ]
        fare_data = [
            Row(medallion="1", hack_license="A", pickup_datetime="2022-01-01 10:00:00", payment_type="card"),
            Row(medallion="2", hack_license="B", pickup_datetime="2022-01-01 11:00:00", payment_type="card"),
            Row(medallion="3", hack_license="C", pickup_datetime="2022-01-01 12:00:00", payment_type="cash"),
            Row(medallion="4", hack_license="D", pickup_datetime="2022-01-01 13:00:00", payment_type="cash"),
            Row(medallion="5", hack_license="E", pickup_datetime="2022-01-01 14:00:00", payment_type=None)
        ]

        trip_data_df = self.spark.createDataFrame(trip_data)
        fare_data_df = self.spark.createDataFrame(fare_data)

        result_df = ada.most_popular_rate_code_by_payment_type(trip_data_df, fare_data_df)
        result = {row.payment_type: row.rate_code for row in result_df.collect()}

        # Перевірка для "card", "cash" та None як окремий payment_type
        self.assertEqual(result, {"card": 1, "cash": 2, None: 2})

    def test_get_most_profitable_rate_codes(self):
        """Test get_most_profitable_rate_codes() function."""
        trip_data = [
            ("medallion_1", "license_1", "vendor_1", 1, "2024-01-01 08:00:00"),
            ("medallion_2", "license_2", "vendor_2", 2, "2024-01-02 09:00:00"),
            ("medallion_3", "license_3", "vendor_1", 1, "2024-01-03 10:00:00"),
            ("medallion_4", "license_4", "vendor_2", 3, "2024-01-04 11:00:00"),
            ("medallion_5", "license_5", "vendor_1", 2, "2024-01-05 12:00:00"),
        ]
        fare_data = [
            ("medallion_1", "license_1", "vendor_1", "2024-01-01 08:00:00", 20.0),
            ("medallion_2", "license_2", "vendor_2", "2024-01-02 09:00:00", 30.0),
            ("medallion_3", "license_3", "vendor_1", "2024-01-03 10:00:00", 10.0),
            ("medallion_4", "license_4", "vendor_2", "2024-01-04 11:00:00", 50.0),
            ("medallion_5", "license_5", "vendor_1", "2024-01-05 12:00:00", 40.0),
        ]

        trip_columns = [c.medallion, c.hack_license, c.vendor_id, c.rate_code, c.pickup_datetime]
        fare_columns = [c.medallion, c.hack_license, c.vendor_id, c.pickup_datetime, c.total_amount]

        trip_data_df = self.spark.createDataFrame(trip_data, schema=trip_columns)
        fare_data_df = self.spark.createDataFrame(fare_data, schema=fare_columns)

        result_df = ada.get_most_profitable_rate_codes(trip_data_df, fare_data_df)

        expected_data = [
            (2, 70.0),
            (3, 50.0),
            (1, 30.0),
        ]
        expected_columns = [c.rate_code, "total_revenue"]

        expected_df = self.spark.createDataFrame(expected_data, schema=expected_columns)

        diff_filtered = result_df.exceptAll(expected_df)
        diff_expected = expected_df.exceptAll(result_df)

        self.assertTrue(diff_filtered.isEmpty())
        self.assertTrue(diff_expected.isEmpty())

    def test_get_rate_codes_with_tolls_percentage(self):
        """Test get_rate_codes_with_tolls_percentage() function."""
        trip_data = [
            ("medallion_1", "license_1", "vendor_1", 1, "2024-01-01 08:00:00"),
            ("medallion_2", "license_2", "vendor_2", 2, "2024-01-02 09:00:00"),
            ("medallion_3", "license_3", "vendor_1", 1, "2024-01-03 10:00:00"),
            ("medallion_3", "license_3", "vendor_1", 1, "2024-01-03 20:00:00"),
            ("medallion_4", "license_4", "vendor_2", 3, "2024-01-04 11:00:00"),
            ("medallion_5", "license_5", "vendor_1", 2, "2024-01-05 12:00:00"),
            ("medallion_6", "license_6", "vendor_2", 1, "2024-01-06 13:00:00"),
        ]
        fare_data = [
            ("medallion_1", "license_1", "vendor_1", "2024-01-01 08:00:00", 5.0),
            ("medallion_2", "license_2", "vendor_2", "2024-01-02 09:00:00", 0.0),
            ("medallion_3", "license_3", "vendor_1", "2024-01-03 10:00:00", 5.0),
            ("medallion_3", "license_3", "vendor_1", "2024-01-03 20:00:00", 2.0),
            ("medallion_4", "license_4", "vendor_2", "2024-01-04 11:00:00", 0.0),
            ("medallion_5", "license_5", "vendor_1", "2024-01-05 12:00:00", 3.0),
            ("medallion_6", "license_6", "vendor_2", "2024-01-06 13:00:00", 0.0),
        ]

        trip_columns = [c.medallion, c.hack_license, c.vendor_id, c.rate_code, c.pickup_datetime]
        fare_columns = [c.medallion, c.hack_license, c.vendor_id, c.pickup_datetime, c.tolls_amount]

        trip_data_df = self.spark.createDataFrame(trip_data, schema=trip_columns)
        fare_data_df = self.spark.createDataFrame(fare_data, schema=fare_columns)

        result_df = ada.get_rate_codes_with_tolls_percentage(trip_data_df, fare_data_df)

        expected_data = [
            (1, 4, 3, 75.0),
            (2, 2, 1, 50.0),
            (3, 1, 0, 0.0),
        ]
        expected_columns = [c.rate_code, "total_trips", "tolls_count", "tolls_percent"]

        expected_df = self.spark.createDataFrame(expected_data, schema=expected_columns)

        diff_filtered = result_df.exceptAll(expected_df)
        diff_expected = expected_df.exceptAll(result_df)

        self.assertTrue(diff_filtered.isEmpty())
        self.assertTrue(diff_expected.isEmpty())


if __name__ == "__main__":
    unittest.main()

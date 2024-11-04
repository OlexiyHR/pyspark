import unittest
from pyspark.sql import SparkSession

import columns as c
import data_analysis.fare_data_analysis as fd_analysis


class FareDataAnalysisTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_average_card_payment_total(self):
        """
        Tests calculation of the average total amount for trips paid by card.
        """
        data = [
            ("CRD", 15.0),
            ("CRD", 25.0),
            ("CSH", 10.0)
        ]
        columns = [c.payment_type, c.total_amount]
        df = self.spark.createDataFrame(data, columns)

        avg_total = fd_analysis.average_card_payment_total(df)

        self.assertEqual(avg_total, 20.0)

    def test_average_card_payment_no_card_trips(self):
        """
        Tests average calculation when there are no card trips.
        """
        data = [
            ("CSH", 15.0),
            ("CSH", 25.0)
        ]
        columns = [c.payment_type, c.total_amount]
        df = self.spark.createDataFrame(data, columns)

        avg_total = fd_analysis.average_card_payment_total(df)

        self.assertEqual(avg_total, None)


    def test_count_trips_cost_at_least_50_dollars(self):
        data = [
            (60.0, 2),
            (45.0, 4),
            (55.0, 3),
            (50.0, 1)
        ]
        columns = [c.total_amount, c.passenger_count]
        df = self.spark.createDataFrame(data, columns)

        trips_cost_at_least_50_count = fd_analysis.count_expensive_trips(df)

        self.assertEqual(trips_cost_at_least_50_count, 3)  # 60, 55, and 50 are >= 50


    def test_average_tip_by_payment_type(self):
        data = [
            ("Card", 2.5),
            ("Cash", 0.0),
            ("Card", 3.0),
            ("Cash", 1.0)
        ]
        columns = ["payment_type", "tip_amount"]
        df = self.spark.createDataFrame(data, columns)

        result_df = fd_analysis.average_tip_by_payment_type(df)
        result = {row.payment_type: row.average_tip for row in result_df.collect()}

        self.assertAlmostEqual(result["Card"], 2.75, places=2)
        self.assertAlmostEqual(result["Cash"], 0.5, places=2)


    def test_vendor_with_highest_fare(self):
        data = [
            ("V1", 100.0),
            ("V2", 150.0),
            ("V1", 200.0),
            ("V2", 50.0)
        ]
        columns = ["vendor_id", "fare_amount"]
        df = self.spark.createDataFrame(data, columns)

        result_df = fd_analysis.vendor_with_highest_fare(df)
        top_vendor = result_df.first()

        self.assertEqual(top_vendor.vendor_id, "V1")
        self.assertAlmostEqual(top_vendor.total_fare, 300.0, places=2)


    def test_cumulative_total_fare_on_july_4(self):
        data = [
            ("D1", "2023-07-04 08:00:00", 50.0),
            ("D1", "2023-07-04 09:00:00", 20.0),
            ("D2", "2023-07-04 10:00:00", 30.0),
            ("D1", "2023-07-04 11:00:00", 10.0),
            ("D1", "2023-07-05 11:00:00", 100.0)
        ]
        columns = ["hack_license", "pickup_datetime", "total_amount"]
        df = self.spark.createDataFrame(data, columns)

        result_df = fd_analysis.cumulative_total_fare_on_july_4(df)
        result = {row.hack_license: row.cumulative_fare for row in
                  result_df.filter("pickup_datetime = '2023-07-04 11:00:00'").collect()}

        self.assertAlmostEqual(result["D1"], 80.0, places=2)


    def test_top_5_drivers_by_trip_count_on_july_4(self):
        data = [
            ("D1", "2023-07-04 08:00:00", 50.0),
            ("D2", "2023-07-04 09:00:00", 20.0),
            ("D1", "2023-07-04 10:00:00", 70.0),
            ("D3", "2023-07-04 11:00:00", 30.0),
            ("D1", "2023-07-04 12:00:00", 40.0),
            ("D2", "2023-07-04 13:00:00", 60.0),
            ("D2", "2023-07-05 13:00:00", 160.0)
        ]
        columns = ["hack_license", "pickup_datetime", "total_amount"]
        df = self.spark.createDataFrame(data, columns)

        result_df = fd_analysis.top_5_drivers_by_trip_count_on_july_4(df)
        result = [(row.hack_license, row.total_fare) for row in result_df.collect()]

        expected_top_drivers = [("D1", 160.0), ("D2", 80.0), ("D3", 30.0)]
        self.assertEqual(result, expected_top_drivers)


    def test_count_evening_rides_with_high_total_amount(self):
        """Test count_evening_rides_with_high_total_amount() in general case."""
        data = [
            ("2024-10-05 11:30:00", 22.0),
            ("2024-10-05 22:30:00", 24.0),
            ("2024-10-06 18:30:00", 50.0),
            ("2024-10-06 22:30:00", 49.0),
            ("2024-10-07 08:15:00", 50.0),
            ("2024-10-07 19:00:00", 16.0),
            ("2024-10-08 21:00:00", 13.0),
            ("2024-10-08 20:00:00", 78.0),
            ("2024-10-09 23:59:00", 13.0)
        ]

        columns = [c.pickup_datetime, c.total_amount]

        df = self.spark.createDataFrame(data, schema=columns)

        evening_rides_with_high_total_amount_count = fd_analysis.count_evening_rides_with_high_total_amount(df)
        self.assertIsInstance(evening_rides_with_high_total_amount_count, int)
        self.assertEqual(evening_rides_with_high_total_amount_count, 3)

    def test_count_evening_rides_with_high_total_amount_no_evening_trips(self):
        """Test count_evening_rides_with_high_total_amount() when there are no evening trips."""
        data = [
            ("2024-10-05 11:30:00", 5.0),
            ("2024-10-05 12:30:00", 24.0),
            ("2024-10-06 07:30:00", 50.0),
            ("2024-10-07 08:15:00", 50.0),
        ]

        columns = [c.pickup_datetime, c.total_amount]

        df = self.spark.createDataFrame(data, schema=columns)

        evening_rides_with_high_total_amount_count = fd_analysis.count_evening_rides_with_high_total_amount(df)
        self.assertIsInstance(evening_rides_with_high_total_amount_count, int)
        self.assertEqual(evening_rides_with_high_total_amount_count, 0)

    def test_count_evening_rides_with_high_total_amount_no_high_total_amount(self):
        """Test count_evening_rides_with_high_total_amount() when there are total_amount values > average."""
        data = [
            ("2024-10-05 11:30:00", 10.0),
            ("2024-10-05 22:30:00", 10.0),
            ("2024-10-06 18:30:00", 10.0),
            ("2024-10-07 08:15:00", 10.0),
            ("2024-10-08 20:00:00", 10.0),
            ("2024-10-09 23:59:00", 10.0)
        ]

        columns = [c.pickup_datetime, c.total_amount]

        df = self.spark.createDataFrame(data, schema=columns)

        evening_rides_with_high_total_amount_count = fd_analysis.count_evening_rides_with_high_total_amount(df)
        self.assertIsInstance(evening_rides_with_high_total_amount_count, int)
        self.assertEqual(evening_rides_with_high_total_amount_count, 0)

    def test_count_cash_tips_above_average(self):
        """Test count_cash_tips_above_average() in general case."""
        data = [
            ("CSH", 5.0),
            ("CSH", 10.0),
            ("CSH", 12.0),
            ("CSH", 0.0),
            ("CRD", 5.0),
            ("CRD", 3.0),
            ("CRD", 12.0),
            ("CRD", 18.0)
        ]

        columns = [c.payment_type, c.tip_amount]

        df = self.spark.createDataFrame(data, schema=columns)

        cash_tips_above_average_count = fd_analysis.count_cash_tips_above_average(df)
        self.assertIsInstance(cash_tips_above_average_count, int)
        self.assertEqual(cash_tips_above_average_count, 2)

    def test_count_cash_tips_above_average_no_cash(self):
        """Test count_cash_tips_above_average() when there are no cash values."""
        data = [
            ("CRD", 5.0),
            ("CRD", 3.0),
            ("CRD", 12.0),
            ("CRD", 18.0)
        ]

        columns = [c.payment_type, c.tip_amount]

        df = self.spark.createDataFrame(data, schema=columns)

        cash_tips_above_average_count = fd_analysis.count_cash_tips_above_average(df)
        self.assertIsInstance(cash_tips_above_average_count, int)
        self.assertEqual(cash_tips_above_average_count, 0)

    def test_count_cash_tips_above_average_no_high_tips(self):
        """Test count_cash_tips_above_average() when no tip_amount > average."""
        data = [
            ("CSH", 5.0),
            ("CSH", 5.0),
            ("CSH", 5.0),
            ("CSH", 5.0),
            ("CRD", 5.0),
            ("CRD", 5.0),
            ("CRD", 5.0),
            ("CRD", 5.0)
        ]

        columns = [c.payment_type, c.tip_amount]

        df = self.spark.createDataFrame(data, schema=columns)

        cash_tips_above_average_count = fd_analysis.count_cash_tips_above_average(df)
        self.assertIsInstance(cash_tips_above_average_count, int)
        self.assertEqual(cash_tips_above_average_count, 0)

    def test_filter_weekday_credit_card_trips_with_high_tips(self):
        """Test filter_weekday_credit_card_trips_with_high_tips()."""
        data = [
            ("2024-10-01 18:30:00", "CSH", 5.0, 20.0),
            ("2024-10-02 12:30:00", "CRD", 5.0, 5.0),
            ("2024-10-02 10:00:00", "CRD", 12.0, 10.0),
            ("2024-10-03 20:30:00", "CRD", 18.0, 37.0),
            ("2024-10-05 22:30:00", "CSH", 0.0, 24.0),
            ("2024-10-06 18:30:00", "CRD", 15.0, 10.0),
            ("2024-10-07 19:00:00", "CRD", 3.0, 13.0),
            ("2024-10-07 20:00:00", "CRD", 3.0, 2.0),
            ("2024-10-08 21:00:00", "CSH", 60.0, 18.0),
            ("2024-10-08 20:00:00", "CRD", 60.0, 18.0),
            ("2024-10-11 15:30:00", "CRD", 10.0, 5.0),
        ]

        columns = [c.pickup_datetime, c.payment_type, c.tip_amount, c.fare_amount]

        df = self.spark.createDataFrame(data, schema=columns)

        weekday_credit_card_trips_with_high_tips_df = fd_analysis.filter_weekday_credit_card_trips_with_high_tips(df)

        expected_data = [
            ("2024-10-02 10:00:00", 12.0, 10.0),
            ("2024-10-07 20:00:00", 3.0, 2.0),
            ("2024-10-08 20:00:00", 60.0, 18.0),
            ("2024-10-11 15:30:00", 10.0, 5.0),
        ]

        columns = [c.pickup_datetime, c.tip_amount, c.fare_amount]

        expected_df = self.spark.createDataFrame(expected_data, schema=columns)

        diff_filtered = weekday_credit_card_trips_with_high_tips_df.exceptAll(expected_df)
        diff_expected = expected_df.exceptAll(weekday_credit_card_trips_with_high_tips_df)

        self.assertTrue(diff_filtered.isEmpty())
        self.assertTrue(diff_expected.isEmpty())


if __name__ == "__main__":
    unittest.main()

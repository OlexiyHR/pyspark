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

    def test_top_10_successful_drivers(self):
        """Test top_10_successful_drivers() for identifying top drivers by trips, income, and tips."""

        data = [
            ("medallion_1", "license_1", 20.0, 5.0),
            ("medallion_1", "license_1", 20.0, 5.0),
            ("medallion_1", "license_1", 20.0, 120.0),
            ("medallion_2", "license_2", 100.0, 25.0),
            ("medallion_3", "license_3", 50.0, 10.0),
            ("medallion_4", "license_4", 10.0, 3.0),
            ("medallion_5", "license_5", 75.0, 15.0),
            ("medallion_5", "license_5", 75.0, 15.0),
            ("medallion_6", "license_6", 60.0, 18.0),
            ("medallion_7", "license_7", 45.0, 12.0),
            ("medallion_7", "license_7", 45.0, 12.0),
            ("medallion_8", "license_8", 30.0, 6.0),
            ("medallion_9", "license_9", 90.0, 20.0),
            ("medallion_10", "license_10", 15.0, 4.0),
            ("medallion_11", "license_11", 120.0, 30.0),
            ("medallion_11", "license_11", 120.0, 30.0),
            ("medallion_11", "license_11", 120.0, 30.0),
            ("medallion_11", "license_11", 120.0, 30.0)
        ]

        columns = ["medallion", "hack_license", "total_amount", "tip_amount"]
        df = self.spark.createDataFrame(data, schema=columns)

        top_10_by_trips, top_10_by_trip_income, top_10_by_tips = fd_analysis.top_10_successful_drivers(df)

        expected_by_trips = [
            ("medallion_11", "license_11", 4),
            ("medallion_1", "license_1", 3),
            ("medallion_7", "license_7", 2),
            ("medallion_5", "license_5", 2),
            ("medallion_3", "license_3", 1),
            ("medallion_6", "license_6", 1),
            ("medallion_4", "license_4", 1),
            ("medallion_2", "license_2", 1),
            ("medallion_8", "license_8", 1),
            ("medallion_10", "license_10", 1),
        ]

        expected_by_income = [
            ("medallion_11", "license_11", 480.0),
            ("medallion_5", "license_5", 150.0),
            ("medallion_2", "license_2", 100.0),
            ("medallion_7", "license_7", 90.0),
            ("medallion_9", "license_9", 90.0),
            ("medallion_6", "license_6", 60.0),
            ("medallion_1", "license_1", 60.0),
            ("medallion_3", "license_3", 50.0),
            ("medallion_8", "license_8", 30.0),
            ("medallion_10", "license_10", 15.0),
        ]

        expected_by_tips = [
            ("medallion_1", "license_1", 130.0),
            ("medallion_11", "license_11", 120.0),
            ("medallion_5", "license_5", 30.0),
            ("medallion_2", "license_2", 25.0),
            ("medallion_7", "license_7", 24.0),
            ("medallion_9", "license_9", 20.0),
            ("medallion_6", "license_6", 18.0),
            ("medallion_3", "license_3", 10.0),
            ("medallion_8", "license_8", 6.0),
            ("medallion_10", "license_10", 4.0),
        ]

        expected_by_trips_df = self.spark.createDataFrame(expected_by_trips,
                                                          ["medallion", "hack_license", "total_trips"])
        expected_by_income_df = self.spark.createDataFrame(expected_by_income,
                                                           ["medallion", "hack_license", "total_trip_cost"])
        expected_by_tips_df = self.spark.createDataFrame(expected_by_tips, ["medallion", "hack_license", "total_tips"])

        self.assertTrue(top_10_by_trips.subtract(expected_by_trips_df).isEmpty())
        self.assertTrue(expected_by_trips_df.subtract(top_10_by_trips).isEmpty())

        self.assertTrue(top_10_by_trip_income.subtract(expected_by_income_df).isEmpty())
        self.assertTrue(expected_by_income_df.subtract(top_10_by_trip_income).isEmpty())

        self.assertTrue(top_10_by_tips.subtract(expected_by_tips_df).isEmpty())
        self.assertTrue(expected_by_tips_df.subtract(top_10_by_tips).isEmpty())

    def test_most_profitable_months_and_days(self):
        """Test most_profitable_months_and_days() with month and day names."""
        data = [
            ("2024-01-01 18:30:00", 20.0),
            ("2024-01-15 12:30:00", 15.0),
            ("2024-02-10 10:00:00", 30.0),
            ("2024-02-20 20:30:00", 25.0),
            ("2024-03-05 22:30:00", 50.0),
            ("2024-03-06 18:30:00", 60.0),
            ("2024-03-07 19:00:00", 45.0),
            ("2024-03-15 20:00:00", 35.0),
            ("2024-04-01 21:00:00", 70.0),
            ("2024-04-15 20:00:00", 80.0),
            ("2024-04-20 15:30:00", 90.0),
        ]

        columns = [c.pickup_datetime, c.total_amount]
        df = self.spark.createDataFrame(data, schema=columns)

        month_profit, day_of_week_profit = fd_analysis.most_profitable_months_and_days(df)

        expected_month_data = [
            ("April", 240.0),
            ("March", 190.0),
            ("February", 55.0),
            ("January", 35.0),
        ]
        expected_month_columns = ["month_name", "total_trip_cost"]
        expected_month_df = self.spark.createDataFrame(expected_month_data, schema=expected_month_columns)

        expected_day_data = [
            ("Monday", 185.0),
            ("Saturday", 120.0),
            ("Tuesday", 75.0),
            ("Wednesday", 60.0),
            ("Thursday", 45.0),
            ("Friday", 35.0),
        ]
        expected_day_columns = ["day_name", "total_trip_cost"]
        expected_day_df = self.spark.createDataFrame(expected_day_data, schema=expected_day_columns)

        diff_months = month_profit.exceptAll(expected_month_df)
        diff_months_expected = expected_month_df.exceptAll(month_profit)
        self.assertTrue(diff_months.isEmpty())
        self.assertTrue(diff_months_expected.isEmpty())

        diff_days = day_of_week_profit.exceptAll(expected_day_df)
        diff_days_expected = expected_day_df.exceptAll(day_of_week_profit)
        self.assertTrue(diff_days.isEmpty())
        self.assertTrue(diff_days_expected.isEmpty())

    def test_monthly_mta_tax_by_vendor(self):
        """Test monthly_mta_tax_by_vendor() function."""
        data = [
            ("vendor_1", "2024-01-15 08:00:00", 0.5),
            ("vendor_1", "2024-01-20 12:00:00", 0.5),
            ("vendor_2", "2024-02-10 10:00:00", 0.5),
            ("vendor_1", "2024-02-15 09:00:00", 0.0),
            ("vendor_2", "2024-02-25 18:00:00", 0.5),
            ("vendor_1", "2024-03-01 07:00:00", 0.5),
            ("vendor_2", "2024-03-20 11:00:00", 0.0),
        ]

        columns = ["vendor_id", "pickup_datetime", "mta_tax"]

        df = self.spark.createDataFrame(data, schema=columns)

        result_df = fd_analysis.monthly_mta_tax_by_vendor(df)

        expected_data = [
            ("vendor_2", "February", 1.0),
            ("vendor_1", "January", 1.0),
            ("vendor_1", "March", 0.5),
            ("vendor_1", "February", 0.0),
            ("vendor_2", "March", 0.0)
        ]

        expected_columns = ["vendor_id", "month_name", "total_mta_tax"]

        expected_df = self.spark.createDataFrame(expected_data, schema=expected_columns)

        result_df_sorted = result_df.orderBy("total_mta_tax")
        expected_df_sorted = expected_df.orderBy("total_mta_tax")

        diff_filtered = result_df_sorted.exceptAll(expected_df_sorted)
        diff_expected = expected_df_sorted.exceptAll(result_df_sorted)

        self.assertTrue(diff_filtered.isEmpty())
        self.assertTrue(diff_expected.isEmpty())


if __name__ == "__main__":
    unittest.main()

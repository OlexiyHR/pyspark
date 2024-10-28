import unittest
from pyspark.sql import SparkSession

import columns as c
from data_analysis.fare_data_analysis import average_card_payment_total


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

        avg_total = average_card_payment_total(df)

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

        avg_total = average_card_payment_total(df)

        self.assertEqual(avg_total, 0.0)


if __name__ == "__main__":
    unittest.main()

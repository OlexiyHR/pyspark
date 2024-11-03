from pyspark.sql import DataFrame
from pyspark.sql.functions import col, month, avg, count

import columns as c


def count_short_trips(df: DataFrame) -> int:
    """
    Filters trips with a distance shorter than 1 mile and returns the filtered DataFrame.
    Also prints the count of filtered trips.

    Args:
        df (DataFrame): Spark DataFrame containing trip data.

    Returns:
        DataFrame: A new DataFrame with only trips shorter than 1 mile.

    Examples:
        >>> short_trips_count = count_short_trips(trip_data_df)
    """
    return df.where(col(c.trip_distance) < 1).count()


def count_large_group_trips(df: DataFrame) -> int:
    """
    Filters trips with at least 6 passengers and returns the filtered DataFrame.
    Also prints the count of filtered trips.

    Args:
        df (DataFrame): Spark DataFrame containing trip data.

    Returns:
        DataFrame: A new DataFrame with only trips that have at least 6 passengers.

    Examples:
        >>> large_group_trips_count = count_large_group_trips(trip_data_df)
    """
    return df.where(col(c.passenger_count) >= 6).count()


def count_medium_duration_trips(df: DataFrame) -> int:
    """
    Filters trips with a duration between 30 minutes and 1 hour and returns the count of these trips.

    Args:
        df (DataFrame): Spark DataFrame containing trip data.

    Returns:
        int: The number of trips with a duration between 30 minutes (1800 seconds) and 1 hour (3600 seconds).

    Examples:
        >>> medium_duration_trips_count = count_medium_duration_trips(trip_data_df)
    """
    return df.where((col(c.trip_time_in_secs) >= 1800)
                     & (col(c.trip_time_in_secs) <= 3600)).count()


def jfk_airport_trips_with_four_passengers(df: DataFrame) -> DataFrame:
    """
    Filters trips that have exactly 4 passengers and used the JFK Airport rate code (rate_code = 2).
    Returns details of these trips.

    Args:
        df (DataFrame): Spark DataFrame containing trip data.

    Returns:
         DataFrame: A new DataFrame containing only trips with 4 passengers and the JFK Airport rate code.

    Examples:
        >>> jfk_trips_with_four_passengers = jfk_airport_trips_with_four_passengers(trip_data_df)
    """
    filtered_df = df.filter((col(c.passenger_count) == 4) & (col(c.rate_code) == 2))

    # Drop the passenger_count and rate_code columns, as they are the same and known.
    return filtered_df.drop(c.passenger_count, c.rate_code)


def trip_amounts_distribution_by_vendor(df: DataFrame) -> DataFrame:
    """
        Groups the trips by vendor ID and counts the rows for each vendor.

        Args:
            df (DataFrame): Spark DataFrame containing trip data.

        Returns:
             DataFrame: A new DataFrame containing the vendor IDs and the number of trips of that vendor.

        Examples:
            >>> vendor_trip_counts = trip_amounts_distribution_by_vendor(trip_data_df)
    """
    return df.groupBy(c.vendor_id).agg(count("*").alias("trip_count"))


def average_trip_speed_by_month(df: DataFrame) -> DataFrame:
    """
    Calculates the average trip speed for each month.

    Args:
        df (DataFrame): Spark DataFrame containing trip data.

    Returns:
        DataFrame: A new DataFrame with month and average trip speed.

    Examples:
        >>> avg_speed_per_month = average_trip_speed_by_month(trip_data_df)
    """
    month_column_name = "month"
    trip_speed_column_name = "trip_speed_in_miles_per_hour"
    average_trip_speed_column_name = "average_trip_speed_in_miles_per_hour"

    return (
        df.withColumn(month_column_name, month(c.pickup_datetime))
          .withColumn(trip_speed_column_name, col(c.trip_distance) / (col(c.trip_time_in_secs) / 3600))
          .groupBy(month_column_name)
          .agg(avg(trip_speed_column_name).alias(average_trip_speed_column_name))
    )


def passenger_count_distribution(df: DataFrame) -> DataFrame:
    """
    Calculates the distribution of the number of passengers per trip.

    Args:
        df (DataFrame): Spark DataFrame containing trip data.

    Returns:
        DataFrame: A new DataFrame with passenger count and the number of trips for each count.

    Examples:
        >>> passenger_distribution = passenger_count_distribution(trip_data_df)
    """
    return df.groupBy(c.passenger_count).agg(count("*").alias("trip_count"))

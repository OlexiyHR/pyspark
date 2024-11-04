from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as f

import columns as c
import settings as s


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
    return df.where(f.col(c.trip_distance) < 1).count()


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
    return df.where(f.col(c.passenger_count) >= 6).count()


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
    return df.where((f.col(c.trip_time_in_secs) >= 1800)
                     & (f.col(c.trip_time_in_secs) <= 3600)).count()


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
    filtered_df = df.filter((f.col(c.passenger_count) == 4) & (f.col(c.rate_code) == 2))

    # Drop the passenger_count and rate_code columns, as they are the same and known.
    return filtered_df.drop(c.passenger_count, c.rate_code)


def top_10_drivers_by_between_ride_distance(df):
    """
    Select the top 10 taxi drivers with the biggest total distance driven between rides.

    Note:
        Question 30.
        Distance in coordinates id converted to kilometers via haversine formula.

    Args:
        df (DataFrame): Trip data DataFrame to process.

    Returns:
        DataFrame: DataFrame with medallion, hack_license, total between-ride distance of top 10 drivers.
                   Sorted in descending order.
    """
    taxi_driver_window = Window.partitionBy(c.medallion, c.hack_license).orderBy(c.pickup_datetime)

    lagged_dropoff_coordinates_df = (df.withColumn("prev_dropoff_longitude", f.lag(c.dropoff_longitude).over(taxi_driver_window))
                                       .withColumn("prev_dropoff_latitude", f.lag(c.dropoff_latitude).over(taxi_driver_window))
                                    )

    df_with_radians = (lagged_dropoff_coordinates_df
                       .withColumn("pickup_longitude_rad", f.radians(c.pickup_longitude))
                       .withColumn("pickup_latitude_rad", f.radians(c.pickup_latitude))
                       .withColumn("prev_dropoff_longitude_rad", f.radians("prev_dropoff_longitude"))
                       .withColumn("prev_dropoff_latitude_rad", f.radians("prev_dropoff_latitude"))
                      )

    df_with_haversine_distance = df_with_radians.withColumn(
        "inter_ride_distance",
        2 * s.EARTH_RADIUS_KM
        * f.asin(
            f.sqrt(
                f.pow(
                    f.sin(
                        (f.col("pickup_latitude_rad") - f.col("prev_dropoff_latitude_rad")) / 2
                    ),
                    2
                )
                + f.cos(f.col("pickup_latitude_rad"))
                * f.cos(f.col("prev_dropoff_latitude_rad"))
                * f.pow(
                    f.sin(
                        (f.col("pickup_longitude_rad") - f.col("prev_dropoff_longitude_rad")) / 2
                    ),
                    2
                )
            )
        )
    )

    drivers_with_total_inter_ride_distance = (df_with_haversine_distance
                                              .groupBy(c.medallion, c.hack_license)
                                              .agg(f.sum("inter_ride_distance")
                                              .alias("total_inter_ride_distance"))
                                             )

    top_10_drivers = drivers_with_total_inter_ride_distance.orderBy(f.desc("total_inter_ride_distance")).limit(10)

    return top_10_drivers

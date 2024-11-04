from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as f
from pyspark.sql.functions import col, month, avg, count, rank, dayofweek, hour, when
from pyspark.sql.window import Window

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

    Examples:
        >>> top_10_drivers_df = top_10_drivers_by_between_ride_distance(df)
    """
    earth_radius_km = 6371.0

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

    double_earth_radius_km_col = f.lit(2 * earth_radius_km)

    df_with_haversine_distance = df_with_radians.withColumn(
        "inter_ride_distance",
        double_earth_radius_km_col
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


def get_driver_peak_load_days_in_december(df):
    """
    Find top 3 peak load days for each driver during December.

    Note:
        Question 33.

    Args:
        df (DataFrame): Trip data DataFrame to process .

    Returns:
        DataFrame: DataFrame with `medallion`, `hack_license`, `day`, and `trip_count`
                   for top three peak load days in December.

    Examples:
        >>> ranked_trip_counts_df = get_driver_peak_load_days_in_december(df)
    """
    december_df = df.filter(f.month(c.pickup_datetime) == 12)

    december_df = december_df.withColumn("day", f.to_date("pickup_datetime"))

    driver_hourly_trip_counts = (december_df
                                 .groupBy(c.medallion, c.hack_license, "day")
                                 .agg(f.count("*").alias("trip_count"))
                                )

    driver_window = Window.partitionBy(c.medallion, c.hack_license).orderBy(f.desc("trip_count"))

    ranked_trip_counts = (driver_hourly_trip_counts
                          .withColumn("rank", f.rank().over(driver_window))
                          .filter(f.col("rank").isin(1, 2, 3))
                          .select(c.medallion, c.hack_license, "day", "trip_count")
                         )

    return ranked_trip_counts

  
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


def short_trip_distribution_by_day_ranked(df: DataFrame) -> DataFrame:
    """
    Calculates the distribution of short trips (< 3 km) by day, ranked by trip count.

    Args:
        df (DataFrame): Spark DataFrame containing trip data with `pickup_datetime` and `trip_distance`.

    Returns:
        DataFrame: A new DataFrame with day, the count of short trips for each day, and a rank based on trip frequency.

    Examples:
        >>> short_trip_distribution = short_trip_distribution_by_day_ranked(trip_data_df)
    """
    day_of_week_name = "day_of_week"
    short_trip_count_name = "short_trip_count"
    ranking_name = "ranking"

    # 3 km ~ 1.864 miles
    three_km_in_miles = 1.864
    short_trips_df = df.filter(col(c.trip_distance) < three_km_in_miles)

    short_trips_df = short_trips_df.withColumn(day_of_week_name, dayofweek(c.pickup_datetime))

    daily_short_trip_counts = (
        short_trips_df.groupBy(day_of_week_name)
        .agg(count("*").alias(short_trip_count_name))
    )

    window_spec = Window.orderBy(col(short_trip_count_name).desc())

    return daily_short_trip_counts.withColumn(ranking_name, rank().over(window_spec)).orderBy(ranking_name)


def top_10_drivers_by_distance_per_month(df: DataFrame) -> DataFrame:
    """
    Finds the top 10 drivers for each month by total trip distance.

    Args:
        df (DataFrame): Spark DataFrame containing trip data with `pickup_datetime`, `hack_license`, and `trip_distance`.

    Returns:
        DataFrame: A new DataFrame with month, driver ID, total trip distance, and rank for the top 10 drivers per month.

    Examples:
        >>> top_drivers = top_10_drivers_by_distance_per_month(trip_data_df)
    """
    month_column_name = "month"
    total_distance_column_name = "total_distance"
    ranking_column_name = "ranking"

    monthly_driver_distances = (
        df.withColumn(month_column_name, month(c.pickup_datetime))
          .groupBy(month_column_name, c.hack_license)
          .agg(sum(c.trip_distance).alias(total_distance_column_name))
    )

    window_spec = Window.partitionBy(month_column_name).orderBy(col(total_distance_column_name).desc())

    return (
        monthly_driver_distances
        .withColumn(ranking_column_name, rank().over(window_spec))
        .filter(col(ranking_column_name) <= 10)
        .orderBy(month_column_name, ranking_column_name)
    )


def passenger_count_by_time_of_day(df: DataFrame) -> DataFrame:
    """
    Calculates the average number of passengers for different times of the day.

    Args:
        df (DataFrame): Spark DataFrame containing trip data.

    Returns:
        DataFrame: A new DataFrame with time of day intervals and the average passenger count.

    Examples:
        >>> avg_passenger_count = passenger_count_by_time_of_day(trip_data_df)
    """
    df_with_time_of_day = df.withColumn(
        "time_of_day",
        when((hour(c.pickup_datetime) >= 6) & (hour(c.pickup_datetime) < 12), "morning")
        .when((hour(c.pickup_datetime) >= 12) & (hour(c.pickup_datetime) < 18), "afternoon")
        .when((hour(c.pickup_datetime) >= 18) & (hour(c.pickup_datetime) < 24), "evening")
        .otherwise("night")
    )

    return df_with_time_of_day.groupBy("time_of_day").agg(
        avg(c.pickup_datetime).alias("average_passenger_count")).orderBy("time_of_day")

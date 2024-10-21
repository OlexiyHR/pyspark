"""
This module contains constants for column names used in fare data and trip data DataFrames to simplify column names
usage and reduce name typo error level.
"""


medallion = "medallion"
"""str: Unique taxi medallion number."""

hack_license = "hack_license"
"""str: Taxi driver's license unique number."""

vendor_id = "vendor_id"
"""str: Taxi service vendor name."""

pickup_datetime = "pickup_datetime"
"""str: Timestamp of passenger pickup."""

payment_type = "payment_type"
"""str: Payment type used to pay for the trip."""

fare_amount = "fare_amount"
"""str: Fare amount for the trip."""

surcharge = "surcharge"
"""str: Any additional surcharges for the trip."""

mta_tax = "mta_tax"
"""str: MTA tax for the trip."""

tip_amount = "tip_amount"
"""str: Tip amount given to the driven."""

tolls_amount = "tolls_amount"
"""str: Tolls amount for the trip."""

total_amount = "total_amount"
"""str: The total amount charged for the trip."""

rate_code = "rate_code"
"""int: Rate code for the trip."""

store_and_fwd_flag = "store_and_fwd_flag"
"""bool: Whether the trip data was stored before forwarding."""

dropoff_datetime = "dropoff_datetime"
"""timestamp: Date and time of the passenger dropoff."""

passenger_count = "passenger_count"
"""int: Number of passengers during the trip."""

trip_time_in_secs = "trip_time_in_secs"
"""int: Duration of the trip in seconds."""

trip_distance = "trip_distance"
"""double: Distance traveled during the trip."""

pickup_longitude = "pickup_longitude"
"""double: Longitude of the pickup location."""

pickup_latitude = "pickup_latitude"
"""double: Latitude of the pickup location."""

dropoff_longitude = "dropoff_longitude"
"""double: Longitude of the dropoff location."""

dropoff_latitude = "dropoff_latitude"
"""double: Latitude of the dropoff location."""

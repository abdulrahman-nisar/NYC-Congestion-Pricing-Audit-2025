"""
Configuration file for NYC Congestion Audit
"""
import os

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW = os.path.join(BASE_DIR, 'data', 'raw')
DATA_PROCESSED = os.path.join(BASE_DIR, 'data', 'processed')
DATA_AUDIT = os.path.join(BASE_DIR, 'data', 'audit')
OUTPUT_FIGURES = os.path.join(BASE_DIR, 'outputs', 'figures')

# Congestion Zone Definition
# Manhattan South of 60th Street LocationIDs
# Based on TLC Taxi Zone Lookup Table
CONGESTION_ZONE_IDS = [
    4, 12, 13, 24, 41, 42, 43, 45, 48, 50, 68, 74, 75, 79, 87, 88, 90,
    100, 103, 104, 105, 107, 113, 114, 116, 120, 125, 127, 128, 137,
    140, 141, 142, 143, 144, 148, 151, 152, 153, 158, 161, 162, 163,
    164, 166, 170, 186, 194, 202, 209, 211, 224, 229, 230, 231, 232,
    233, 234, 236, 237, 238, 239, 243, 244, 246, 249, 261, 262, 263
]

# Border zones (just north of 60th Street)
BORDER_ZONE_IDS = [
    140, 141, 142, 143, 158, 161, 162, 163, 164, 229, 230, 231, 232, 233
]

# Date when congestion pricing started
CONGESTION_START_DATE = '2025-01-05'

# Ghost Trip Detection Thresholds
MAX_SPEED_MPH = 65
MIN_TELEPORT_TIME_MINUTES = 1
MIN_TELEPORT_FARE = 20
MIN_STATIONARY_FARE = 0

# Weather API
WEATHER_API_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_PARAMS = {
    "latitude": 40.7831,  # Central Park
    "longitude": -73.9712,
    "start_date": "2025-01-01",
    "end_date": "2025-12-31",
    "daily": "precipitation_sum",
    "timezone": "America/New_York"
}

# Schema mapping for unified dataset
UNIFIED_SCHEMA = {
    'pickup_time': 'tpep_pickup_datetime',
    'dropoff_time': 'tpep_dropoff_datetime',
    'pickup_loc': 'PULocationID',
    'dropoff_loc': 'DOLocationID',
    'trip_distance': 'trip_distance',
    'fare': 'fare_amount',
    'total_amount': 'total_amount',
    'tip_amount': 'tip_amount',
    'congestion_surcharge': 'congestion_surcharge'
}

GREEN_SCHEMA = {
    'pickup_time': 'lpep_pickup_datetime',
    'dropoff_time': 'lpep_dropoff_datetime',
    'pickup_loc': 'PULocationID',
    'dropoff_loc': 'DOLocationID',
    'trip_distance': 'trip_distance',
    'fare': 'fare_amount',
    'total_amount': 'total_amount',
    'tip_amount': 'tip_amount',
    'congestion_surcharge': 'congestion_surcharge'
}
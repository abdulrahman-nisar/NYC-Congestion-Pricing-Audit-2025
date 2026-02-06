import requests
import pandas as pd
from src.config import WEATHER_API_URL, WEATHER_PARAMS
import dask.dataframe as dd


def fetch_weather_data():
    print("\n🌦️  Fetching weather data...")
    
    try:
        response = requests.get(WEATHER_API_URL, params=WEATHER_PARAMS, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        weather_df = pd.DataFrame({
            'date': pd.to_datetime(data['daily']['time']),
            'precipitation_mm': data['daily']['precipitation_sum']
        })
        
        print(f"✅ Fetched {len(weather_df)} days of weather data")
        return weather_df
    
    except Exception as e:
        print(f"❌ Error fetching weather data: {e}")
        return None


def calculate_rain_elasticity(ddf, weather_df):
    if weather_df is None:
        return None, None
    
    ddf_2025 = ddf[ddf['pickup_time'].dt.year == 2025]
    
    ddf_2025['date'] = ddf_2025['pickup_time'].dt.date
    daily_trips = ddf_2025.groupby('date').size().compute()
    
    trips_df = pd.DataFrame({
        'date': daily_trips.index,
        'trip_count': daily_trips.values
    })
    trips_df['date'] = pd.to_datetime(trips_df['date'])
    
    merged = pd.merge(trips_df, weather_df, on='date', how='inner')
    
    correlation = merged['trip_count'].corr(merged['precipitation_mm'])
    
    weather_df['month'] = weather_df['date'].dt.month
    wettest_month = weather_df.groupby('month')['precipitation_mm'].sum().idxmax()
    
    wettest_data = merged[merged['date'].dt.month == wettest_month]
    
    return correlation, wettest_data
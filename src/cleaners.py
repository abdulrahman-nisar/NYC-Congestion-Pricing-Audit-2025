import dask.dataframe as dd
import pandas as pd
from src.config import (
    MAX_SPEED_MPH, 
    MIN_TELEPORT_TIME_MINUTES, 
    MIN_TELEPORT_FARE,
    MIN_STATIONARY_FARE,
    DATA_AUDIT
)
import os
import numpy as np


def calculate_speed(ddf):
    ddf['trip_duration_hours'] = (
        (ddf['dropoff_time'] - ddf['pickup_time']).dt.total_seconds() / 3600
    )
    
    ddf['speed_mph'] = ddf['trip_distance'] / ddf['trip_duration_hours']
    ddf['speed_mph'] = ddf['speed_mph'].replace([np.inf, -np.inf], 0)
    ddf['speed_mph'] = ddf['speed_mph'].fillna(0)
    
    return ddf


def detect_ghost_trips(ddf):
    print("\n🔍 Detecting ghost trips...")
    
    # Calculate speed
    ddf = calculate_speed(ddf)
    
    # Calculate trip duration in minutes
    ddf['trip_duration_minutes'] = (
        (ddf['dropoff_time'] - ddf['pickup_time']).dt.total_seconds() / 60
    )
    
    # Define ghost trip rules
    print("   Applying ghost trip rules...")
    
    is_impossible_speed = ddf['speed_mph'] > MAX_SPEED_MPH
    
    is_teleporter = (
        (ddf['trip_duration_minutes'] < MIN_TELEPORT_TIME_MINUTES) & 
        (ddf['fare'] > MIN_TELEPORT_FARE)
    )
    
    is_stationary = (ddf['trip_distance'] == 0) & (ddf['fare'] > MIN_STATIONARY_FARE)
    
    is_ghost = is_impossible_speed | is_teleporter | is_stationary
    
    ddf['ghost_reason'] = 'Clean'
    ddf['ghost_reason'] = ddf['ghost_reason'].where(~is_stationary, 'Stationary Ride')
    ddf['ghost_reason'] = ddf['ghost_reason'].where(~is_teleporter, 'Teleporter')
    ddf['ghost_reason'] = ddf['ghost_reason'].where(~is_impossible_speed, 'Impossible Speed')
    
    clean_ddf = ddf[~is_ghost]
    ghost_ddf = ddf[is_ghost]
    
    print("   Computing statistics...")
    ghost_count = is_ghost.sum().compute()
    total_count = len(ddf)
    
    print(f"🚨 Found {ghost_count:,} ghost trips ({ghost_count/total_count*100:.2f}%)")
    
    print("💾 Saving ghost trips to audit log...")
    
    if ghost_count > 100000:
        print(f"   (Sampling 100,000 for storage efficiency)")
        ghost_sample = ghost_ddf.sample(frac=100000/ghost_count, random_state=42)
        ghost_trips = ghost_sample.compute()
    else:
        ghost_trips = ghost_ddf.compute()
    
    os.makedirs(DATA_AUDIT, exist_ok=True)
    ghost_trips.to_parquet(
        os.path.join(DATA_AUDIT, 'ghost_trips.parquet'),
        index=False
    )
    
    print(f"✅ Ghost trips saved: {len(ghost_trips):,} records")
    
    return clean_ddf, ghost_trips


def get_ghost_trip_summary(ghost_df):
    if ghost_df.empty:
        print("⚠️  No ghost trips found")
        return pd.DataFrame()
    
    try:
        summary = ghost_df.groupby('ghost_reason').agg({
            'fare': ['count', 'mean', 'sum'],
            'trip_distance': 'mean',
            'speed_mph': 'mean'
        }).round(2)
        
        return summary
    except Exception as e:
        print(f"⚠️  Could not generate summary: {e}")
        return pd.DataFrame()
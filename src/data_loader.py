import dask.dataframe as dd
import os
import glob
from src.config import DATA_RAW, UNIFIED_SCHEMA, GREEN_SCHEMA
import pandas as pd


def load_taxi_data(taxi_type='yellow', year=2025, month=None):
    if month:
        pattern = os.path.join(DATA_RAW, f'{taxi_type}_tripdata_{year}-{month:02d}.parquet')
    else:
        pattern = os.path.join(DATA_RAW, f'{taxi_type}_tripdata_{year}-*.parquet')
    
    files = sorted(glob.glob(pattern))
    
    if not files:
        print(f"⚠️  No {taxi_type} taxi files found for {year}-{month if month else 'all'}")
        return None
    
    print(f"📂 Loading {len(files)} files for {taxi_type} taxi {year}")
    
    ddf = dd.read_parquet(files, engine='pyarrow')
    
    schema = GREEN_SCHEMA if taxi_type == 'green' else UNIFIED_SCHEMA
    column_mapping = {v: k for k, v in schema.items()}
    
    existing_cols = [col for col in schema.values() if col in ddf.columns]
    ddf = ddf[existing_cols]
    ddf = ddf.rename(columns=column_mapping)
    ddf['taxi_type'] = taxi_type
    
    return ddf


def check_december_2025():
    dec_2025_yellow = glob.glob(os.path.join(DATA_RAW, 'yellow_tripdata_2025-12.parquet'))
    dec_2025_green = glob.glob(os.path.join(DATA_RAW, 'green_tripdata_2025-12.parquet'))
    
    if not dec_2025_yellow:
        print("⚠️  December 2025 Yellow taxi data missing")
    
    if not dec_2025_green:
        print("⚠️  December 2025 Green taxi data missing")
    
    if not dec_2025_yellow or not dec_2025_green:
        print("ℹ️  Analysis will proceed with available data")
        return False
    
    print("✅ December 2025 data found")
    return True


def load_all_data():
    dfs = []
    
    # Load 2024 data
    for taxi_type in ['yellow', 'green']:
        print(f"\n🚕 Loading {taxi_type} taxi 2024 data...")
        ddf_2024 = load_taxi_data(taxi_type, 2024)
        if ddf_2024 is not None:
            dfs.append(ddf_2024)
    
    # Load 2025 data
    for taxi_type in ['yellow', 'green']:
        print(f"\n🚕 Loading {taxi_type} taxi 2025 data...")
        ddf_2025 = load_taxi_data(taxi_type, 2025)
        if ddf_2025 is not None:
            dfs.append(ddf_2025)
    
    if not dfs:
        raise ValueError("❌ No data files found! Please download data first.")
    
    combined = dd.concat(dfs, axis=0, ignore_unknown_divisions=True)
    
    print(f"\n✅ Loaded data successfully")
    return combined
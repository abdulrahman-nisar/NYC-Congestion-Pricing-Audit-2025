import pandas as pd
import dask.dataframe as dd
from src.config import CONGESTION_ZONE_IDS, CONGESTION_START_DATE


def calculate_trip_volume_change(ddf):
    print("   Filtering Q1 data...")
    
    try:
        ddf['year'] = ddf['pickup_time'].dt.year
        ddf['month'] = ddf['pickup_time'].dt.month
        
        is_q1 = ddf['month'].isin([1, 2, 3])
        is_entering = (ddf['enters_zone'] == True)
        
        q1_entering = ddf[is_q1 & is_entering]
        
        print("   Counting trips by year and taxi type...")
        volume_counts = (
            q1_entering
            .groupby(['year', 'taxi_type'])
            .size()
            .compute()
        )
        
        if len(volume_counts) == 0:
            print("   ⚠️  No Q1 data found")
            return pd.DataFrame()
        
        volume_df = volume_counts.unstack(level='year', fill_value=0)
        
        if 2024 in volume_df.columns and 2025 in volume_df.columns:
            volume_df['pct_change'] = (
                (volume_df[2025] - volume_df[2024]) / 
                volume_df[2024].replace(0, 1)
            ) * 100
            
            volume_df.loc[volume_df[2024] == 0, 'pct_change'] = 0
        elif 2024 in volume_df.columns:
            volume_df['pct_change'] = -100
        elif 2025 in volume_df.columns:
            volume_df['pct_change'] = 100  # All trips new
        
        print(f"   ✅ Volume comparison complete")
        return volume_df
        
    except Exception as e:
        print(f"   ⚠️  Error calculating volume change: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def calculate_average_speed_by_time(ddf):
    print("   Filtering zone trips in Q1...")
    
    try:
        ddf['year'] = ddf['pickup_time'].dt.year
        ddf['month'] = ddf['pickup_time'].dt.month
        ddf['hour'] = ddf['pickup_time'].dt.hour
        ddf['day_of_week'] = ddf['pickup_time'].dt.dayofweek
        
        is_in_zone = (ddf['starts_in_zone'] == True)
        is_q1 = ddf['month'].isin([1, 2, 3])
        
        zone_q1 = ddf[is_in_zone & is_q1]
        
        print("   Computing average speeds by time...")
        speed_by_time = (
            zone_q1
            .groupby(['year', 'day_of_week', 'hour'])['speed_mph']
            .mean()
            .compute()
        )
        
        print(f"   ✅ Speed analysis complete")
        return speed_by_time
        
    except Exception as e:
        print(f"   ⚠️  Error calculating speeds: {e}")
        import traceback
        traceback.print_exc()
        return pd.Series()


def calculate_tip_vs_surcharge(ddf):
    print("   Calculating monthly statistics...")
    
    try:
        ddf['year_month'] = ddf['pickup_time'].dt.to_period('M')
        
        ddf['tip_pct'] = (ddf['tip_amount'] / ddf['fare'].replace(0, 0.01)) * 100
        
        valid_tips = (ddf['tip_pct'] >= 0) & (ddf['tip_pct'] <= 100) & (ddf['fare'] > 0)
        
        print("   Aggregating by month...")
        monthly_stats = (
            ddf[valid_tips]
            .groupby('year_month')
            .agg({
                'congestion_surcharge': 'mean',
                'tip_pct': 'mean',
                'fare': 'mean'
            })
            .compute()
        )
        
        print(f"   ✅ Monthly stats complete ({len(monthly_stats)} months)")
        return monthly_stats
        
    except Exception as e:
        print(f"   ⚠️  Error calculating tip analysis: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def calculate_total_revenue(ddf):
    print("   Calculating expected 2025 revenue...")
    
    try:
        SURCHARGE_PER_TRIP = 2.50
        
        eligible_mask = (
            (ddf['enters_zone'] == True) & 
            (ddf['pickup_time'] >= '2025-01-05')
        )
        
        trip_count = eligible_mask.sum().compute()
        
        if trip_count == 0:
            print("   ⚠️  No trips found entering zone")
            return {'total_revenue': 0.0, 'trip_count': 0, 'avg_surcharge': 0.0}
        
        expected_revenue = trip_count * SURCHARGE_PER_TRIP
        
        print(f"   ✅ Expected revenue calculation complete")
        print(f"      Eligible trips: {trip_count:,}")
        print(f"      Expected revenue: ${expected_revenue:,.2f}")
        print(f"      (Theoretical: $2.50 per trip)")
        
        return {
            'total_revenue': float(expected_revenue),
            'trip_count': int(trip_count),
            'avg_surcharge': SURCHARGE_PER_TRIP
        }
        
    except Exception as e:
        print(f"   ⚠️  Error: {e}")
        return {'total_revenue': 0.0, 'trip_count': 0, 'avg_surcharge': 0.0}
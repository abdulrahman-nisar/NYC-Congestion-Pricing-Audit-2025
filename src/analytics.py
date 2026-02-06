"""
Core analytics functions - MEMORY OPTIMIZED (COMPLETE)
"""
import pandas as pd
import dask.dataframe as dd
from src.config import CONGESTION_ZONE_IDS, CONGESTION_START_DATE


def calculate_trip_volume_change(ddf):
    """
    Compare Q1 2024 vs Q1 2025 trip volumes - OPTIMIZED
    """
    print("   Filtering Q1 data...")
    
    try:
        # Add temporal columns
        ddf['year'] = ddf['pickup_time'].dt.year
        ddf['month'] = ddf['pickup_time'].dt.month
        
        # Filter Q1 and entering zone
        is_q1 = ddf['month'].isin([1, 2, 3])
        is_entering = (ddf['enters_zone'] == True)
        
        q1_entering = ddf[is_q1 & is_entering]
        
        # Aggregate before computing
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
        
        # Reshape - unstack 'year' to columns
        volume_df = volume_counts.unstack(level='year', fill_value=0)
        
        # Calculate percentage change if both years exist
        if 2024 in volume_df.columns and 2025 in volume_df.columns:
            volume_df['pct_change'] = (
                (volume_df[2025] - volume_df[2024]) / 
                volume_df[2024].replace(0, 1)
            ) * 100
            
            # Set to 0 where 2024 was 0
            volume_df.loc[volume_df[2024] == 0, 'pct_change'] = 0
        elif 2024 in volume_df.columns:
            volume_df['pct_change'] = -100  # All trips lost
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
    """
    Calculate average speed heatmap - OPTIMIZED
    """
    print("   Filtering zone trips in Q1...")
    
    try:
        # Add temporal columns
        ddf['year'] = ddf['pickup_time'].dt.year
        ddf['month'] = ddf['pickup_time'].dt.month
        ddf['hour'] = ddf['pickup_time'].dt.hour
        ddf['day_of_week'] = ddf['pickup_time'].dt.dayofweek
        
        # Filter: in zone, Q1 only
        is_in_zone = (ddf['starts_in_zone'] == True)
        is_q1 = ddf['month'].isin([1, 2, 3])
        
        zone_q1 = ddf[is_in_zone & is_q1]
        
        # Aggregate before computing
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
    """
    Monthly tip vs surcharge analysis - OPTIMIZED
    """
    print("   Calculating monthly statistics...")
    
    try:
        # Add year-month
        ddf['year_month'] = ddf['pickup_time'].dt.to_period('M')
        
        # Calculate tip percentage (avoid division by zero)
        ddf['tip_pct'] = (ddf['tip_amount'] / ddf['fare'].replace(0, 0.01)) * 100
        
        # Filter outliers
        valid_tips = (ddf['tip_pct'] >= 0) & (ddf['tip_pct'] <= 100) & (ddf['fare'] > 0)
        
        # Aggregate by month
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
    """
    Calculate total surcharge revenue - MEMORY EFFICIENT
    
    FIX: Use .sum() on boolean mask instead of len()
    """
    print("   Calculating 2025 revenue...")
    
    try:
        # Add year column if not exists
        if 'year' not in ddf.columns:
            ddf['year'] = ddf['pickup_time'].dt.year
        
        # Create 2025 mask
        is_2025 = (ddf['year'] == 2025)
        
        # FIX: Count trips using sum on boolean mask (memory efficient)
        print("   Counting 2025 trips...")
        trip_count = is_2025.sum().compute()
        
        if trip_count == 0:
            print("   ⚠️  No 2025 trips found")
            return {
                'total_revenue': 0.0,
                'trip_count': 0,
                'avg_surcharge': 0.0
            }
        
        # Filter to 2025 trips for aggregations
        ddf_2025 = ddf[is_2025]
        
        # Calculate revenue statistics (aggregations are memory efficient)
        print("   Calculating revenue metrics...")
        total_revenue = ddf_2025['congestion_surcharge'].sum().compute()
        avg_surcharge = ddf_2025['congestion_surcharge'].mean().compute()
        
        print(f"   ✅ Revenue calculation complete")
        
        return {
            'total_revenue': float(total_revenue),
            'trip_count': int(trip_count),
            'avg_surcharge': float(avg_surcharge)
        }
        
    except Exception as e:
        print(f"   ⚠️  Error calculating revenue: {e}")
        import traceback
        traceback.print_exc()
        return {
            'total_revenue': 0.0,
            'trip_count': 0,
            'avg_surcharge': 0.0
        }
import pandas as pd
import dask.dataframe as dd
from src.config import CONGESTION_ZONE_IDS, BORDER_ZONE_IDS, CONGESTION_START_DATE


def identify_zone_trips(ddf):
    print("   Adding zone identification flags...")
    
    ddf['starts_in_zone'] = ddf['pickup_loc'].isin(CONGESTION_ZONE_IDS)
    
    ddf['ends_in_zone'] = ddf['dropoff_loc'].isin(CONGESTION_ZONE_IDS)
    
    ddf['enters_zone'] = (~ddf['starts_in_zone']) & (ddf['ends_in_zone'])
    
    ddf['dropoff_at_border'] = ddf['dropoff_loc'].isin(BORDER_ZONE_IDS)
    
    print("   ✅ Zone flags added")
    return ddf


def calculate_compliance_rate(ddf):
    print("   Filtering trips entering zone after Jan 5, 2025...")
    
    try:
        enters_zone_mask = (ddf['enters_zone'] == True)
        after_date_mask = (ddf['pickup_time'] >= CONGESTION_START_DATE)
        combined_mask = enters_zone_mask & after_date_mask
        
        print("   Counting trips entering zone...")
        total_entering = combined_mask.sum().compute()
        
        if total_entering == 0:
            print("   ⚠️  No trips found entering zone after congestion pricing start")
            return 0.0, pd.Series()
        
        print(f"   Found {total_entering:,} trips entering zone")
        
        print("   Checking compliance...")
        entering_with_surcharge = (combined_mask & (ddf['congestion_surcharge'] > 0)).sum().compute()
        
        compliance_rate = (entering_with_surcharge / total_entering) * 100
        
        print(f"   Compliance: {entering_with_surcharge:,} / {total_entering:,} = {compliance_rate:.2f}%")
        
        print("   Identifying top leakage locations...")
        
        non_compliant_mask = combined_mask & (ddf['congestion_surcharge'] == 0)
        
        leakage_by_location = (
            ddf[non_compliant_mask]
            .groupby('pickup_loc')
            .size()
            .compute()
        )
        
        if len(leakage_by_location) > 0:
            top_leakage = leakage_by_location.nlargest(3)
            print(f"   Found leakage in {len(leakage_by_location)} locations")
        else:
            top_leakage = pd.Series()
            print("   No leakage detected")
        
        return compliance_rate, top_leakage
    
    except Exception as e:
        print(f"   ⚠️  Error calculating compliance: {e}")
        import traceback
        traceback.print_exc()
        return 0.0, pd.Series()


def analyze_border_effect(ddf):
    print("   Analyzing border dropoff patterns...")
    
    try:
        ddf['year'] = ddf['pickup_time'].dt.year
        ddf['month'] = ddf['pickup_time'].dt.month
        
        is_q1 = ddf['month'].isin([1, 2, 3])
        is_border = (ddf['dropoff_at_border'] == True)
        
        print("   Filtering Q1 border dropoffs...")
        q1_border = ddf[is_q1 & is_border]
        
        print("   Computing border dropoff counts by year...")
        border_counts = (
            q1_border
            .groupby(['year', 'dropoff_loc'])
            .size()
            .compute()
        )
        
        if len(border_counts) == 0:
            print("   ⚠️  No border dropoffs found in Q1")
            return pd.DataFrame()
        
        print(f"   Found {len(border_counts)} year-location combinations")
        
        border_comparison = border_counts.unstack(level=0, fill_value=0)
        
        if 2024 in border_comparison.columns and 2025 in border_comparison.columns:
            border_comparison['pct_change'] = (
                (border_comparison[2025] - border_comparison[2024]) / 
                border_comparison[2024].replace(0, 1)
            ) * 100
            
            border_comparison.loc[border_comparison[2024] == 0, 'pct_change'] = 0
            
            print(f"   ✅ Analyzed {len(border_comparison)} border zones")
        else:
            print("   ⚠️  Missing year data for comparison")
            if 2024 not in border_comparison.columns:
                print("      - No 2024 Q1 data found")
            if 2025 not in border_comparison.columns:
                print("      - No 2025 Q1 data found")
            border_comparison['pct_change'] = 0
        
        return border_comparison
    
    except Exception as e:
        print(f"   ⚠️  Error analyzing border effect: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
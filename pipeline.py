import warnings
warnings.filterwarnings('ignore')

from src.data_loader import load_all_data, check_december_2025
from src.cleaners import detect_ghost_trips, get_ghost_trip_summary
from src.geospatial import (
    identify_zone_trips, 
    calculate_compliance_rate,
    analyze_border_effect
)
from src.analytics import (
    calculate_trip_volume_change,
    calculate_average_speed_by_time,
    calculate_tip_vs_surcharge,
    calculate_total_revenue
)
from src.weather import fetch_weather_data, calculate_rain_elasticity
from src.visualizations import (
    plot_border_effect,
    plot_speed_heatmap,
    plot_tip_vs_surcharge,
    plot_rain_elasticity,
    plot_trip_volume_change
)
import pandas as pd
import os
from src.config import OUTPUT_FIGURES, DATA_PROCESSED

os.makedirs(OUTPUT_FIGURES, exist_ok=True)
os.makedirs(DATA_PROCESSED, exist_ok=True)


def main():
    print("=" * 60)
    print("🚖 NYC CONGESTION PRICING AUDIT 2025")
    print("=" * 60)
    
    # PHASE 1: DATA ENGINEERING
    print("\n" + "="*60)
    print("PHASE 1: BIG DATA ENGINEERING")
    print("="*60)
    
    check_december_2025()
    
    print("\n📥 Loading taxi trip data...")
    ddf = load_all_data()
    
    clean_ddf, ghost_df = detect_ghost_trips(ddf)
    
    if not ghost_df.empty:
        print("\n📊 Ghost Trip Summary:")
        print(get_ghost_trip_summary(ghost_df))
    
    print("\n" + "="*60)
    print("PHASE 2: CONGESTION ZONE IMPACT ANALYSIS")
    print("="*60)
    
    print("\n🗺️  Identifying congestion zone trips...")
    clean_ddf = identify_zone_trips(clean_ddf)
    
    print("\n📋 Calculating surcharge compliance...")
    compliance_rate, top_leakage = calculate_compliance_rate(clean_ddf)
    print(f"Compliance Rate: {compliance_rate:.2f}%")
    if not top_leakage.empty:
        print("\nTop 3 Pickup Locations with Missing Surcharges:")
        print(top_leakage)
    
    print("\n📉 Analyzing trip volume changes (Q1 2024 vs Q1 2025)...")
    volume_df = calculate_trip_volume_change(clean_ddf)
    print(volume_df)
    
    print("\n🚧 Analyzing border effect...")
    border_comparison = analyze_border_effect(clean_ddf)
    
    print("\n" + "="*60)
    print("PHASE 3: VISUAL AUDIT")
    print("="*60)
    
    print("\n📊 Generating visualizations...")
    
    if not border_comparison.empty:
        plot_border_effect(border_comparison)
    
    print("\n⏱️  Calculating average speeds...")
    speed_pivot = calculate_average_speed_by_time(clean_ddf)
    plot_speed_heatmap(speed_pivot)
    
    print("\n💰 Analyzing tip crowding out effect...")
    monthly_stats = calculate_tip_vs_surcharge(clean_ddf)
    plot_tip_vs_surcharge(monthly_stats)
    
    plot_trip_volume_change(volume_df)
    
    print("\n" + "="*60)
    print("PHASE 4: RAIN TAX ANALYSIS")
    print("="*60)
    
    weather_df = fetch_weather_data()
    
    if weather_df is not None:

        print("\n��️  Calculating rain elasticity...")
        correlation, wettest_data = calculate_rain_elasticity(clean_ddf, weather_df)
        
        if correlation is not None:
            print(f"Rain Elasticity (Correlation): {correlation:.4f}")
            
            if correlation > 0.3:
                elasticity = "ELASTIC (Demand increases with rain)"
            elif correlation < -0.3:
                elasticity = "ELASTIC (Demand decreases with rain)"
            else:
                elasticity = "INELASTIC (Weak relationship)"
            
            print(f"Interpretation: {elasticity}")
            
            if wettest_data is not None and not wettest_data.empty:
                plot_rain_elasticity(wettest_data)
    
    print("\n" + "="*60)
    print("PHASE 5: REVENUE ANALYSIS")
    print("="*60)
    
    print("\n💵 Calculating total 2025 surcharge revenue...")
    revenue_stats = calculate_total_revenue(clean_ddf)
    print(f"Total Revenue: ${revenue_stats['total_revenue']:,.2f}")
    print(f"Average Surcharge per Trip: ${revenue_stats['avg_surcharge']:.2f}")
    
    print("\n💾 Saving summary statistics...")
    summary_stats = {
        'total_revenue': revenue_stats['total_revenue'],
        'avg_surcharge': revenue_stats['avg_surcharge'],
        'compliance_rate': compliance_rate,
        'ghost_trip_count': len(ghost_df),
        'rain_elasticity': correlation if correlation is not None else 0
    }
    
    summary_df = pd.DataFrame([summary_stats])
    summary_df.to_csv(os.path.join(DATA_PROCESSED, 'summary_statistics.csv'), index=False)
    
    print("\n" + "="*60)
    print("✅ PIPELINE COMPLETED SUCCESSFULLY")
    print("="*60)
    print(f"\n📁 All outputs saved to: {OUTPUT_FIGURES}")
    print("\n🎯 Next Steps:")
    print("   1. Run dashboard: streamlit run dashboard.py")
    print("   2. Review figures in outputs/figures/")
    print("   3. Generate audit_report.pdf")


if __name__ == "__main__":
    main()
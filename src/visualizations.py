"""
Visualization functions - FIXED
"""
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from src.config import OUTPUT_FIGURES
import os

# Set style
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)


def plot_border_effect(border_comparison):
    """
    Plot % change in dropoffs at border zones
    """
    if border_comparison.empty:
        print("⚠️  No border data to plot")
        return
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    border_sorted = border_comparison.sort_values('pct_change', ascending=False)
    
    colors = ['red' if x > 0 else 'green' for x in border_sorted['pct_change']]
    
    ax.barh(range(len(border_sorted)), border_sorted['pct_change'], color=colors)
    ax.set_yticks(range(len(border_sorted)))
    ax.set_yticklabels([f"Zone {idx}" for idx in border_sorted.index])
    ax.set_xlabel('% Change in Dropoffs (Q1 2024 vs Q1 2025)')
    ax.set_title('Border Effect: Are Passengers Avoiding the Toll?', fontsize=14, fontweight='bold')
    ax.axvline(0, color='black', linewidth=0.8)
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_FIGURES, 'border_effect.png'), dpi=300)
    print(f"✅ Saved: border_effect.png")
    plt.close()


def plot_speed_heatmap(speed_pivot):
    """
    Plot velocity heatmap for 2024 and 2025
    """
    if speed_pivot.empty:
        print("⚠️  No speed data to plot")
        return
    
    days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    
    for year in [2024, 2025]:
        try:
            if year not in speed_pivot.index.get_level_values(0):
                print(f"⚠️  No data for {year}")
                continue
            
            data_year = speed_pivot.xs(year, level='year')
            pivot_table = data_year.unstack(level='hour', fill_value=0)
            
            fig, ax = plt.subplots(figsize=(14, 6))
            sns.heatmap(
                pivot_table, 
                cmap='RdYlGn', 
                annot=False, 
                fmt='.1f',
                cbar_kws={'label': 'Avg Speed (MPH)'},
                yticklabels=days,
                ax=ax
            )
            ax.set_title(f'Q1 {year}: Average Trip Speed in Congestion Zone', 
                         fontsize=14, fontweight='bold')
            ax.set_xlabel('Hour of Day')
            ax.set_ylabel('Day of Week')
            
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_FIGURES, f'speed_heatmap_{year}.png'), dpi=300)
            print(f"✅ Saved: speed_heatmap_{year}.png")
            plt.close()
        except Exception as e:
            print(f"⚠️  Error plotting {year} heatmap: {e}")


def plot_tip_vs_surcharge(monthly_stats):
    """
    Dual-axis chart: Surcharge vs Tip Percentage
    """
    if monthly_stats.empty:
        print("⚠️  No tip/surcharge data to plot")
        return
    
    fig, ax1 = plt.subplots(figsize=(14, 6))
    
    x = range(len(monthly_stats))
    labels = [str(idx) for idx in monthly_stats.index]
    
    # Bar chart: Average surcharge
    ax1.bar(x, monthly_stats['congestion_surcharge'], color='steelblue', alpha=0.7, label='Avg Surcharge ($)')
    ax1.set_xlabel('Month')
    ax1.set_ylabel('Average Surcharge ($)', color='steelblue')
    ax1.tick_params(axis='y', labelcolor='steelblue')
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, rotation=45)
    
    # Line chart: Tip percentage
    ax2 = ax1.twinx()
    ax2.plot(x, monthly_stats['tip_pct'], color='darkred', marker='o', linewidth=2, label='Avg Tip %')
    ax2.set_ylabel('Average Tip (%)', color='darkred')
    ax2.tick_params(axis='y', labelcolor='darkred')
    
    plt.title('Tip "Crowding Out" Effect: Surcharge vs Tips', fontsize=14, fontweight='bold')
    fig.tight_layout()
    plt.savefig(os.path.join(OUTPUT_FIGURES, 'tip_vs_surcharge.png'), dpi=300)
    print(f"✅ Saved: tip_vs_surcharge.png")
    plt.close()


def plot_rain_elasticity(wettest_data):
    """
    Scatter plot: Daily trips vs precipitation
    """
    if wettest_data is None or wettest_data.empty:
        print("⚠️  No weather data to plot")
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.scatter(wettest_data['precipitation_mm'], wettest_data['trip_count'], 
               alpha=0.6, s=50, color='navy')
    
    # Add trend line
    z = np.polyfit(wettest_data['precipitation_mm'], wettest_data['trip_count'], 1)
    p = np.poly1d(z)
    ax.plot(wettest_data['precipitation_mm'], 
            p(wettest_data['precipitation_mm']), 
            "r--", linewidth=2, label='Trend Line')
    
    ax.set_xlabel('Precipitation (mm)')
    ax.set_ylabel('Daily Trip Count')
    ax.set_title('Rain Elasticity of Demand (Wettest Month 2025)', 
                 fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_FIGURES, 'rain_elasticity.png'), dpi=300)
    print(f"✅ Saved: rain_elasticity.png")
    plt.close()


def plot_trip_volume_change(volume_df):
    """
    Bar chart comparing Q1 trip volumes - FIXED
    """
    # FIX: Check if DataFrame is valid
    if volume_df is None or volume_df.empty:
        print("⚠️  No volume data to plot")
        return
    
    print(f"   DEBUG: volume_df shape: {volume_df.shape}")
    print(f"   DEBUG: volume_df columns: {volume_df.columns.tolist()}")
    print(f"   DEBUG: volume_df index: {volume_df.index.tolist()}")
    
    try:
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Check if we have both years
        has_2024 = 2024 in volume_df.columns
        has_2025 = 2025 in volume_df.columns
        
        if not has_2024 and not has_2025:
            print("⚠️  No 2024 or 2025 data in volume_df")
            return
        
        # Plot available years
        if has_2024 and has_2025:
            volume_df[[2024, 2025]].plot(kind='bar', ax=ax, color=['#1f77b4', '#ff7f0e'])
            legend_labels = ['2024', '2025']
        elif has_2024:
            volume_df[[2024]].plot(kind='bar', ax=ax, color=['#1f77b4'])
            legend_labels = ['2024']
        else:
            volume_df[[2025]].plot(kind='bar', ax=ax, color=['#ff7f0e'])
            legend_labels = ['2025']
        
        ax.set_title('Q1 Trip Volume: Yellow vs Green Taxis Entering Zone', 
                     fontsize=14, fontweight='bold')
        ax.set_xlabel('Taxi Type')
        ax.set_ylabel('Number of Trips')
        ax.set_xticklabels(volume_df.index, rotation=0)
        ax.legend(legend_labels)
        
        # Add percentage change annotations if available
        if 'pct_change' in volume_df.columns and has_2024 and has_2025:
            for i, (idx, row) in enumerate(volume_df.iterrows()):
                max_val = max(row[2024], row[2025])
                ax.text(i, max_val * 1.05, 
                        f"{row['pct_change']:+.1f}%", 
                        ha='center', fontweight='bold', 
                        color='red' if row['pct_change'] < 0 else 'green')
        
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_FIGURES, 'trip_volume_change.png'), dpi=300)
        print(f"✅ Saved: trip_volume_change.png")
        plt.close()
        
    except Exception as e:
        print(f"⚠️  Error plotting trip volume: {e}")
        import traceback
        traceback.print_exc()
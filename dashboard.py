import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from PIL import Image
import os
from src.config import OUTPUT_FIGURES, DATA_PROCESSED

st.set_page_config(
    page_title="NYC Congestion Audit 2025",
    page_icon="🚖",
    layout="wide"
)

st.title("🚖 NYC Congestion Pricing Audit 2025")
st.markdown("**Interactive Dashboard** | Data-Driven Policy Analysis")

try:
    summary_df = pd.read_csv(os.path.join(DATA_PROCESSED, 'summary_statistics.csv'))
    summary = summary_df.iloc[0]
except:
    st.error("❌ Please run pipeline.py first to generate data!")
    st.stop()

st.sidebar.header("📊 Key Metrics")
st.sidebar.metric("Total Revenue", f"${summary['total_revenue']:,.0f}")
st.sidebar.metric("Compliance Rate", f"{summary['compliance_rate']:.1f}%")
st.sidebar.metric("Ghost Trips Detected", f"{summary['ghost_trip_count']:,.0f}")
st.sidebar.metric("Rain Elasticity", f"{summary['rain_elasticity']:.3f}")

tab1, tab2, tab3, tab4 = st.tabs([
    "🗺️ The Map", 
    "⚡ The Flow", 
    "💰 The Economics", 
    "🌧️ The Weather"
])

with tab1:
    st.header("Border Effect Analysis")
    st.markdown("**Hypothesis**: Passengers end trips just outside the zone to avoid toll")
    
    img_path = os.path.join(OUTPUT_FIGURES, 'border_effect.png')
    if os.path.exists(img_path):
        img = Image.open(img_path)
        st.image(img, use_container_width=True)
    else:
        st.warning("⚠️ Chart not found. Run pipeline.py first.")
    
    st.markdown("### Insights")
    st.markdown("""
    - **Red bars**: Increase in dropoffs (possible toll avoidance)
    - **Green bars**: Decrease in dropoffs
    - Focus on zones with >20% increase
    """)

with tab2:
    st.header("Congestion Velocity Heatmaps")
    st.markdown("**Question**: Did the toll actually speed up traffic?")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Q1 2024 (Before)")
        img_2024 = os.path.join(OUTPUT_FIGURES, 'speed_heatmap_2024.png')
        if os.path.exists(img_2024):
            st.image(Image.open(img_2024), use_container_width=True)
    
    with col2:
        st.subheader("Q1 2025 (After)")
        img_2025 = os.path.join(OUTPUT_FIGURES, 'speed_heatmap_2025.png')
        if os.path.exists(img_2025):
            st.image(Image.open(img_2025), use_container_width=True)
    
    st.markdown("### Analysis")
    st.markdown("""
    - **Darker green** = Higher speeds
    - Compare peak hours (8-10am, 5-7pm)
    - Did congestion pricing improve flow?
    """)

with tab3:
    st.header("Tip Crowding Out Effect")
    st.markdown("**Hypothesis**: Higher tolls reduce tips for drivers")
    
    img_tip = os.path.join(OUTPUT_FIGURES, 'tip_vs_surcharge.png')
    if os.path.exists(img_tip):
        st.image(Image.open(img_tip), use_container_width=True)
    else:
        st.warning("⚠️ Chart not found.")
    
    st.markdown("### Key Questions")
    st.markdown("""
    - Does tip percentage decline when surcharge increases?
    - Is the relationship statistically significant?
    - Policy implication: Should drivers be compensated?
    """)
    
    st.subheader("Trip Volume Change")
    img_volume = os.path.join(OUTPUT_FIGURES, 'trip_volume_change.png')
    if os.path.exists(img_volume):
        st.image(Image.open(img_volume), use_container_width=True)

with tab4:
    st.header("Rain Elasticity of Demand")
    st.markdown("**Question**: How does weather affect taxi demand?")
    
    img_rain = os.path.join(OUTPUT_FIGURES, 'rain_elasticity.png')
    if os.path.exists(img_rain):
        st.image(Image.open(img_rain), use_container_width=True)
    else:
        st.warning("⚠️ Chart not found.")
    
    st.markdown("### Elasticity Interpretation")
    
    elasticity = summary['rain_elasticity']
    
    if elasticity > 0.3:
        st.success(f"✅ **ELASTIC**: Strong positive correlation ({elasticity:.3f})")
        st.markdown("More rain = More taxi demand")
    elif elasticity < -0.3:
        st.error(f"❌ **ELASTIC**: Strong negative correlation ({elasticity:.3f})")
        st.markdown("More rain = Less taxi demand (unexpected!)")
    else:
        st.info(f"⚪ **INELASTIC**: Weak correlation ({elasticity:.3f})")
        st.markdown("Rain has minimal impact on demand")

st.markdown("---")
st.markdown("**Data Source**: NYC TLC Trip Record Data | **Analysis Period**: 2024-2025")
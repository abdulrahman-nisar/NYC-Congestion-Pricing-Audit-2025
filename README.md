# 🚖 NYC Congestion Pricing Audit 2025

## 📌 Project Overview
This project analyzes the impact of Manhattan's Congestion Relief Zone Toll (implemented Jan 5, 2025) on the taxi industry using **Big Data** techniques.

## 🎯 Research Questions
1. **Did it work?** (Traffic flow and revenue)
2. **Is it fair?** (Impact on driver tips)
3. **Is it watertight?** (Fraud detection)

---

## 📁 Project Structure
```
nyc-congestion-audit/
├── data/
│   ├── raw/              # Put downloaded parquet files here
│   ├── processed/
│   └── audit/
├── src/
│   ├── config.py
│   ├── data_loader.py
│   ├── cleaners.py
│   ├── geospatial.py
│   ├── analytics.py
│   ├── weather.py
│   └── visualizations.py
├── pipeline.py           # Main ETL script
├── dashboard.py          # Streamlit dashboard
├── outputs/figures/
└── requirements.txt
```

---

## 🚀 Setup Instructions

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Download Data
Go to: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Download these files to `data/raw/`:
- yellow_tripdata_2024-01.parquet to 2024-12.parquet
- yellow_tripdata_2025-01.parquet to 2025-12.parquet
- green_tripdata_2024-01.parquet to 2024-12.parquet
- green_tripdata_2025-01.parquet to 2025-12.parquet

### 3. Run Pipeline
```bash
python pipeline.py
```

This will:
- Load and clean data
- Detect ghost trips
- Perform all analyses
- Generate visualizations

### 4. Launch Dashboard
```bash
streamlit run dashboard.py
```

---

## 📊 Key Outputs

### Visualizations (in `outputs/figures/`)
1. `border_effect.png` - Toll avoidance analysis
2. `speed_heatmap_2024.png` & `speed_heatmap_2025.png` - Traffic flow
3. `tip_vs_surcharge.png` - Economic impact on drivers
4. `rain_elasticity.png` - Weather demand analysis
5. `trip_volume_change.png` - Volume comparison

### Data Files
- `data/audit/ghost_trips.parquet` - Suspicious trips
- `data/processed/summary_statistics.csv` - Key metrics

---

## 🧪 Technical Details

### Big Data Tools
- **Dask**: Lazy evaluation for large datasets
- **PyArrow**: Fast parquet reading
- **Aggregation First Rule**: All groupby operations in Dask before Pandas

### Ghost Trip Detection Rules
1. **Impossible Physics**: Speed > 65 MPH
2. **The Teleporter**: <1 min trip but fare >$20
3. **The Stationary Ride**: Distance = 0 but fare > 0

---

## 📈 Sample Results

**Expected Metrics:**
- Total 2025 Surcharge Revenue: ~$XXX million
- Compliance Rate: ~XX%
- Rain Elasticity: 0.XX (elastic/inelastic)
- Ghost Trips: ~X% of total

---

## 👤 Author
**Your Name**
- Data Science Assignment
- January 2026

## 📄 License
MIT License
```
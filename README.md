# NYC Congestion Pricing Audit 2025

## Project Overview
This project analyzes the impact of Manhattan's Congestion Relief Zone Toll (implemented January 5, 2025) on the NYC taxi industry using Big Data processing techniques. The analysis covers yellow and green taxi trip data from 2024-2025 to evaluate the effectiveness, fairness, and compliance of the congestion pricing policy.

## Research Questions
1. **Effectiveness**: Did traffic flow improve and what revenue was generated?
2. **Fairness**: How did the surcharge impact driver tips and income?
3. **Compliance**: What is the surcharge compliance rate and fraud detection?
4. **Behavioral Changes**: Did passengers avoid toll zones and how does weather affect demand?

## Project Structure
```
nyc_congestion_audit/
├── data/
│   ├── raw/                     Taxi trip parquet files (2024-2025)
│   ├── processed/               Cleaned data and summary statistics
│   └── audit/                   Ghost trip detection results
├── src/
│   ├── __init__.py             Package initialization
│   ├── config.py               Configuration settings and constants
│   ├── data_loader.py          Dask-based data loading functions
│   ├── cleaners.py             Ghost trip detection and data cleaning
│   ├── geospatial.py           Congestion zone analysis functions
│   ├── analytics.py            Core analytics calculations
│   ├── weather.py              Weather data integration
│   └── visualizations.py       Matplotlib/Seaborn plotting functions
├── outputs/
│   └── figures/                Generated visualizations
├── pipeline.py                 Main ETL and analysis pipeline
├── dashboard.py                Streamlit interactive dashboard
└── README.md                   Project documentation

```

## Setup Instructions

### Prerequisites
- Python 3.8 or higher
- 10GB+ available disk space

### Installation
```bash
pip install dask[complete] pandas numpy matplotlib seaborn streamlit requests pyarrow pillow
```

### Data Acquisition
Download NYC TLC Trip Record Data from: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Required files in `data/raw/`:
- yellow_tripdata_2024-01.parquet through 2024-12.parquet
- yellow_tripdata_2025-01.parquet through 2025-12.parquet
- green_tripdata_2024-01.parquet through 2024-12.parquet
- green_tripdata_2025-01.parquet through 2025-12.parquet

### Execution
Run the complete analysis pipeline:
```bash
python pipeline.py
```

Launch the interactive dashboard:
```bash
streamlit run dashboard.py
```

## Analysis Components

### Phase 1: Data Engineering
- Lazy loading of large parquet files using Dask
- Ghost trip detection using physics-based rules
- Data validation and anomaly filtering

### Phase 2: Congestion Zone Impact
- Zone trip identification based on location IDs
- Surcharge compliance rate calculation
- Border effect analysis for toll avoidance patterns
- Q1 2024 vs Q1 2025 trip volume comparison

### Phase 3: Visual Audit
- Border dropoff pattern visualizations
- Speed heatmaps by hour and day
- Tip vs surcharge correlation analysis
- Trip volume change charts

### Phase 4: Weather Integration
- Open-Meteo API weather data integration
- Rain elasticity of demand calculation
- Correlation analysis between precipitation and trip counts

### Phase 5: Revenue Analysis
- Total surcharge revenue estimation
- Average surcharge per trip calculation
- Summary statistics export

## Output Files

### Visualizations
- `border_effect.png` - Percentage change in border zone dropoffs
- `speed_heatmap_2024.png` - Q1 2024 average speeds by time
- `speed_heatmap_2025.png` - Q1 2025 average speeds by time
- `tip_vs_surcharge.png` - Monthly tip crowding out effect
- `rain_elasticity.png` - Trip demand vs precipitation scatter plot
- `trip_volume_change.png` - Yellow vs green taxi volume comparison

### Data Outputs
- `data/audit/ghost_trips.parquet` - Detected fraudulent trips
- `data/processed/summary_statistics.csv` - Key metrics summary

## Technical Implementation

### Big Data Processing
- **Dask DataFrames**: Parallel computation for datasets exceeding memory
- **PyArrow Engine**: Fast parquet file reading
- **Lazy Evaluation**: Operations deferred until compute() called
- **Aggregation-First**: Groupby operations performed in Dask before Pandas conversion

### Ghost Trip Detection
1. **Impossible Physics**: Speed exceeds 65 MPH
2. **Teleporter**: Trip duration under 1 minute with fare over $20
3. **Stationary Ride**: Zero distance with positive fare

### Congestion Zone Definition
Manhattan south of 60th Street (69 location IDs)
Border zones: 14 locations adjacent to the 60th Street boundary

### Weather Data
Source: Open-Meteo Archive API
Location: Central Park (40.7831°N, 73.9712°W)
Metric: Daily precipitation sum (mm)

## Dashboard Features
- Key metrics sidebar (revenue, compliance, ghost trips, elasticity)
- Interactive tabs for different analyses
- Image-based visualization display
- Real-time metric calculations from summary statistics

## Dependencies
- dask: Distributed computing framework
- pandas: Data manipulation
- numpy: Numerical computing
- matplotlib: Plotting library
- seaborn: Statistical visualization
- streamlit: Web dashboard framework
- requests: HTTP library for API calls
- pyarrow: Parquet file support
- pillow: Image processing

## Configuration
All settings defined in `src/config.py`:
- File paths
- Congestion zone location IDs
- Ghost trip thresholds
- Weather API parameters
- Schema mappings for yellow/green taxis

## Performance Considerations
- Memory-optimized aggregations prevent out-of-memory errors
- Sampling applied when ghost trip count exceeds 100,000
- Boolean masking used for efficient filtering
- Vectorized operations avoid Python loops

## Author
Abdulrahman Nisar
BSE-6A | Roll: 23F-3048

## Data Source
NYC Taxi & Limousine Commission (TLC) Trip Record Data
Analysis Period: January 2024 - December 2025
# MaritimeRiskCare: Comprehensive Maritime Risk Assessment

A data mining project that analyzes and predicts maritime risks across three critical dimensions: collision encounters, piracy attacks, and adverse weather conditions. This tool leverages machine learning and multi-source data integration to provide actionable insights for maritime safety and route planning.

---

## Project Overview

MaritimeRiskCare combines vessel tracking data, historical incident records, and real-time environmental data to assess and predict maritime risks. The system analyzes patterns across multiple risk domains to support safer navigation decisions.

**Key Risk Categories:**

- **Collision Risk** - Vessel encounter analysis and proximity hazards
- **Piracy Risk** - Historical attack patterns and geographic hotspots
- **Weather Risk** - Wave, wind, and current impacts on navigation safety

---

## Repository Structure

```
datamining/
├── collision.ipynb              # Collision risk analysis
├── piracy.ipynb                 # Piracy pattern analysis
├── weather.ipynb                # Weather impact analysis
├── collision/
│   ├── collision_risk.csv       # Processed encounter data
│   └── ais raw data/
│       └── gfw_encounters_recent.csv
├── piracy/
│   ├── cleaned_data_output/
│   │   ├── cleaned_pirate_attacks.csv    # Raw incidents cleaned (deduplicated, imputed, standardized)
│   │   ├── df_integrated_risks.csv       # Final Integrated dataframe with composite risk score and risk level
│   │   └── df_with_risk_scores.csv       # Enhanced dataframe with risk scores for all subsequent analysis
│   └── raw_data_archive/
│       ├── pirate_attacks.csv
│       ├── country_codes.csv
│       └── country_indicators.csv
└── weather/
    ├── ais_weather.csv          # Integrated vessel-weather data
    ├── wind10m.nc               # NCEP GFS wind data
    ├── waves_subset.nc          # CMEMS wave data
    └── currents_subset.nc       # CMEMS current data
```

---

## 1. Collision Risk Analysis

**File**: `collision.ipynb`

### Objectives

- Identify high-risk vessel encounter patterns using proximity analysis
- Assess temporal and spatial collision risk factors across maritime routes
- Classify encounter severity based on distance and vessel characteristics

### Data Sources

- **Global Fishing Watch (GFW)**: Vessel encounter events with detailed metrics
- **AIS Tracking Data**: Position (lat/lon), speed, course, and vessel identifiers (MMSI)
- **Vessel Metadata**: Flag, type, and operational status information

### Key Features

- **Encounter Metrics** (10+ features):

  - **Proximity Analysis**: Median distance between vessels, closest point of approach
  - **Duration Analysis**: Encounter start/end timestamps, total duration in hours
  - **Speed Metrics**: Median speed during encounter for both vessels
  - **Vessel Characteristics**: Type, flag, MMSI for both encounter participants
  - **Geographic Context**: Distance from shore, distance from nearest port, bounding box coordinates
  - **Risk Classification**: True/False high-risk flag for encounters

- **Temporal Features**:
  - Start/end timestamps for encounter window analysis
  - Duration calculations for prolonged proximity events

### Methodology

1. **Data Collection & Cleaning**:

   - Parse nested JSON fields (boundingBox, regions, vessel details)
   - Convert timestamps to datetime format
   - Extract relevant columns from raw GFW encounter data
   - Handle missing values and data type conversions 

2. **Feature Engineering**:

   - Calculate encounter duration from start/end times
   - Compute distance metrics (shore, port, vessel-to-vessel)

3. **Exploratory Data Analysis**:

   - Analyze distribution of encounter distances and durations
   - Identify outliers using IQR method on numeric features
   - Identify multicollinearity

4. **Risk Analysis**:
   - Flag encounters with possible risk (based on classification model training)
   - Analyze vessel type combinations in high-risk events
   - Assess correlation between distance from shore/port and risk levels
   - Analyse geographical areas with high risks

### Key Insights

- **Proximity Risk**: Encounters <1km identified as high collision risk requiring immediate attention
- **Vessel Type Patterns**: Certain vessel type combinations show higher encounter frequency
- **Temporal Trends**: Peak encounter periods correlate with high-traffic maritime hours
- **Geographic Distribution**: Proximity to ports and shipping lanes increases encounter density
- **Outlier Detection**: Anomalous encounters (extreme duration or proximity) flagged for investigation

---

## 2. Piracy Risk Analysis

**File**: `piracy.ipynb`

### Objectives

- Map global piracy attack hotspots and identify high-risk maritime corridors
- Analyze temporal trends in maritime piracy incidents spanning 27+ years (1993-2020)
- Classify attack types and assess vessel vulnerability patterns

### Data Sources

- **Kaggle Dataset**: Global Maritime Pirate Attacks (1993-2020) with 9,000+ incidents
- **Country Codes Dataset**: ISO codes for geographic region mapping and EEZ identification
- **Country Indicators**: Socioeconomic data for regional context (optional)

### Key Features

- **Geographic Features** (5+ features):

  - **Location Analysis**: Nearest country, EEZ (Exclusive Economic Zone) country, detailed location descriptions
  - **Coordinate Data**: Latitude/longitude for spatial mapping
  - **Regional Context**: International waters vs territorial waters classification

- **Attack Classification Features** (8+ features):

  - **Attack Types**: Boarding, hijacking, attempted attack, suspicious approach, fired upon
  - **Vessel Status**: Anchored, steaming, berthed, at anchor variations
  - **Outcome Metrics**: Casualties reported, damages incurred, crew status (captured, safe, injured)

- **Temporal Features** (4+ features):
  - **Date Extraction**: Year, month, day from attack timestamps
  - **Trend Analysis**: Decade classification, seasonal patterns

### Methodology

1. **Data Collection & Cleaning**:

   - Load 27+ years of piracy attack records from Kaggle dataset
   - Remove exact duplicates and filter high-missing-value columns (>80%)
   - Drop non-essential columns: time, attack_description, vessel_name, vessel_type
   - Standardize column names (lowercase, underscore separation)

2. **Missing Value Treatment**:

   - Fill missing `nearest_country` from `eez_country` where available
   - Assign "Unknown" to remaining missing `nearest_country` values
   - Fill missing `eez_country` as "International Waters"
   - Standardize `vessel_status` and `attack_type` with "Unknown" category

3. **Feature Engineering**:

   - Convert date strings to datetime format
   - Extract year, month for temporal trend analysis
   - Standardize attack type categories (consolidate similar types)
   - Create geographic region mappings using country codes

4. **Exploratory Data Analysis & Risk Modeling**:
   - Temporal trend analysis: yearly/monthly attack frequency patterns
   - Geographic hotspot identification: attacks per country/region
   - Attack type distribution: frequency by category and region
   - Vessel vulnerability assessment: attack success rates by vessel status

### Key Insights

- **Geographic Hotspots**: Identification of high-risk maritime corridors (Gulf of Aden, Malacca Strait, West Africa)
- **Temporal Trends**: Piracy incidents show distinct yearly patterns with peak periods in early 2000s and 2010s
- **Attack Type Distribution**: Boarding most common (60%+), hijacking concentrated in specific regions
- **Vessel Vulnerability**: Anchored/slow-moving vessels at significantly higher risk than steaming vessels
- **Seasonal Patterns**: Certain months show elevated attack frequency correlated with weather conditions

---

## 3. Weather Risk Analysis

**File**: `weather.ipynb`

### Objectives

- Integrate multi-source weather data with vessel navigation records
- Engineer maritime-specific features for weather impact assessment
- Classify weather-related maritime risks using machine learning
- Provide temporal and geographic weather risk insights

### Data Sources

- **AIS Vessel Data**: Position, speed, course from collision encounters
- **Copernicus Marine Service (CMEMS)**: Wave height, period, direction, and ocean currents
- **NCEP GFS**: 10m surface wind components (u10, v10)

### Weather Dimensions Analyzed

1. **Waves**: Significant height (VHM0), direction (VMDR), period (VTM10), steepness
2. **Currents**: East-west (uo) and north-south (vo) velocity components
3. **Wind**: Speed, direction, crosswind/headwind factors

### Key Features

- **Engineered Features** (50+ total):

  - **Wave Features**: Beam sea factor, head sea factor, wave energy, steepness, period stability
  - **Current Features**: Cross-current intensity, set-drift risk, current beam factor, directional stability
  - **Wind Features**: Crosswind intensity, headwind factor, wind direction stability, vessel-wind interaction
  - **Composite Features**: Environmental stress, wave-current ratio, operational complexity index

- **Temporal Features** (23 total):

  - Hour/day/month cyclical encodings (sin/cos)
  - Rush hour flags (6-9 AM, 5-8 PM)
  - Weekend/weekday indicators
  - Time of day categories (morning/afternoon/evening/night)

- **Geographic Features**:
  - Two-tier zone clustering (main operational vs remote zones)
  - Distance from operational center
  - Spatial risk distribution analysis

### Machine Learning Models

#### Model Selection

- **Wave & Current Risk**: Baseline RF, Tuned RF, XGBoost+SMOTE
- **Wind Risk**: Random Forest, Logistic Regression, Decision Tree

#### Training Configuration

- **Class Imbalance Handling**: SMOTE + class_weight='balanced'
- **Train/Test Split**: 80/20 stratified by vessel (mmsi)
- **Hyperparameter Tuning**: GridSearchCV with 3-fold CV
- **Threshold Optimization**: F1-score maximization (0.3-0.7 range)

#### Performance Metrics

- **Wave Model**: Tuned RF (F1=0.5260, optimized threshold=0.45)
- **Current Model**: Tuned RF (F1=0.5389, optimized threshold=0.35)
- **Wind Model**: Decision Tree (F1=0.9917, default threshold=0.50)
- **Evaluation**: Precision-Recall curves, AUC-PR, confusion matrices

### Key Analyses

1. **Risk-Type-Specific Modeling**: Separate models for wave, current, and wind hazards
2. **Temporal Pattern Analysis**: 6 time dimensions (hourly, daily, seasonal, rush hour, weekend)
3. **Geographic Clustering**: Two-tier operational zones with comparative risk profiles
4. **Feature Importance**: Interaction features (wave×period, vessel×wind) ranked highest

### Key Insights

- Wind hazards most predictable (F1=0.99), wave/current more complex (F1~0.53)
- Rush hours (6-9 AM, 5-8 PM) account for 20.8% of observations
- Geographic patterns show 85-95% of operations in main operational zone
- Temporal analysis reveals hour-specific and seasonal risk variations
- Engineered interaction features outperform raw sensor measurements

---

## 🛠️ Technologies & Libraries

### Data Processing

- **pandas**, **numpy**: Data manipulation and numerical operations
- **xarray**: NetCDF weather data processing
- **geopy**: Geographic distance calculations

### Machine Learning

- **scikit-learn**: Classification models, preprocessing, evaluation
- **imblearn**: SMOTE oversampling for class imbalance
- **statsmodels**: VIF analysis for feature selection

### Visualization

- **matplotlib**, **seaborn**: Statistical plots and heatmaps
- **folium** (optional): Interactive geographic mapping

### Data Sources

- **Copernicus Marine Service**: CMEMS wave and current data
- **NCEP GFS**: Global Forecast System wind data
- **Global Fishing Watch**: AIS vessel encounter data
- **Kaggle**: Historical piracy attack records

---

## Key Results

### Collision Risk

- **Data Processing**: Parsed and cleaned GFW encounter data with 10+ proximity and temporal metrics
- **Risk Classification**: High-risk encounters (<1km) successfully identified and flagged from AIS tracking data
- **Temporal Patterns**: Peak encounter periods aligned with high-traffic maritime hours and shipping lane activity
- **Geographic Insights**: Proximity to ports and major shipping routes significantly increases encounter density
- **Outlier Analysis**: IQR-based detection identified anomalous encounters requiring further investigation

### Piracy Risk

- **Dataset Scale**: 27+ years of global attack data (1993-2020) spanning 9,000+ incidents cleaned and standardized
- **Geographic Hotspots**: High-risk maritime corridors identified across Gulf of Aden, Malacca Strait, and West Africa
- **Temporal Evolution**: Distinct yearly patterns with peak piracy periods in early 2000s and 2010s documented
- **Attack Classification**: Boarding attacks dominate (60%+) with regional variations in hijacking and armed robbery
- **Vessel Vulnerability**: Anchored/slow-moving vessels face significantly elevated attack risk compared to steaming vessels

### Weather Risk

- **Model Performance**: F1-Scores achieved: Wind (0.9917), Wave (0.5260), Current (0.5389)
- **Feature Engineering**: 50+ maritime-specific features created capturing vessel-environment interactions
- **Temporal Analysis**: 6 time dimensions analyzed revealing rush hour (20.8% of data), seasonal, and hourly risk patterns
- **Geographic Clustering**: Two-tier zoning system developed with 85-95% of operations in main operational zone
- **Model Optimization**: Threshold tuning improved precision-recall balance (Wave: 0.45, Current: 0.35, Wind: 0.50)

---

## Usage

### Prerequisites

```bash
pip install pandas numpy xarray matplotlib seaborn scipy statsmodels
pip install scikit-learn imblearn geopy copernicusmarine folium 
```

### Running the Notebooks

1. **Collision Analysis**: Open and run `collision.ipynb`
2. **Piracy Analysis**: Open and run `piracy.ipynb`
3. **Weather Analysis**: Open and run `weather.ipynb`

### Data Requirements

- Ensure all data files are in their respective directories (`collision/`, `piracy/`, `weather/`)
- NetCDF weather files must be downloaded from CMEMS/NCEP sources
- AIS and piracy CSV files should be placed as per directory structure

---

## Contributors

IS424 Data Mining & Business Analytics 2025/26 Term 1 G1T3

---

## License

This project is for educational purposes as part of IS424 coursework.

---

## References

- Global Fishing Watch: https://globalfishingwatch.org/our-apis/documentation#events-get-http-request
- Copernicus Marine Service: https://data.marine.copernicus.eu/product/GLOBAL_ANALYSISFORECAST_PHY_001_024/description
- NOAA NCEP GFS: https://nomads.ncep.noaa.gov/gribfilter.php?ds=gdas_0p25
- Kaggle Piracy Dataset: https://www.kaggle.com/datasets/n0n5ense/global-maritime-pirate-attacks-19932020

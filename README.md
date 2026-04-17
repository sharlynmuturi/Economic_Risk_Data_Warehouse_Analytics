# Economic Risk & Stability Analysis

### Data Warehouse Project

This project builds a **data warehouse pipeline** to analyze macroeconomic performance and risk across multiple countries using data from the **World Bank** and **International Monetary Fund**.

The system transforms raw economic data into **clean, structured, and analytics-ready datasets**, enabling **insightful reporting, risk classification, and economic health scoring**.

## Objectives

*   Standardize multi-source economic data into a unified model
*   Build a **layered data architecture (Bronze - Silver - Gold)**
*   Develop **interpretable economic indicators and risk metrics**
*   Enable **analytics-ready datasets for visualization tools**
*   Generate **insight-driven reports for decision-making**


## Architecture

This project follows a **Medallion Architecture**:

### Bronze Layer (Raw Data)

*   Source data ingested from:
    *   World Bank (GDP, Inflation, Unemployment, Current Account)
    *   IMF (Government Debt)
*   Stored in raw, untransformed format

### Silver Layer (Data Cleaning & Standardization)

*   Unpivoted wide datasets into **time-series format**
*   Standardized schema:
    *   `country`, `year`, `indicator`, `value`
*   Handled:
    *   Null values using `TRY_CAST`
    *   Data inconsistencies
    *   Type conversions
*   Added:
    *   Source tracking (`World Bank`, `IMF`)


### Gold Layer (Business Logic & Metrics)

*   Pivoted indicators into **single row per country-year**
*   Engineered key features:

#### Economic Indicators

*   GDP Growth (%)
*   Inflation (%)
*   Unemployment (%)
*   Current Account (% GDP)
*   Government Debt (% GDP)

#### Derived Metrics

*   **Economic Health Score**
*   **Stability Score**
*   **Risk Category (LOW / MEDIUM / HIGH)**
*   Binary flags:
    *   `is_strong_economy`
    *   `is_economic_stress`

### Analytics Layer

*   Created **analysis-ready views**
*   Exported results as **CSV datasets**
*   Designed for:
    *   Trend analysis
    *   Cross-country comparison
    *   Risk profiling

## Key Analytical Insights

### 1. Persistent High Risk Across Countries

All countries analyzed fall under **HIGH RISK**, driven by:

*   High debt levels
*   Persistent current account deficits
*   Inflation volatility

### 2. Economic Shocks (2008 & 2020)

*   **Global Financial Crisis**
    *   GDP contraction across multiple economies
*   **COVID-19 pandemic**
    *   Severe GDP declines (e.g., India, South Africa, Nigeria)
    *   Rising debt levels globally


### 3. Country Highlights

#### 🇮🇳 India

*   Strong GDP growth recovery post-2020
*   Moderate debt growth
*   Still classified as high risk due to inflation spikes

#### 🇰🇪 Kenya

*   Stable growth (~5–7%)
*   Persistent current account deficits
*   Rising debt trend

#### 🇧🇷 Brazil

*   Volatile growth with recession periods
*   Increasing government debt
*   Weak external balance

#### 🇳🇬 Nigeria

*   High inflation volatility
*   Strong current account early on, later weakened
*   Moderate debt but rising instability

#### 🇿🇦 South Africa

*   Structural challenges:
    *   High unemployment (>25%)
    *   Low growth
*   Consistently low stability scores


## Data Quality Framework

Comprehensive quality checks implemented:

*  Missing value detection
*  Duplicate validation
*  Time-series completeness (2005–2024)
*  Outlier detection
*  Indicator standardization
*  Source consistency validation


## Tech Stack

*   **SQL Server** (Data Warehouse)
*   **T-SQL** (Transformations & Analytics)
*   **Medallion Architecture**
*   **CSV Export for BI tools**

## How to Run

1.  Load Bronze tables
2.  Execute:

    ```
    EXEC silver.load_silver;
    ```

3.  Build Gold layer:
    
    ```
    EXEC gold.build_gold;
    ```

4.  Run analytics queries
5.  Export results as CSV:

## Future Enhancements

*   Add more countries and indicators
*   Integrate real-time APIs
*   Apply machine learning for **economic forecasting**
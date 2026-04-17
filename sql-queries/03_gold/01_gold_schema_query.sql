/*
DDL Script: creates views for the Gold layer (final dimension) in the data warehouse. 

TABLE 1: COUNTRY YEAR FACT TABLE
    -This is the central analytical table in the Gold layer. Each row represents One country and One year
    - It stores key macroeconomic indicators in a structured form suitable for time-series analysis and dashboarding.
    - Silver layer data is in "long format" (indicator-value pairs). This converts it into a "wide format" fact table
*/

IF OBJECT_ID('gold.country_year_metrics', 'U') IS NOT NULL
    DROP TABLE gold.country_year_metrics;
GO
IF OBJECT_ID('gold.country_year_metrics', 'U') IS NOT NULL
    DROP TABLE gold.country_year_metrics;
GO

CREATE TABLE gold.country_year_metrics (
    country NVARCHAR(100),
    country_code NVARCHAR(10),
    year INT,

    -- Core macroeconomic indicators
    gdp_growth FLOAT,   -- economic expansion/contraction
    inflation FLOAT,    -- price stability indicator
    unemployment FLOAT, -- labour market health
    current_account FLOAT,  -- trade + capital flow balance
    government_debt FLOAT,  -- fiscal sustainability risk

    -- Data quality + lineage tracking
    data_completeness_score FLOAT,  -- potential future data quality metric
    dwh_created_date DATETIME2 DEFAULT GETDATE()
);
GO


/*
STEP 1: PROCEDURE: LOAD COUNTRY YEAR METRICS
    Transforms normalized Silver-layer data into a structured analytical Gold-layer fact table.
    Transforms indicator-value rows  to  Column-based metrics (pivoting)
        - BI tools prefer columnar structure
        - Enables fast aggregation and filtering
        - Improves readability for analysts
*/
CREATE OR ALTER PROCEDURE gold.load_country_year_metrics
AS
BEGIN
    SET NOCOUNT ON;

    -- Clear existing Gold data before reloading (full refresh strategy)
    TRUNCATE TABLE gold.country_year_metrics;

    INSERT INTO gold.country_year_metrics (
        country,
        country_code,
        year,
        gdp_growth,
        inflation,
        unemployment,
        current_account,
        government_debt
    )
    SELECT
        country,
        country_code,
        year,

        -- Pivoting indicator values into columns using conditional aggregation
        MAX(CASE WHEN indicator = 'GDP growth (annual %)' THEN value END),
        MAX(CASE WHEN indicator LIKE '%Inflation%' THEN value END),
        MAX(CASE WHEN indicator LIKE '%Unemployment%' THEN value END),
        MAX(CASE WHEN indicator LIKE '%Current account%' THEN value END),
        MAX(CASE WHEN indicator LIKE '%Debt%' THEN value END)

    FROM silver.economic_indicators

    -- Grouping ensures one row per country-year combination
    GROUP BY country, country_code, year;

END;
GO


/*
TABLE 2: COUNTRY ECONOMIC INDEX (SCORE MODEL)
This table introduces a simple economic scoring model to evaluate macroeconomic performance.
It transforms raw indicators into:
    1. Economic Health Score (performance)
    2. Stability Score (volatility / resilience)
        - Converts raw data into business intelligence
        - Enables ranking of countries
        - Supports risk classification models
*/

IF OBJECT_ID('gold.country_economic_index', 'U') IS NOT NULL
    DROP TABLE gold.country_economic_index;
GO

CREATE TABLE gold.country_economic_index (
    country NVARCHAR(100),
    year INT,

    -- Raw indicators (for transparency and recalculation)
    gdp_growth FLOAT,
    inflation FLOAT,
    unemployment FLOAT,
    debt FLOAT,

    -- Derived intelligence metrics (core analytics output)
    economic_health_score FLOAT,
    stability_score FLOAT,

    dwh_created_date DATETIME2 DEFAULT GETDATE()
);
GO



/*
PROCEDURE: LOAD ECONOMIC INDEX
    Builds composite economic indicators using a weighted model.
        - Positive factors: GDP growth
        - Negative factors: inflation, unemployment, debt
*/

CREATE OR ALTER PROCEDURE gold.load_country_economic_index
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE gold.country_economic_index;

    INSERT INTO gold.country_economic_index (
        country,
        year,
        gdp_growth,
        inflation,
        unemployment,
        debt,
        economic_health_score,
        stability_score
    )
    SELECT
        country,
        year,
        gdp_growth,
        inflation,
        unemployment,
        government_debt,

        /*
           ECONOMIC HEALTH SCORE (COMPOSITE INDEX)
           A weighted scoring model that estimates overall macroeconomic performance.
               + GDP growth improves score (positive signal)
               - Inflation reduces score (economic instability)
               - Unemployment reduces score (social/economic stress)
               - Debt reduces score (fiscal burden)
        */
        (
            ISNULL(gdp_growth, 0) * 0.35
            - ISNULL(inflation, 0) * 0.20
            - ISNULL(unemployment, 0) * 0.25
            - ISNULL(government_debt, 0) * 0.20
        ) AS economic_health_score,

        /* 
           STABILITY SCORE
           Measures macroeconomic volatility.
            - Lower inflation + unemployment = more stability
       */
        (
            100
            - ABS(ISNULL(inflation, 0))
            - ABS(ISNULL(unemployment, 0))
        ) AS stability_score

    FROM gold.country_year_metrics;

END;
GO


/*
TABLE 3: COUNTRY RISK DASHBOARD
This is the final aggregated dataset for visualization.
    - Power BI dashboards
    - Executive reporting
    - Country risk comparison
    - Storytelling & insights

It summarizes Multi-year performance, Risk classification and Average macro indicators
 */

IF OBJECT_ID('gold.country_risk_dashboard', 'U') IS NOT NULL
    DROP TABLE gold.country_risk_dashboard;
GO

CREATE TABLE gold.country_risk_dashboard (
    country NVARCHAR(100),

    -- Multi-year averages (trend smoothing)
    avg_gdp_growth FLOAT,
    avg_inflation FLOAT,
    avg_unemployment FLOAT,
    avg_debt FLOAT,

    -- Composite score for ranking countries
    economic_health_score FLOAT,

    -- Business-friendly classification label
    risk_category NVARCHAR(50),

    dwh_created_date DATETIME2 DEFAULT GETDATE()
);
GO


/*
LOAD PROCEDURE (RISK CLASSIFICATION)
Aggregates country-level performance and assigns risk labels.
    - LOW RISK (strong economy)
    - MEDIUM RISK (moderate instability)
    - HIGH RISK (economic stress)
 */

CREATE OR ALTER PROCEDURE gold.load_country_risk_dashboard
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE gold.country_risk_dashboard;

    INSERT INTO gold.country_risk_dashboard (
        country,
        avg_gdp_growth,
        avg_inflation,
        avg_unemployment,
        avg_debt,
        economic_health_score,
        risk_category
    )
    SELECT
        country,

        -- Multi-year trend aggregation (smoothing volatility)
        AVG(gdp_growth),
        AVG(inflation),
        AVG(unemployment),
        AVG(debt),

        -- Overall country performance score
        AVG(economic_health_score),


        /* 
           RISK CLASSIFICATION LOGIC
           Converts numeric score into business-friendly labels for dashboards and decision-making.
       */
        CASE
            WHEN AVG(economic_health_score) > 5 THEN 'LOW RISK'
            WHEN AVG(economic_health_score) BETWEEN 0 AND 5 THEN 'MEDIUM RISK'
            ELSE 'HIGH RISK'
        END

    FROM gold.country_economic_index
    GROUP BY country;

END;
GO

-- FINAL PIPELINE EXECUTION ORDER
EXEC bronze.load_bronze;
EXEC silver.load_silver;

EXEC gold.load_country_year_metrics;
EXEC gold.load_country_economic_index;
EXEC gold.load_country_risk_dashboard;
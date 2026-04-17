/*
ANALYTICS LAYER (SEMANTIC / CONSUMPTION LAYER)
This layer transforms Gold-layer data into Business-ready analytical views, ML-ready feature tables andExportable flat files


SCHEMA: CREATING SCHEMA ANALYTICS
This schema isolates Reporting views, Dashboard datasets, ML feature tables
*/

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'analytics')
BEGIN
    EXEC('CREATE SCHEMA analytics');
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'analytics')
BEGIN
    EXEC('CREATE SCHEMA analytics');
END;
GO



-- VIEW: COUNTRY ECONOMIC TIMESERIES
-- Primary dataset for Line charts, Time-series comparisons and Country economic evolution tracking (directly consumable by Power BI without transformations)
DROP VIEW IF EXISTS analytics.country_economic_timeseries;
GO

CREATE VIEW analytics.country_economic_timeseries AS
SELECT 
    country,
    country_code,
    year,

    -- Core macroeconomic indicators used in trend analysis
    gdp_growth,
    inflation,
    unemployment,
    current_account,
    government_debt,

    /*
        DATA COMPLETENESS FEATURE (DATA QUALITY METRIC)
        Helps analysts quickly identify missing data patterns across countries and years.
            - Data reliability scoring
            - Filtering incomplete records in dashboards
            - Highlighting weak data coverage regions
    */
    CASE 
        WHEN gdp_growth IS NULL THEN 1 ELSE 0 
    END +
    CASE 
        WHEN inflation IS NULL THEN 1 ELSE 0 
    END +
    CASE 
        WHEN unemployment IS NULL THEN 1 ELSE 0 
    END +
    CASE 
        WHEN government_debt IS NULL THEN 1 ELSE 0 
    END AS missing_indicator_count

FROM gold.country_year_metrics;
GO


SELECT * 
FROM analytics.country_economic_timeseries;



-- VIEW: COUNTRY SCORECARD
-- Converts raw numeric scores into Business-friendly labels, Dashboard KPIs and Executive-level summaries
DROP VIEW IF EXISTS analytics.country_scorecard;
GO

CREATE VIEW analytics.country_scorecard AS
SELECT 
    country,
    year,

    economic_health_score,
    stability_score,

    /*
       INTERPRETABLE LABELS FOR REPORTING
        Converts abstract numeric scores into meaningful categories for non-technical stakeholders.
    */
    CASE 
        WHEN economic_health_score >= 5 THEN 'Strong Economy'
        WHEN economic_health_score BETWEEN 0 AND 5 THEN 'Moderate Economy'
        ELSE 'Weak Economy'
    END AS economic_label,

    CASE 
        WHEN stability_score >= 70 THEN 'Stable'
        WHEN stability_score BETWEEN 40 AND 70 THEN 'Moderate'
        ELSE 'Volatile'
    END AS stability_label

FROM gold.country_economic_index;
GO

SELECT * 
FROM analytics.country_scorecard;



-- VIEW: COUNTRY RISK SUMMARY
-- Provides aggregated country-level risk insights for Executive dashboards, KPI cards and Country benchmarking
-- It removes time complexity and focuses on strategic-level decision metrics.
DROP VIEW IF EXISTS analytics.country_risk_summary;
GO

CREATE VIEW analytics.country_risk_summary AS
SELECT 
    country,

    avg_gdp_growth,
    avg_inflation,
    avg_unemployment,
    avg_debt,

    economic_health_score,
    risk_category,
    /*
        BUSINESS SEGMENTATION FEATURES
        These transform numeric averages into readable profiles for storytelling and decision-making.
    */
    CASE 
        WHEN avg_gdp_growth > 5 THEN 'High Growth'
        WHEN avg_gdp_growth BETWEEN 2 AND 5 THEN 'Stable Growth'
        ELSE 'Low Growth'
    END AS growth_profile,

    CASE 
        WHEN avg_debt > 100 THEN 'Highly Indebted'
        WHEN avg_debt BETWEEN 50 AND 100 THEN 'Moderate Debt'
        ELSE 'Low Debt'
    END AS debt_profile

FROM gold.country_risk_dashboard;
GO

SELECT * 
FROM analytics.country_risk_summary;




-- VIEW: ECONOMIC HEATMAP
-- Designed for Heatmaps (Country × Year), Visual anomaly detection and Risk intensity mapping
-- Enables intuitive visualization of macroeconomic stress.
DROP VIEW IF EXISTS analytics.economic_heatmap;
GO

CREATE VIEW analytics.economic_heatmap AS
SELECT 
    country,
    year,

    gdp_growth,
    inflation,
    unemployment,

    /*
        RISK SIGNAL ENGINEERING
        Converts continuous values into categorical signals for visualization (heatmaps, dashboards).
    */
    CASE 
        WHEN inflation > 20 THEN 'HIGH_INFLATION'
        WHEN inflation BETWEEN 5 AND 20 THEN 'MODERATE_INFLATION'
        ELSE 'STABLE_INFLATION'
    END AS inflation_signal,

    CASE 
        WHEN unemployment > 25 THEN 'HIGH_UNEMPLOYMENT'
        WHEN unemployment BETWEEN 10 AND 25 THEN 'MODERATE_UNEMPLOYMENT'
        ELSE 'LOW_UNEMPLOYMENT'
    END AS unemployment_signal

FROM gold.country_year_metrics;
GO


SELECT * 
FROM analytics.economic_heatmap;



-- VIEW: ECONOMIC MOMENTUM
-- Captures YEAR-ON-YEAR CHANGE (trend dynamics)
DROP VIEW IF EXISTS analytics.economic_momentum;
GO

CREATE VIEW analytics.economic_momentum AS
SELECT 
    country,
    year,

    gdp_growth,

    /*
        MOMENTUM FEATURES (TIME SERIES DERIVATIVES)
        These measure acceleration / deceleration of economy and directional changes in macro indicators
    */
    gdp_growth - LAG(gdp_growth) OVER (
        PARTITION BY country ORDER BY year
    ) AS gdp_growth_change,

    inflation - LAG(inflation) OVER (
        PARTITION BY country ORDER BY year
    ) AS inflation_change,

    unemployment - LAG(unemployment) OVER (
        PARTITION BY country ORDER BY year
    ) AS unemployment_change

FROM gold.country_year_metrics;
GO


SELECT * 
FROM analytics.economic_momentum;



-- VIEW: FINAL EXPORT DATASET
--  designed for Machine learning pipelines, CSV exports, Python/R analysis and Feature engineering datasets
DROP VIEW IF EXISTS analytics.final_export_dataset;
GO

CREATE VIEW analytics.final_export_dataset AS
SELECT 
    m.country,
    m.country_code,
    m.year,

    -- Raw macroeconomic indicators (ground truth features)
    m.gdp_growth,
    m.inflation,
    m.unemployment,
    m.current_account,
    m.government_debt,

    -- Model-derived intelligence features
    i.economic_health_score,
    i.stability_score,

    -- Risk classification label
    d.risk_category,


    /*
        FINAL FEATURE ENGINEERING LAYER
        These binary flags simplify ML modeling and classification:
        - strong vs weak economies
        - stress detection
    */
    CASE 
        WHEN i.economic_health_score > 5 THEN 1
        ELSE 0
    END AS is_strong_economy,

    CASE 
        WHEN m.inflation > 10 OR m.unemployment > 20 THEN 1
        ELSE 0
    END AS is_economic_stress

FROM gold.country_year_metrics m
LEFT JOIN gold.country_economic_index i
    ON m.country = i.country AND m.year = i.year
LEFT JOIN gold.country_risk_dashboard d
    ON m.country = d.country;
GO


SELECT *
FROM analytics.final_export_dataset;
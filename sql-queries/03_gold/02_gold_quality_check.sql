/*
=====================================================
GOLD LAYER QUALITY CHECKS
=====================================================
Purpose:
- Validate business-ready datasets
- Ensure scoring consistency
- Detect anomalies in KPIs
- Confirm dashboard readiness
=====================================================
*/

-- 1. BASIC DATA HEALTH
-- Total records check
SELECT COUNT(*) AS total_gold_metrics
FROM gold.country_year_metrics;

SELECT COUNT(*) AS total_gold_index
FROM gold.country_economic_index;

SELECT COUNT(*) AS total_dashboard_records
FROM gold.country_risk_dashboard;

-- Null check in core metrics
SELECT *
FROM gold.country_year_metrics
WHERE country IS NULL
   OR year IS NULL;

-- 2. DUPLICATE DETECTION (CRITICAL)
-- Country-year duplicates in fact table
SELECT 
    country,
    year,
    COUNT(*) AS duplicate_count
FROM gold.country_year_metrics
GROUP BY country, year
HAVING COUNT(*) > 1;

-- Index table duplicates
SELECT 
    country,
    year,
    COUNT(*) AS duplicate_count
FROM gold.country_economic_index
GROUP BY country, year
HAVING COUNT(*) > 1;

-- 3. KPI RANGE VALIDATION (BUSINESS RULES)
-- GDP Growth sanity
SELECT *
FROM gold.country_year_metrics
WHERE gdp_growth NOT BETWEEN -50 AND 50;

-- Inflation sanity
SELECT *
FROM gold.country_year_metrics
WHERE inflation NOT BETWEEN -50 AND 200;

-- Unemployment sanity
SELECT *
FROM gold.country_year_metrics
WHERE unemployment NOT BETWEEN 0 AND 100;

-- Debt sanity
SELECT *
FROM gold.country_year_metrics
WHERE government_debt < 0
   OR government_debt > 500;


-- 4. SCORE MODEL VALIDATION
-- Check missing scores
SELECT *
FROM gold.country_economic_index
WHERE economic_health_score IS NULL
   OR stability_score IS NULL;

-- Score range validation (should not explode)
SELECT 
    MIN(economic_health_score) AS min_score,
    MAX(economic_health_score) AS max_score,
    AVG(economic_health_score) AS avg_score
FROM gold.country_economic_index;

-- Stability score sanity
SELECT *
FROM gold.country_economic_index
WHERE stability_score < 0
   OR stability_score > 100;

-- 5. RISK CATEGORY CONSISTENCY
SELECT 
    risk_category,
    COUNT(*) AS total_records
FROM gold.country_risk_dashboard
GROUP BY risk_category;


-- Invalid categories check
SELECT *
FROM gold.country_risk_dashboard
WHERE risk_category NOT IN ('LOW RISK', 'MEDIUM RISK', 'HIGH RISK');

-- 6. CROSS-LAYER CONSISTENCY (VERY IMPORTANT)
-- Ensure Gold matches Silver coverage
SELECT 
    COUNT(DISTINCT country) AS gold_countries
FROM gold.country_year_metrics;


SELECT 
    COUNT(DISTINCT country) AS silver_countries
FROM silver.economic_indicators;

-- Missing countries in Gold vs Silver
SELECT DISTINCT s.country
FROM silver.economic_indicators s
LEFT JOIN gold.country_year_metrics g
    ON s.country = g.country
WHERE g.country IS NULL;


-- 7. TIME SERIES CONTINUITY CHECK
-- Detect missing years per country
WITH year_check AS (
    SELECT 
        country,
        year,
        LAG(year) OVER (PARTITION BY country ORDER BY year) AS prev_year
    FROM gold.country_year_metrics
)
SELECT *
FROM year_check
WHERE prev_year IS NOT NULL
AND year - prev_year > 1;


-- 8. SCORE DISTRIBUTION VALIDATION
-- Economic health distribution
SELECT 
    CASE 
        WHEN economic_health_score > 5 THEN 'HIGH'
        WHEN economic_health_score BETWEEN 0 AND 5 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS score_band,
    COUNT(*) AS count
FROM gold.country_economic_index
GROUP BY 
    CASE 
        WHEN economic_health_score > 5 THEN 'HIGH'
        WHEN economic_health_score BETWEEN 0 AND 5 THEN 'MEDIUM'
        ELSE 'LOW'
    END;

-- 9. OUTLIER COUNTRIES (FLAGGING ANOMALIES)
SELECT *
FROM gold.country_economic_index
WHERE ABS(economic_health_score) > 50;

-- 10. DASHBOARD READINESS CHECK (FINAL GATE)
-- Ensure no critical nulls in dashboard table
SELECT *
FROM gold.country_risk_dashboard
WHERE country IS NULL
   OR avg_gdp_growth IS NULL
   OR economic_health_score IS NULL;


-- Final readiness summary
SELECT 
    COUNT(*) AS total_countries,
    COUNT(DISTINCT risk_category) AS category_diversity,
    MIN(economic_health_score) AS min_score,
    MAX(economic_health_score) AS max_score
FROM gold.country_risk_dashboard;


-- Country ranking leaderboard
SELECT 
    country,
    economic_health_score,
    RANK() OVER (ORDER BY economic_health_score DESC) AS ranking
FROM gold.country_economic_index;

-- Stability vs Growth anomaly detection
SELECT *
FROM gold.country_economic_index
WHERE economic_health_score > 5
  AND stability_score < 30;
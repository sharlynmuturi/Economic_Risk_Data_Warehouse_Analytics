-- 1. BASIC HEALTH CHECKS

-- Row count
SELECT COUNT(*) AS total_rows
FROM silver.economic_indicators;

-- Null value check
SELECT 
    COUNT(*) AS null_values
FROM silver.economic_indicators
WHERE value IS NULL;

-- Null breakdown by indicator
SELECT 
    indicator,
    COUNT(*) AS null_values
FROM silver.economic_indicators
WHERE value IS NULL
GROUP BY indicator
ORDER BY null_values DESC;


-- 2. DUPLICATES & DATA INTEGRITY

-- Duplicate grain check (expected grain = country + indicator + year + source)
SELECT 
    country,
    indicator,
    year,
    source,
    COUNT(*) AS duplicate_count
FROM silver.economic_indicators
GROUP BY country, indicator, year, source
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 3. COVERAGE / COMPLETENESS

-- Year coverage per indicator
SELECT 
    indicator,
    COUNT(DISTINCT year) AS years_present
FROM silver.economic_indicators
GROUP BY indicator
ORDER BY years_present;


-- Expected full coverage check (2005–2024 = 20 years)
SELECT 
    country,
    indicator,
    COUNT(DISTINCT year) AS years_available,
    20 - COUNT(DISTINCT year) AS missing_years
FROM silver.economic_indicators
GROUP BY country, indicator
ORDER BY missing_years DESC;

-- 4. SOURCE CONSISTENCY
SELECT 
    indicator,
    source,
    COUNT(*) AS records
FROM silver.economic_indicators
GROUP BY indicator, source
ORDER BY indicator;


-- 5. COUNTRY & INDICATOR STANDARDIZATION

-- Countries list
SELECT DISTINCT country
FROM silver.economic_indicators
ORDER BY country;

-- Indicators list
SELECT DISTINCT indicator
FROM silver.economic_indicators
ORDER BY indicator;


-- 6. DATA RANGE / OUTLIER DETECTION
    -- adding a global statistical view + rules instead of hardcoding per indicator
-- Global extreme values
SELECT 
    indicator,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    AVG(value) AS avg_value
FROM silver.economic_indicators
GROUP BY indicator;


-- Business-rule based outliers
SELECT *
FROM silver.economic_indicators
WHERE 
    (indicator LIKE '%Inflation%' AND (value > 100 OR value < -10))
    OR
    (indicator LIKE '%GDP%' AND (value > 15 OR value < -15))
    OR
    (indicator LIKE '%Unemployment%' AND (value < 0 OR value > 60))
    OR
    (indicator LIKE '%Debt%' AND value > 200)
    OR
    (indicator LIKE '%Current account%' AND ABS(value) > 50);


-- 7. TIME CONTINUITY (GAPS)
WITH ordered AS (
    SELECT 
        country,
        indicator,
        year,
        LAG(year) OVER (
            PARTITION BY country, indicator 
            ORDER BY year
        ) AS prev_year
    FROM silver.economic_indicators
)
SELECT *
FROM ordered
WHERE prev_year IS NOT NULL
AND year - prev_year > 1;


-- NEGATIVE VALUE VALIDATION (DOMAIN RULES)
SELECT *
FROM silver.economic_indicators
WHERE 
    (indicator LIKE '%Unemployment%' AND value < 0)
    OR
    (indicator LIKE '%Debt%' AND value < 0);


-- 9. NULL COUNTRY CODE CHECK
SELECT *
FROM silver.economic_indicators
WHERE country_code IS NULL;


-- 10. COMPLETENESS SCORE

-- Overall completeness per country
SELECT 
    country,
    COUNT(*) AS total_records,
    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS missing_values,
    ROUND(
        100.0 * (1 - SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*)),
        2
    ) AS completeness_pct
FROM silver.economic_indicators
GROUP BY country
ORDER BY completeness_pct ASC;


-- 11. DATA DENSITY CHECK
SELECT 
    country,
    indicator,
    COUNT(*) AS total_records
FROM silver.economic_indicators
GROUP BY country, indicator
ORDER BY total_records;


-- 12. SOURCE RELIABILITY CHECK
SELECT 
    source,
    COUNT(*) AS total_records,
    COUNT(DISTINCT indicator) AS indicators_covered
FROM silver.economic_indicators
GROUP BY source;
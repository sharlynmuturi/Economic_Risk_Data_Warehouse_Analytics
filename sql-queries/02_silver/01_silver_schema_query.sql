/*
SILVER LAYER: ECONOMIC INDICATORS STANDARDIZATION

This DDL script builds the Silver layer of the data warehouse responsible for:
- Cleaning raw Bronze data
- Standardizing structure across multiple sources
- Converting wide tables into a normalized format
- Creating a unified "indicator-based" dataset



TABLE: SILVER ECONOMIC INDICATORS
This is a standardized, normalized fact table where each row represents 1 country, 1 indicator, 1 year (LONG FORMAT not wide) to support flexible analytics, pivoting, and aggregation.
*/
IF OBJECT_ID('silver.economic_indicators', 'U') IS NOT NULL
    DROP TABLE silver.economic_indicators;
GO

CREATE TABLE silver.economic_indicators (
    country NVARCHAR(100),
    country_code NVARCHAR(10),

    -- The metric being measured (e.g. GDP, Inflation)
    indicator NVARCHAR(150),

    -- Time dimension for trend analysis
    year INT,

    -- Numerical value of the indicator
    value FLOAT,

    -- Data lineage tracking (for auditability)
    source NVARCHAR(50),
    dwh_created_date DATETIME2 DEFAULT GETDATE()
);
GO


/*
PROCEDURE: LOAD SILVER LAYER
This procedure transforms multiple raw Bronze tables into a unified Silver schema.
    1. Wide format (year columns) - Long format (row-based)
    2. Multiple data sources - unified schema
    3. Data type cleaning (TRY_CAST)
    4. Removal of invalid values
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    -- Full refresh approach ensures no stale data remains
    TRUNCATE TABLE silver.economic_indicators;

    PRINT 'Loading Silver Layer...';


    -- DATASET 1: CURRENT ACCOUNT BALANCE (World Bank)
    -- This transforms yearly columns into rows using UNPIVOT logic. Each country-year-value becomes a single record.
    INSERT INTO silver.economic_indicators (
        country, country_code, indicator, year, value, source
    )
    SELECT 
        b.country_name,
        b.country_code,
        b.indicator_name,

        -- Extract year from column name (year_2005 - 2005)
        CAST(REPLACE(v.year_col, 'year_', '') AS INT) AS year,

        -- Safely convert values (prevents pipeline failure on bad data)
        TRY_CAST(v.value AS FLOAT),
        'World Bank'
    FROM (SELECT DISTINCT * FROM bronze.world_bank_current_account) b

    -- UNPIVOT operation using CROSS APPLY
    -- Converts multiple year columns into row format
    CROSS APPLY (VALUES
        ('year_2005', b.year_2005),
        ('year_2006', b.year_2006),
        ('year_2007', b.year_2007),
        ('year_2008', b.year_2008),
        ('year_2009', b.year_2009),
        ('year_2010', b.year_2010),
        ('year_2011', b.year_2011),
        ('year_2012', b.year_2012),
        ('year_2013', b.year_2013),
        ('year_2014', b.year_2014),
        ('year_2015', b.year_2015),
        ('year_2016', b.year_2016),
        ('year_2017', b.year_2017),
        ('year_2018', b.year_2018),
        ('year_2019', b.year_2019),
        ('year_2020', b.year_2020),
        ('year_2021', b.year_2021),
        ('year_2022', b.year_2022),
        ('year_2023', b.year_2023),
        ('year_2024', b.year_2024)
    ) v(year_col, value)

    -- Remove null/invalid numeric values to maintain data quality
    WHERE TRY_CAST(v.value AS FLOAT) IS NOT NULL;



    -- DATASET 2: GDP GROWTH (World Bank)
    -- Same transformation pattern applied: Wide - Long format normalization
    INSERT INTO silver.economic_indicators (
        country, country_code, indicator, year, value, source
    )
    SELECT 
        b.country_name,
        b.country_code,
        b.indicator_name,
        CAST(REPLACE(v.year_col, 'year_', '') AS INT),
        TRY_CAST(v.value AS FLOAT),
        'World Bank'
    FROM (SELECT DISTINCT * FROM bronze.world_bank_gdp_growth) b
    CROSS APPLY (VALUES
        ('year_2005', b.year_2005),
        ('year_2006', b.year_2006),
        ('year_2007', b.year_2007),
        ('year_2008', b.year_2008),
        ('year_2009', b.year_2009),
        ('year_2010', b.year_2010),
        ('year_2011', b.year_2011),
        ('year_2012', b.year_2012),
        ('year_2013', b.year_2013),
        ('year_2014', b.year_2014),
        ('year_2015', b.year_2015),
        ('year_2016', b.year_2016),
        ('year_2017', b.year_2017),
        ('year_2018', b.year_2018),
        ('year_2019', b.year_2019),
        ('year_2020', b.year_2020),
        ('year_2021', b.year_2021),
        ('year_2022', b.year_2022),
        ('year_2023', b.year_2023),
        ('year_2024', b.year_2024)
    ) v(year_col, value)
    WHERE TRY_CAST(v.value AS FLOAT) IS NOT NULL;


    -- DATASET 3: INFLATION RATE
    -- Captures price instability across countries and time. Used later in Gold layer for risk scoring.
    INSERT INTO silver.economic_indicators (
        country, country_code, indicator, year, value, source
    )
    SELECT 
        b.country_name,
        b.country_code,
        b.indicator_name,
        CAST(REPLACE(v.year_col, 'year_', '') AS INT),
        TRY_CAST(v.value AS FLOAT),
        'World Bank'
    FROM (SELECT DISTINCT * FROM bronze.world_bank_inflation) b
    CROSS APPLY (VALUES
        ('year_2005', b.year_2005),
        ('year_2006', b.year_2006),
        ('year_2007', b.year_2007),
        ('year_2008', b.year_2008),
        ('year_2009', b.year_2009),
        ('year_2010', b.year_2010),
        ('year_2011', b.year_2011),
        ('year_2012', b.year_2012),
        ('year_2013', b.year_2013),
        ('year_2014', b.year_2014),
        ('year_2015', b.year_2015),
        ('year_2016', b.year_2016),
        ('year_2017', b.year_2017),
        ('year_2018', b.year_2018),
        ('year_2019', b.year_2019),
        ('year_2020', b.year_2020),
        ('year_2021', b.year_2021),
        ('year_2022', b.year_2022),
        ('year_2023', b.year_2023),
        ('year_2024', b.year_2024)
    ) v(year_col, value)
    WHERE TRY_CAST(v.value AS FLOAT) IS NOT NULL;



    -- DATASET 4: UNEMPLOYMENT RATE
    -- Key labour market indicator used in Economic Health Score and Risk classification model
    INSERT INTO silver.economic_indicators (
        country, country_code, indicator, year, value, source
    )
    SELECT 
        b.country_name,
        b.country_code,
        b.indicator_name,
        CAST(REPLACE(v.year_col, 'year_', '') AS INT),
        TRY_CAST(v.value AS FLOAT),
        'World Bank'
    FROM (SELECT DISTINCT * FROM bronze.world_bank_unemployment) b
    CROSS APPLY (VALUES
        ('year_2005', b.year_2005),
        ('year_2006', b.year_2006),
        ('year_2007', b.year_2007),
        ('year_2008', b.year_2008),
        ('year_2009', b.year_2009),
        ('year_2010', b.year_2010),
        ('year_2011', b.year_2011),
        ('year_2012', b.year_2012),
        ('year_2013', b.year_2013),
        ('year_2014', b.year_2014),
        ('year_2015', b.year_2015),
        ('year_2016', b.year_2016),
        ('year_2017', b.year_2017),
        ('year_2018', b.year_2018),
        ('year_2019', b.year_2019),
        ('year_2020', b.year_2020),
        ('year_2021', b.year_2021),
        ('year_2022', b.year_2022),
        ('year_2023', b.year_2023),
        ('year_2024', b.year_2024)
    ) v(year_col, value)
    WHERE TRY_CAST(v.value AS FLOAT) IS NOT NULL;



    -- DATASET 5: GOVERNMENT DEBT (% GDP)
    -- This dataset uses a custom indicator name because IMF structure differs from World Bank datasets.
    INSERT INTO silver.economic_indicators (
        country, country_code, indicator, year, value, source
    )
    SELECT 
        b.country,
        b.country_code,
        'Government Debt (% GDP)',
        CAST(REPLACE(v.year_col, 'year_', '') AS INT),
        TRY_CAST(v.value AS FLOAT),
        'IMF'
    FROM (SELECT DISTINCT * FROM bronze.imf_debt_raw) b
    CROSS APPLY (VALUES
        ('year_2005', b.year_2005),
        ('year_2006', b.year_2006),
        ('year_2007', b.year_2007),
        ('year_2008', b.year_2008),
        ('year_2009', b.year_2009),
        ('year_2010', b.year_2010),
        ('year_2011', b.year_2011),
        ('year_2012', b.year_2012),
        ('year_2013', b.year_2013),
        ('year_2014', b.year_2014),
        ('year_2015', b.year_2015),
        ('year_2016', b.year_2016),
        ('year_2017', b.year_2017),
        ('year_2018', b.year_2018),
        ('year_2019', b.year_2019),
        ('year_2020', b.year_2020),
        ('year_2021', b.year_2021),
        ('year_2022', b.year_2022),
        ('year_2023', b.year_2023),
        ('year_2024', b.year_2024)
    ) v(year_col, value)
    WHERE TRY_CAST(v.value AS FLOAT) IS NOT NULL;


    PRINT 'Silver Load Complete';
END;
GO

-- Runs full Silver transformation process
EXEC silver.load_silver;

-- Quick sanity checks
SELECT COUNT(*) FROM silver.economic_indicators;
SELECT TOP 10 * FROM silver.economic_indicators;
SELECT * FROM silver.economic_indicators;
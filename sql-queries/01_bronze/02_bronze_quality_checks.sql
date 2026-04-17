SELECT COUNT(*) FROM bronze.world_bank_current_account;
SELECT COUNT(*) FROM bronze.world_bank_gdp_growth;
SELECT COUNT(*) FROM bronze.world_bank_inflation;
SELECT COUNT(*) FROM bronze.world_bank_unemployment;

-- Checking row counts per country
SELECT country_name, COUNT(*) 
FROM bronze.world_bank_current_account
GROUP BY country_name;

-- Checking missing values in year columns
SELECT 
    SUM(CASE WHEN year_2009 IS NULL THEN 1 ELSE 0 END) AS missing_2009
FROM bronze.world_bank_current_account;

-- Checking if numeric columns are actually clean (catching: ".." "N/A" empty strings)
SELECT TOP 20 year_2009
FROM bronze.world_bank_current_account
WHERE year_2009 NOT LIKE '%[0-9.-]%';

-- Checking IMF load consistency
SELECT country, COUNT(*)
FROM bronze.imf_debt_raw
GROUP BY country;

-- Sanity check
SELECT 
    country_name,
    year_2019,
    year_2020,
    year_2021
FROM bronze.world_bank_gdp_growth
WHERE country_name = 'Kenya';
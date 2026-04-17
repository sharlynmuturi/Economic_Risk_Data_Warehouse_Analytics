/*
DDL Script to create tables in the bronze schema
*/

IF OBJECT_ID('bronze.world_bank_current_account', 'U') IS NOT NULL
    DROP TABLE bronze.world_bank_current_account;

CREATE TABLE bronze.world_bank_current_account (
    country_name NVARCHAR(100),
    country_code NVARCHAR(10),
    indicator_name NVARCHAR(255),
    indicator_code NVARCHAR(255),
    year_2005 NVARCHAR(255),
    year_2006 NVARCHAR(255),
    year_2007 NVARCHAR(255),
    year_2008 NVARCHAR(255),
    year_2009 NVARCHAR(255),
    year_2010 NVARCHAR(255),
    year_2011 NVARCHAR(255),
    year_2012 NVARCHAR(255),
    year_2013 NVARCHAR(255),
    year_2014 NVARCHAR(255),
    year_2015 NVARCHAR(255),
    year_2016 NVARCHAR(255),
    year_2017 NVARCHAR(255),
    year_2018 NVARCHAR(255),
    year_2019 NVARCHAR(255),
    year_2020 NVARCHAR(255),
    year_2021 NVARCHAR(255),
    year_2022 NVARCHAR(255),
    year_2023 NVARCHAR(255),
    year_2024 NVARCHAR(255)
);
GO


IF OBJECT_ID('bronze.world_bank_gdp_growth', 'U') IS NOT NULL
    DROP TABLE bronze.world_bank_gdp_growth;

CREATE TABLE bronze.world_bank_gdp_growth (
    country_name NVARCHAR(100),
    country_code NVARCHAR(10),
    indicator_name NVARCHAR(255),
    indicator_code NVARCHAR(255),
    year_2005 NVARCHAR(255),
    year_2006 NVARCHAR(255),
    year_2007 NVARCHAR(255),
    year_2008 NVARCHAR(255),
    year_2009 NVARCHAR(255),
    year_2010 NVARCHAR(255),
    year_2011 NVARCHAR(255),
    year_2012 NVARCHAR(255),
    year_2013 NVARCHAR(255),
    year_2014 NVARCHAR(255),
    year_2015 NVARCHAR(255),
    year_2016 NVARCHAR(255),
    year_2017 NVARCHAR(255),
    year_2018 NVARCHAR(255),
    year_2019 NVARCHAR(255),
    year_2020 NVARCHAR(255),
    year_2021 NVARCHAR(255),
    year_2022 NVARCHAR(255),
    year_2023 NVARCHAR(255),
    year_2024 NVARCHAR(255)
);
GO

IF OBJECT_ID('bronze.world_bank_inflation', 'U') IS NOT NULL
    DROP TABLE bronze.world_bank_inflation;

CREATE TABLE bronze.world_bank_inflation (
    country_name NVARCHAR(100),
    country_code NVARCHAR(10),
    indicator_name NVARCHAR(255),
    indicator_code NVARCHAR(255),
    year_2005 NVARCHAR(255),
    year_2006 NVARCHAR(255),
    year_2007 NVARCHAR(255),
    year_2008 NVARCHAR(255),
    year_2009 NVARCHAR(255),
    year_2010 NVARCHAR(255),
    year_2011 NVARCHAR(255),
    year_2012 NVARCHAR(255),
    year_2013 NVARCHAR(255),
    year_2014 NVARCHAR(255),
    year_2015 NVARCHAR(255),
    year_2016 NVARCHAR(255),
    year_2017 NVARCHAR(255),
    year_2018 NVARCHAR(255),
    year_2019 NVARCHAR(255),
    year_2020 NVARCHAR(255),
    year_2021 NVARCHAR(255),
    year_2022 NVARCHAR(255),
    year_2023 NVARCHAR(255),
    year_2024 NVARCHAR(255)
);
GO

IF OBJECT_ID('bronze.world_bank_unemployment', 'U') IS NOT NULL
    DROP TABLE bronze.world_bank_unemployment;

CREATE TABLE bronze.world_bank_unemployment (
    country_name NVARCHAR(100),
    country_code NVARCHAR(10),
    indicator_name NVARCHAR(255),
    indicator_code NVARCHAR(255),
    year_2005 NVARCHAR(255),
    year_2006 NVARCHAR(255),
    year_2007 NVARCHAR(255),
    year_2008 NVARCHAR(255),
    year_2009 NVARCHAR(255),
    year_2010 NVARCHAR(255),
    year_2011 NVARCHAR(255),
    year_2012 NVARCHAR(255),
    year_2013 NVARCHAR(255),
    year_2014 NVARCHAR(255),
    year_2015 NVARCHAR(255),
    year_2016 NVARCHAR(255),
    year_2017 NVARCHAR(255),
    year_2018 NVARCHAR(255),
    year_2019 NVARCHAR(255),
    year_2020 NVARCHAR(255),
    year_2021 NVARCHAR(255),
    year_2022 NVARCHAR(255),
    year_2023 NVARCHAR(255),
    year_2024 NVARCHAR(255)
);
GO


IF OBJECT_ID('bronze.imf_debt_raw', 'U') IS NOT NULL
    DROP TABLE bronze.imf_debt_raw;

CREATE TABLE bronze.imf_debt_raw (
    country NVARCHAR(100),
    country_code NVARCHAR(10),
    indicator NVARCHAR(255),
    year_2005 NVARCHAR(255),
    year_2006 NVARCHAR(255),
    year_2007 NVARCHAR(255),
    year_2008 NVARCHAR(255),
    year_2009 NVARCHAR(255),
    year_2010 NVARCHAR(255),
    year_2011 NVARCHAR(255),
    year_2012 NVARCHAR(255),
    year_2013 NVARCHAR(255),
    year_2014 NVARCHAR(255),
    year_2015 NVARCHAR(255),
    year_2016 NVARCHAR(255),
    year_2017 NVARCHAR(255),
    year_2018 NVARCHAR(255),
    year_2019 NVARCHAR(255),
    year_2020 NVARCHAR(255),
    year_2021 NVARCHAR(255),
    year_2022 NVARCHAR(255),
    year_2023 NVARCHAR(255),
    year_2024 NVARCHAR(255)
);
GO


/*
Stored Procedure to load data into the bronze schema from external CSV files.
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    SET NOCOUNT ON;

    PRINT 'Loading Bronze Layer...';

   
    -- CURRENT ACCOUNT
    TRUNCATE TABLE bronze.world_bank_current_account;

    BULK INSERT bronze.world_bank_current_account
    FROM 'C:\data\processed\current_account_balance.csv'
    WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0A', CODEPAGE='65001', TABLOCK);

    DELETE FROM bronze.world_bank_current_account
    WHERE country_name IS NULL
       OR country_name = ''


    -- GDP GROWTH
    TRUNCATE TABLE bronze.world_bank_gdp_growth;

    BULK INSERT bronze.world_bank_gdp_growth
    FROM 'C:\data\processed\gdp_growth.csv'
    WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0A', CODEPAGE='65001', TABLOCK);


    -- INFLATION
    TRUNCATE TABLE bronze.world_bank_inflation;

    BULK INSERT bronze.world_bank_inflation
    FROM 'C:\data\processed\inflation.csv'
    WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0A', CODEPAGE='65001', TABLOCK);


    -- UNEMPLOYMENT
    TRUNCATE TABLE bronze.world_bank_unemployment;

    BULK INSERT bronze.world_bank_unemployment
    FROM 'C:\data\processed\unemployment.csv'
    WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0A', CODEPAGE='65001', TABLOCK);


    -- IMF DEBT
    TRUNCATE TABLE bronze.imf_debt_raw;

    BULK INSERT bronze.imf_debt_raw
    FROM 'C:\data\processed\government_debt.csv'
    WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0A', CODEPAGE='65001', TABLOCK);

    PRINT 'Bronze Load Complete';
END;
GO

EXEC bronze.load_bronze;
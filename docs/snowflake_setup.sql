-- Run once in a Snowflake worksheet (trial account). XSMALL + auto-suspend keeps credit use tiny.
CREATE WAREHOUSE IF NOT EXISTS CARBON_WH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
CREATE DATABASE IF NOT EXISTS CARBON;
-- The Python code creates the RAW schema and tables on first connect; dbt creates STAGING / MARTS.

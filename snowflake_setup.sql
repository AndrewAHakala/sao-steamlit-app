-- SAO Cost Savings Estimator - Snowflake Setup
-- =============================================
-- Run this script to set up the Snowflake environment for the SAO estimator

-- 1. Create Database and Schema for sandbox
CREATE DATABASE IF NOT EXISTS SAO_SANDBOX;
USE DATABASE SAO_SANDBOX;

CREATE SCHEMA IF NOT EXISTS PUBLIC;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS MARTS;

-- 2. Create Warehouse for simulations (adjust size as needed)
CREATE WAREHOUSE IF NOT EXISTS SAO_ESTIMATOR_WH
    WAREHOUSE_SIZE = 'MEDIUM'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for SAO cost savings simulations';

-- 3. Create stage for Streamlit app files
CREATE STAGE IF NOT EXISTS SAO_ESTIMATOR_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for SAO Estimator Streamlit app files';

-- 4. Create external access integration for API calls (dbt Cloud, OpenAI)
-- Note: Requires ACCOUNTADMIN role
CREATE OR REPLACE NETWORK RULE sao_external_access_rule
    TYPE = HOST_PORT
    MODE = EGRESS
    VALUE_LIST = (
        'cloud.getdbt.com:443',
        'api.openai.com:443',
        'api.nvidia.com:443',
        'integrate.api.nvidia.com:443'
    );

CREATE OR REPLACE SECRET dbt_cloud_api_token
    TYPE = GENERIC_STRING
    SECRET_STRING = '<YOUR_DBT_CLOUD_API_TOKEN>';

CREATE OR REPLACE SECRET openai_api_key
    TYPE = GENERIC_STRING
    SECRET_STRING = '<YOUR_OPENAI_API_KEY>';

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION sao_external_access
    ALLOWED_NETWORK_RULES = (sao_external_access_rule)
    ALLOWED_AUTHENTICATION_SECRETS = (dbt_cloud_api_token, openai_api_key)
    ENABLED = TRUE
    COMMENT = 'External access for SAO Estimator to call dbt Cloud and OpenAI APIs';

-- 5. Upload app files to stage (run from your local machine)
-- PUT file:///path/to/app.py @SAO_SANDBOX.PUBLIC.SAO_ESTIMATOR_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file:///path/to/requirements.txt @SAO_SANDBOX.PUBLIC.SAO_ESTIMATOR_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file:///path/to/modules/*.py @SAO_SANDBOX.PUBLIC.SAO_ESTIMATOR_STAGE/modules/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file:///path/to/config/*.py @SAO_SANDBOX.PUBLIC.SAO_ESTIMATOR_STAGE/config/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- 6. Create Streamlit app
CREATE OR REPLACE STREAMLIT SAO_COST_ESTIMATOR
    ROOT_LOCATION = '@SAO_SANDBOX.PUBLIC.SAO_ESTIMATOR_STAGE'
    MAIN_FILE = 'app.py'
    QUERY_WAREHOUSE = SAO_ESTIMATOR_WH
    EXTERNAL_ACCESS_INTEGRATIONS = (sao_external_access)
    COMMENT = 'SAO Cost Savings Estimator for dbt Labs prospects';

-- 7. Grant access to the app (adjust role as needed)
GRANT USAGE ON STREAMLIT SAO_COST_ESTIMATOR TO ROLE PUBLIC;

-- 8. View the Streamlit app URL
SHOW STREAMLITS LIKE 'SAO_COST_ESTIMATOR';

-- =============================================
-- Helper queries for monitoring
-- =============================================

-- Check synthetic data tables
-- SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN ('STAGING', 'INTERMEDIATE', 'MARTS');

-- Monitor warehouse usage
-- SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
-- WHERE WAREHOUSE_NAME = 'SAO_ESTIMATOR_WH' 
-- ORDER BY START_TIME DESC LIMIT 100;

-- Check query history for cost analysis
-- SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
-- WHERE WAREHOUSE_NAME = 'SAO_ESTIMATOR_WH'
-- ORDER BY START_TIME DESC LIMIT 100;


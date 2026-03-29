#!/bin/bash
# SAO Cost Savings Estimator - Snowflake Deployment Script
# =========================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

STAGE_NAME="SAO_ESTIMATOR_STAGE"

# Function to prompt for input with default
prompt_with_default() {
    local prompt="$1"
    local default="$2"
    local var_name="$3"
    
    if [[ -n "${!var_name}" ]]; then
        echo "${!var_name}"
    else
        read -p "${prompt} [${default}]: " value
        echo "${value:-$default}"
    fi
}

echo ""
echo -e "${GREEN}🚀 SAO Cost Savings Estimator - Snowflake Deployment${NC}"
echo "====================================================="
echo ""

# Prompt for configuration if not set via environment
echo -e "${YELLOW}Enter your Snowflake configuration:${NC}"
echo "(Press Enter to accept defaults shown in brackets)"
echo ""

SNOWFLAKE_ACCOUNT=$(prompt_with_default "Snowflake Account Locator" "zna84829" "SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER=$(prompt_with_default "Snowflake Username" "ANDREW_HAKALA" "SNOWFLAKE_USER")
SNOWFLAKE_DATABASE=$(prompt_with_default "Database" "POV_PRD_WORKHUMAN" "SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA=$(prompt_with_default "Schema" "PUBLIC" "SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE=$(prompt_with_default "Warehouse" "SAO_ESTIMATOR_WH" "SNOWFLAKE_WAREHOUSE")

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo ""
echo -e "${GREEN}Configuration confirmed:${NC}"
echo "  Account:   ${SNOWFLAKE_ACCOUNT}"
echo "  User:      ${SNOWFLAKE_USER}"
echo "  Database:  ${SNOWFLAKE_DATABASE}"
echo "  Schema:    ${SNOWFLAKE_SCHEMA}"
echo "  Warehouse: ${SNOWFLAKE_WAREHOUSE}"
echo "  Stage:     ${STAGE_NAME}"
echo ""

# Confirm before proceeding
read -p "Proceed with deployment? (y/n): " confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo "Deployment cancelled."
    exit 0
fi
echo ""

# Find snowsql - check common installation locations
SNOWSQL_CMD=""
if command -v snowsql &> /dev/null; then
    SNOWSQL_CMD="snowsql"
elif [[ -f "/Applications/SnowSQL.app/Contents/MacOS/snowsql" ]]; then
    SNOWSQL_CMD="/Applications/SnowSQL.app/Contents/MacOS/snowsql"
elif [[ -f "$HOME/Applications/SnowSQL.app/Contents/MacOS/snowsql" ]]; then
    SNOWSQL_CMD="$HOME/Applications/SnowSQL.app/Contents/MacOS/snowsql"
elif [[ -f "/usr/local/bin/snowsql" ]]; then
    SNOWSQL_CMD="/usr/local/bin/snowsql"
fi

if [[ -z "$SNOWSQL_CMD" ]]; then
    echo -e "${RED}❌ SnowSQL is not installed!${NC}"
    echo ""
    echo "Install it with:"
    echo -e "  ${YELLOW}brew install --cask snowflake-snowsql${NC}"
    echo ""
    echo "Or download from:"
    echo "  https://docs.snowflake.com/en/user-guide/snowsql-install-config"
    exit 1
fi

echo -e "${GREEN}✅ SnowSQL found at: ${SNOWSQL_CMD}${NC}"
echo ""

# Create the SQL commands for file uploads
cat << EOF > /tmp/sao_upload.sql
-- Set context
USE DATABASE ${SNOWFLAKE_DATABASE};
USE SCHEMA ${SNOWFLAKE_SCHEMA};
USE WAREHOUSE ${SNOWFLAKE_WAREHOUSE};

-- Create stage if not exists
CREATE STAGE IF NOT EXISTS ${STAGE_NAME}
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for SAO Estimator Streamlit app files';

-- Upload main app file
PUT file://${SCRIPT_DIR}/app.py @${STAGE_NAME}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Upload requirements
PUT file://${SCRIPT_DIR}/requirements.txt @${STAGE_NAME}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Upload modules
PUT file://${SCRIPT_DIR}/modules/__init__.py @${STAGE_NAME}/modules/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://${SCRIPT_DIR}/modules/dbt_api.py @${STAGE_NAME}/modules/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://${SCRIPT_DIR}/modules/data_generator.py @${STAGE_NAME}/modules/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://${SCRIPT_DIR}/modules/project_generator.py @${STAGE_NAME}/modules/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://${SCRIPT_DIR}/modules/cost_calculator.py @${STAGE_NAME}/modules/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://${SCRIPT_DIR}/modules/visualizations.py @${STAGE_NAME}/modules/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Upload config
PUT file://${SCRIPT_DIR}/config/__init__.py @${STAGE_NAME}/config/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://${SCRIPT_DIR}/config/settings.py @${STAGE_NAME}/config/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- List uploaded files
LIST @${STAGE_NAME}/;

EOF

echo -e "${YELLOW}📤 Uploading files to Snowflake stage...${NC}"
echo ""

# Run the upload
echo -e "${YELLOW}Enter your Snowflake password (paste from 1Password):${NC}"
if "${SNOWSQL_CMD}" -a "${SNOWFLAKE_ACCOUNT}" -u "${SNOWFLAKE_USER}" -f /tmp/sao_upload.sql; then
    echo ""
    echo -e "${GREEN}✅ Files uploaded successfully!${NC}"
    echo ""
    echo -e "${YELLOW}📝 Next steps:${NC}"
    echo ""
    echo "1. Run this SQL in Snowsight to create the Streamlit app:"
    echo ""
    echo -e "${GREEN}   CREATE OR REPLACE STREAMLIT ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.SAO_COST_ESTIMATOR"
    echo "       ROOT_LOCATION = '@${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${STAGE_NAME}'"
    echo "       MAIN_FILE = 'app.py'"
    echo -e "       QUERY_WAREHOUSE = ${SNOWFLAKE_WAREHOUSE};${NC}"
    echo ""
    echo "2. Find your app in Snowsight under 'Streamlit' in the left sidebar"
    echo ""
else
    echo ""
    echo -e "${RED}❌ Upload failed. Check your credentials and try again.${NC}"
    exit 1
fi

# Cleanup
rm -f /tmp/sao_upload.sql


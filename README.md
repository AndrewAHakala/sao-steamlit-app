# 🔄 SAO Cost Savings Estimator

A Streamlit application to help dbt Labs prospects estimate cost savings from **State Aware Orchestration (SAO)** when upgrading from dbt Core to dbt Fusion engine.

## Overview

This tool enables dbt Labs sales engineers to:
1. **Collect pipeline characteristics** from prospects about their current dbt Core deployment
2. **Generate synthetic data** that mirrors the prospect's data profile
3. **Create simulated dbt projects** matching their pipeline structure
4. **Run comparison jobs** in a sandbox environment (Core vs. Fusion)
5. **Quantify SAO impact** with detailed cost savings analysis

## Features

### 📊 Pipeline Configuration
- Model count and distribution
- Materialization ratios (incremental/table/view)
- Source definitions and refresh patterns
- DAG depth and dependency structure
- Data characteristics (row count, column count)

### 🎲 Synthetic Data Generation
- **Claude API** (Anthropic) for intelligent schema and data generation
- Realistic business data patterns driven by customer description prompt
- Referential integrity enforcement across all generated tables

### 🚀 dbt Cloud Integration
- Automated sandbox project creation
- Dual job setup (Core baseline vs. Fusion with SAO)
- Job execution and monitoring via API
- Run results collection and comparison

### 💰 Cost Analysis
- Model skip rate calculation
- Execution time comparison
- Snowflake credit savings
- Monthly/annual cost projections
- ROI estimation

## Installation

### Prerequisites
- Python 3.10+
- dbt Cloud account with API access
- Snowflake account
- Anthropic API key (for Claude-powered data generation)

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd SAO-steamlit-app
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your credentials
```

5. Run the application:
```bash
streamlit run app_snowflake.py
```

## Configuration

### Environment Variables

| Variable | Description |
|----------|-------------|
| `DBT_CLOUD_ACCOUNT_ID` | Your dbt Cloud account ID |
| `DBT_CLOUD_API_TOKEN` | dbt Cloud API token |
| `DBT_CLOUD_SANDBOX_PROJECT_ID` | Sandbox project ID for simulations |
| `ANTHROPIC_API_KEY` | Anthropic API key for Claude data generation |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_WAREHOUSE` | Warehouse for sandbox operations |
| `SNOWFLAKE_DATABASE` | Database for synthetic data |

### Sidebar Configuration

The app also supports runtime configuration via the sidebar:
- dbt Cloud credentials
- Data generation API keys
- Snowflake connection settings

## Usage

### Step 1: Configure Pipeline

Enter the prospect's pipeline characteristics:
- **Number of models** (10-2000)
- **Materialization distribution** (% incremental, table, view)
- **Number of sources**
- **Source refresh frequency**
- **Pipeline run frequency**
- **Data size characteristics**

### Step 2: Run Simulation

The app will:
1. Generate synthetic data based on inputs
2. Create a dbt project structure
3. Set up comparison jobs in the sandbox
4. Execute both Core and Fusion pipelines
5. Collect execution metrics

### Step 3: Review Results

Analyze the SAO impact:
- **Monthly savings** estimate
- **Models skipped** per run
- **Execution time** reduction
- **Annual ROI** projection
- Detailed run-by-run breakdown

## Architecture

```
SAO-streamlit-app/
├── app.py                  # Main Streamlit application
├── modules/
│   ├── dbt_api.py         # dbt Cloud API client
│   ├── data_generator.py  # Synthetic data generation
│   ├── project_generator.py # dbt project structure
│   ├── cost_calculator.py # SAO savings calculations
│   └── visualizations.py  # Charts and graphs
├── config/
│   └── settings.py        # Application configuration
├── requirements.txt
└── README.md
```

## SAO Calculation Methodology

### How SAO Works

State Aware Orchestration tracks:
1. **Source freshness** - Which sources have new data
2. **Model dependencies** - The DAG structure
3. **State changes** - What has actually changed

### Savings Factors

| Materialization | SAO Benefit | Reason |
|-----------------|-------------|--------|
| Incremental | High (70% skip rate) | Only processes new/changed data |
| Table | Medium (50% skip rate) | Can skip when upstream unchanged |
| View | Low (10% skip rate) | Always refresh but minimal compute |

### Cost Calculation

```
Savings = (Core Execution Time - Fusion Execution Time) × Credit Rate × Runs/Month
```

## Deploying to Snowflake

To deploy this as a Streamlit in Snowflake application:

1. Create a stage for the app files
2. Upload all Python files to the stage
3. Create a Streamlit app pointing to `app.py`
4. Grant necessary permissions for external access (APIs)

Example:
```sql
CREATE STAGE sao_estimator_stage;

PUT file://app.py @sao_estimator_stage AUTO_COMPRESS=FALSE;
PUT file://modules/*.py @sao_estimator_stage/modules/ AUTO_COMPRESS=FALSE;

CREATE STREAMLIT sao_estimator
  ROOT_LOCATION = '@sao_estimator_stage'
  MAIN_FILE = 'app.py'
  QUERY_WAREHOUSE = 'COMPUTE_WH';
```

## Contributing

Suggestions for improvement:
- Additional input parameters
- Enhanced visualization options
- Historical comparison tracking
- Integration with existing dbt projects

## License

Internal dbt Labs tool - not for external distribution.


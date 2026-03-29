# SAO Cost Savings Estimator - Setup Guide

## Quick Start (5 minutes)

### Prerequisites
- Python 3.10 or higher
- A Snowflake account
- A dbt Cloud account with API access
- An OpenAI API key
- A GitHub repository for dbt models

### Step 1: Install Dependencies

```bash
# Create a virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Mac/Linux
# or: venv\Scripts\activate  # On Windows

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Run the App Locally

```bash
streamlit run app_snowflake.py
```

The app will open in your browser at `http://localhost:8501`

### Step 3: Configure in the App

In the sidebar, you'll need to provide:

1. **dbt Platform Configuration**
   - Account ID (found in your dbt Cloud URL)
   - API Token (create at Account Settings > API Access)
   - Region/Custom URL (if using single-tenant)

2. **Snowflake Connection**
   - Account ID (e.g., `abc12345.us-east-1`)
   - Username
   - Password
   - Role (e.g., `ACCOUNTADMIN`)
   - Warehouse (e.g., `COMPUTE_WH`)

3. **GitHub Configuration**
   - Personal Access Token (with `repo` scope)
   - Repository name (format: `owner/repo-name`)

4. **OpenAI API Key**
   - For synthetic data generation

---

## What This App Does

1. **Simulates SAO Savings**: Estimates cost savings from dbt's State Aware Orchestration
2. **Generates Synthetic Data**: Creates realistic test data based on customer description
3. **Creates dbt Projects**: Generates complete dbt projects with models, tests, and docs
4. **Deploys to dbt Cloud**: Creates comparison jobs (Core vs Fusion)

---

## File Structure

```
SAO-steamlit-app/
├── app_snowflake.py      # Main application (single-file version)
├── requirements.txt      # Python dependencies
├── SETUP.md             # This file
├── README.md            # Project overview
└── env.example.txt      # Example environment variables
```

---

## Troubleshooting

### "ModuleNotFoundError"
Make sure you activated your virtual environment and ran `pip install -r requirements.txt`

### "Connection timeout" errors
The app includes retry logic, but if you're behind a corporate firewall, you may need to configure proxy settings.

### dbt Cloud API errors
- Verify your API token has the correct permissions
- Check that the Account ID matches your dbt Cloud URL
- For single-tenant deployments, use the "Custom URL" option

### Snowflake connection errors
- Verify your account identifier format
- Check that your role has CREATE DATABASE permissions
- Ensure your warehouse is running

---

## Support

For issues or questions, contact the dbt Labs team.


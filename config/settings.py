"""
Application Settings
====================
Configuration settings for the SAO Cost Savings Estimator.
"""

import os
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class DBTCloudSettings:
    """dbt Cloud API settings."""
    account_id: str = field(default_factory=lambda: os.getenv("DBT_CLOUD_ACCOUNT_ID", ""))
    api_token: str = field(default_factory=lambda: os.getenv("DBT_CLOUD_API_TOKEN", ""))
    sandbox_project_id: str = field(default_factory=lambda: os.getenv("DBT_CLOUD_SANDBOX_PROJECT_ID", ""))
    base_url: str = "https://cloud.getdbt.com/api/v2"
    
    # Job configuration
    default_threads: int = 4
    default_timeout_seconds: int = 3600


@dataclass
class SnowflakeSettings:
    """Snowflake connection settings."""
    account: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_ACCOUNT", ""))
    user: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_USER", ""))
    password: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_PASSWORD", ""))
    warehouse: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"))
    database: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_DATABASE", "SAO_SANDBOX"))
    schema: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"))
    role: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_ROLE", ""))
    
    # Optional key-pair authentication
    private_key_path: Optional[str] = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    )
    
    # Pricing settings
    credit_cost_usd: float = 3.0  # Default credit cost


@dataclass
class DataGeneratorSettings:
    """Synthetic data generation settings."""
    anthropic_api_key: str = field(default_factory=lambda: os.getenv("ANTHROPIC_API_KEY", ""))

    # Generation limits
    max_rows_per_table: int = 100000
    max_preview_rows: int = 1000


@dataclass
class AppSettings:
    """Main application settings."""
    dbt: DBTCloudSettings = field(default_factory=DBTCloudSettings)
    snowflake: SnowflakeSettings = field(default_factory=SnowflakeSettings)
    data_generator: DataGeneratorSettings = field(default_factory=DataGeneratorSettings)
    
    # App configuration
    debug_mode: bool = field(default_factory=lambda: os.getenv("DEBUG", "false").lower() == "true")
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    
    # Simulation defaults
    default_simulation_runs: int = 24
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert settings to dictionary (excluding sensitive data)."""
        return {
            "dbt": {
                "account_id": self.dbt.account_id,
                "base_url": self.dbt.base_url,
                "has_token": bool(self.dbt.api_token),
            },
            "snowflake": {
                "account": self.snowflake.account,
                "warehouse": self.snowflake.warehouse,
                "database": self.snowflake.database,
            },
            "data_generator": {
                "has_anthropic_key": bool(self.data_generator.anthropic_api_key),
            },
        }


# Global settings instance
settings = AppSettings()


# Warehouse pricing reference
WAREHOUSE_PRICING = {
    "X-Small": {"credits_per_hour": 1, "typical_use": "Development, light workloads"},
    "Small": {"credits_per_hour": 2, "typical_use": "Small production workloads"},
    "Medium": {"credits_per_hour": 4, "typical_use": "Standard production"},
    "Large": {"credits_per_hour": 8, "typical_use": "High-volume processing"},
    "X-Large": {"credits_per_hour": 16, "typical_use": "Heavy analytical workloads"},
    "2X-Large": {"credits_per_hour": 32, "typical_use": "Very large data processing"},
}


# SAO benefit factors (based on materialization type)
SAO_BENEFIT_FACTORS = {
    "incremental": {
        "skip_rate": 0.7,  # 70% skip rate when unchanged
        "description": "Highest benefit - processes only new/changed data",
    },
    "table": {
        "skip_rate": 0.5,  # 50% skip rate
        "description": "Medium benefit - can skip when upstream unchanged",
    },
    "view": {
        "skip_rate": 0.1,  # 10% skip rate (views always refresh)
        "description": "Low benefit - views refresh but are cheap",
    },
}


"""
Synthetic Data Generation Module
================================
Generates realistic synthetic data for the SAO simulation using
NVIDIA Data Designer or Snowflake Cortex.
"""

import os
import random
import string
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Try to import data_designer, fall back to local generation
try:
    from data_designer.essentials import (
        DataDesigner,
        DataDesignerConfigBuilder,
        LLMTextColumnConfig,
        SamplerColumnConfig,
        SamplerType,
        CategorySamplerParams,
        PersonSamplerParams,
        IntegerSamplerParams,
        FloatSamplerParams,
        DateSamplerParams,
    )
    DATA_DESIGNER_AVAILABLE = True
except ImportError:
    DATA_DESIGNER_AVAILABLE = False


class SyntheticDataGenerator:
    """
    Generates synthetic data for SAO pipeline simulation.
    
    Uses NVIDIA Data Designer when available, falls back to
    pandas/numpy for basic synthetic data generation.
    """
    
    # Common business data patterns
    BUSINESS_DOMAINS = [
        "customers", "orders", "products", "transactions",
        "events", "sessions", "inventory", "shipments",
        "payments", "users", "accounts", "subscriptions"
    ]
    
    COLUMN_TYPES = {
        "id": {"type": "integer", "unique": True},
        "name": {"type": "string", "pattern": "name"},
        "email": {"type": "string", "pattern": "email"},
        "phone": {"type": "string", "pattern": "phone"},
        "address": {"type": "string", "pattern": "address"},
        "amount": {"type": "float", "min": 0, "max": 10000},
        "quantity": {"type": "integer", "min": 1, "max": 100},
        "date": {"type": "date"},
        "timestamp": {"type": "timestamp"},
        "status": {"type": "category", "values": ["pending", "active", "completed", "cancelled"]},
        "category": {"type": "category"},
        "description": {"type": "text"},
        "price": {"type": "float", "min": 0.01, "max": 9999.99},
        "score": {"type": "float", "min": 0, "max": 100},
        "count": {"type": "integer", "min": 0, "max": 1000000},
        "percentage": {"type": "float", "min": 0, "max": 100},
        "flag": {"type": "boolean"},
    }
    
    def __init__(
        self,
        openai_api_key: Optional[str] = None,
        nvidia_api_key: Optional[str] = None,
        use_data_designer: bool = True
    ):
        """
        Initialize the data generator.
        
        Args:
            openai_api_key: OpenAI API key for Data Designer
            nvidia_api_key: NVIDIA API key for Data Designer
            use_data_designer: Whether to use Data Designer when available
        """
        self.openai_api_key = openai_api_key or os.environ.get("OPENAI_API_KEY")
        self.nvidia_api_key = nvidia_api_key or os.environ.get("NVIDIA_API_KEY")
        self.use_data_designer = use_data_designer and DATA_DESIGNER_AVAILABLE
        
        if self.use_data_designer:
            # Set API key for Data Designer
            if self.openai_api_key:
                os.environ["OPENAI_API_KEY"] = self.openai_api_key
            elif self.nvidia_api_key:
                os.environ["NVIDIA_API_KEY"] = self.nvidia_api_key
            
            try:
                self.data_designer = DataDesigner()
            except Exception:
                self.use_data_designer = False
                self.data_designer = None
        else:
            self.data_designer = None
    
    def generate_pipeline_data(
        self,
        num_sources: int,
        avg_row_count: int = 10000,
        avg_column_count: int = 15,
        seed: int = 42
    ) -> Dict[str, pd.DataFrame]:
        """
        Generate synthetic data for a complete pipeline simulation.
        
        Args:
            num_sources: Number of source tables to generate
            avg_row_count: Average rows per table
            avg_column_count: Average columns per table
            seed: Random seed for reproducibility
            
        Returns:
            Dictionary mapping table names to DataFrames
        """
        np.random.seed(seed)
        random.seed(seed)
        
        tables = {}
        source_names = self._generate_source_names(num_sources)
        
        for source_name in source_names:
            # Vary row and column counts around the average
            row_count = max(100, int(avg_row_count * np.random.uniform(0.3, 2.0)))
            col_count = max(5, int(avg_column_count * np.random.uniform(0.5, 1.5)))
            
            if self.use_data_designer:
                tables[source_name] = self._generate_with_data_designer(
                    source_name, row_count, col_count
                )
            else:
                tables[source_name] = self._generate_with_pandas(
                    source_name, row_count, col_count
                )
        
        return tables
    
    def _generate_source_names(self, num_sources: int) -> List[str]:
        """Generate realistic source table names."""
        prefixes = ["raw", "stg", "src", "landing"]
        suffixes = ["_data", "_events", "_records", "_log", ""]
        
        names = []
        domains_to_use = random.sample(
            self.BUSINESS_DOMAINS * ((num_sources // len(self.BUSINESS_DOMAINS)) + 1),
            num_sources
        )
        
        for i, domain in enumerate(domains_to_use):
            prefix = random.choice(prefixes)
            suffix = random.choice(suffixes)
            names.append(f"{prefix}_{domain}{suffix}")
        
        return names
    
    def _generate_with_data_designer(
        self,
        table_name: str,
        row_count: int,
        col_count: int
    ) -> pd.DataFrame:
        """Generate data using NVIDIA Data Designer."""
        config_builder = DataDesignerConfigBuilder()
        
        # Add primary key
        config_builder.add_column(
            SamplerColumnConfig(
                name="id",
                sampler_type=SamplerType.INTEGER,
                params=IntegerSamplerParams(min_value=1, max_value=row_count * 10)
            )
        )
        
        # Determine table type from name and add appropriate columns
        column_templates = self._get_column_templates(table_name, col_count - 1)
        
        for col_name, col_config in column_templates.items():
            if col_config["type"] == "integer":
                config_builder.add_column(
                    SamplerColumnConfig(
                        name=col_name,
                        sampler_type=SamplerType.INTEGER,
                        params=IntegerSamplerParams(
                            min_value=col_config.get("min", 0),
                            max_value=col_config.get("max", 1000000)
                        )
                    )
                )
            elif col_config["type"] == "float":
                config_builder.add_column(
                    SamplerColumnConfig(
                        name=col_name,
                        sampler_type=SamplerType.FLOAT,
                        params=FloatSamplerParams(
                            min_value=col_config.get("min", 0.0),
                            max_value=col_config.get("max", 10000.0)
                        )
                    )
                )
            elif col_config["type"] == "category":
                values = col_config.get("values", ["A", "B", "C", "D"])
                config_builder.add_column(
                    SamplerColumnConfig(
                        name=col_name,
                        sampler_type=SamplerType.CATEGORY,
                        params=CategorySamplerParams(values=values)
                    )
                )
            elif col_config["type"] == "date":
                config_builder.add_column(
                    SamplerColumnConfig(
                        name=col_name,
                        sampler_type=SamplerType.DATE,
                        params=DateSamplerParams(
                            start_date="2020-01-01",
                            end_date="2024-12-31"
                        )
                    )
                )
            elif col_config["type"] == "text":
                config_builder.add_column(
                    LLMTextColumnConfig(
                        name=col_name,
                        model_alias="openai-text" if self.openai_api_key else "nvidia-text",
                        prompt=f"Generate a brief {col_name} description for a {table_name} record."
                    )
                )
        
        try:
            # Generate preview with Data Designer
            preview = self.data_designer.preview(
                config_builder=config_builder,
                num_records=min(row_count, 1000)  # Limit for API costs
            )
            return preview.to_pandas()
        except Exception as e:
            # Fall back to pandas generation
            print(f"Data Designer error: {e}, falling back to pandas")
            return self._generate_with_pandas(table_name, row_count, col_count)
    
    def _generate_with_pandas(
        self,
        table_name: str,
        row_count: int,
        col_count: int
    ) -> pd.DataFrame:
        """Generate data using pandas/numpy (fallback method)."""
        data = {}
        
        # Primary key
        data["id"] = range(1, row_count + 1)
        
        # Get column templates
        column_templates = self._get_column_templates(table_name, col_count - 1)
        
        for col_name, col_config in column_templates.items():
            data[col_name] = self._generate_column_data(
                col_config, row_count
            )
        
        return pd.DataFrame(data)
    
    def _get_column_templates(
        self,
        table_name: str,
        num_columns: int
    ) -> Dict[str, Dict[str, Any]]:
        """Get column configuration templates based on table type."""
        templates = {}
        
        # Determine table domain from name
        domain = None
        for d in self.BUSINESS_DOMAINS:
            if d in table_name.lower():
                domain = d
                break
        
        # Domain-specific columns
        domain_columns = {
            "customers": [
                ("customer_id", {"type": "integer", "min": 1000, "max": 999999}),
                ("name", {"type": "string", "pattern": "name"}),
                ("email", {"type": "string", "pattern": "email"}),
                ("created_at", {"type": "date"}),
                ("status", {"type": "category", "values": ["active", "inactive", "pending"]}),
                ("tier", {"type": "category", "values": ["bronze", "silver", "gold", "platinum"]}),
            ],
            "orders": [
                ("order_id", {"type": "integer", "min": 100000, "max": 9999999}),
                ("customer_id", {"type": "integer", "min": 1000, "max": 999999}),
                ("order_date", {"type": "date"}),
                ("total_amount", {"type": "float", "min": 1, "max": 10000}),
                ("status", {"type": "category", "values": ["pending", "shipped", "delivered", "cancelled"]}),
                ("shipping_method", {"type": "category", "values": ["standard", "express", "overnight"]}),
            ],
            "products": [
                ("product_id", {"type": "integer", "min": 10000, "max": 99999}),
                ("name", {"type": "string", "pattern": "product_name"}),
                ("category", {"type": "category", "values": ["electronics", "clothing", "home", "sports", "books"]}),
                ("price", {"type": "float", "min": 0.99, "max": 999.99}),
                ("inventory_count", {"type": "integer", "min": 0, "max": 10000}),
                ("rating", {"type": "float", "min": 1, "max": 5}),
            ],
            "transactions": [
                ("transaction_id", {"type": "integer", "min": 1000000, "max": 99999999}),
                ("account_id", {"type": "integer", "min": 10000, "max": 999999}),
                ("amount", {"type": "float", "min": -50000, "max": 50000}),
                ("transaction_date", {"type": "date"}),
                ("transaction_type", {"type": "category", "values": ["debit", "credit", "transfer"]}),
                ("status", {"type": "category", "values": ["completed", "pending", "failed"]}),
            ],
            "events": [
                ("event_id", {"type": "integer", "min": 1, "max": 999999999}),
                ("user_id", {"type": "integer", "min": 1000, "max": 999999}),
                ("event_timestamp", {"type": "timestamp"}),
                ("event_type", {"type": "category", "values": ["click", "view", "purchase", "signup", "logout"]}),
                ("session_id", {"type": "string", "pattern": "uuid"}),
                ("device_type", {"type": "category", "values": ["mobile", "desktop", "tablet"]}),
            ],
        }
        
        # Get domain columns or use generic
        if domain and domain in domain_columns:
            available_columns = domain_columns[domain]
        else:
            # Generic columns
            available_columns = [
                ("record_id", {"type": "integer", "min": 1, "max": 999999999}),
                ("created_at", {"type": "date"}),
                ("updated_at", {"type": "date"}),
                ("value", {"type": "float", "min": 0, "max": 10000}),
                ("status", {"type": "category", "values": ["active", "inactive", "pending", "archived"]}),
                ("type", {"type": "category", "values": ["A", "B", "C", "D"]}),
            ]
        
        # Add columns up to num_columns
        for i, (col_name, col_config) in enumerate(available_columns):
            if i >= num_columns:
                break
            templates[col_name] = col_config
        
        # Fill remaining with generic columns
        generic_types = ["integer", "float", "category", "date"]
        while len(templates) < num_columns:
            col_type = random.choice(generic_types)
            col_name = f"col_{len(templates) + 1}"
            
            if col_type == "integer":
                templates[col_name] = {"type": "integer", "min": 0, "max": 100000}
            elif col_type == "float":
                templates[col_name] = {"type": "float", "min": 0, "max": 1000}
            elif col_type == "category":
                templates[col_name] = {"type": "category", "values": [f"val_{i}" for i in range(5)]}
            else:
                templates[col_name] = {"type": "date"}
        
        return templates
    
    def _generate_column_data(
        self,
        config: Dict[str, Any],
        row_count: int
    ) -> List[Any]:
        """Generate data for a single column based on configuration."""
        col_type = config["type"]
        
        if col_type == "integer":
            min_val = config.get("min", 0)
            max_val = config.get("max", 1000000)
            return np.random.randint(min_val, max_val + 1, size=row_count).tolist()
        
        elif col_type == "float":
            min_val = config.get("min", 0.0)
            max_val = config.get("max", 10000.0)
            return np.round(np.random.uniform(min_val, max_val, size=row_count), 2).tolist()
        
        elif col_type == "category":
            values = config.get("values", ["A", "B", "C"])
            return np.random.choice(values, size=row_count).tolist()
        
        elif col_type == "date":
            start = datetime(2020, 1, 1)
            end = datetime(2024, 12, 31)
            delta = end - start
            return [
                (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")
                for _ in range(row_count)
            ]
        
        elif col_type == "timestamp":
            start = datetime(2020, 1, 1)
            end = datetime(2024, 12, 31)
            delta = end - start
            return [
                (start + timedelta(
                    days=random.randint(0, delta.days),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )).isoformat()
                for _ in range(row_count)
            ]
        
        elif col_type == "boolean":
            return np.random.choice([True, False], size=row_count).tolist()
        
        elif col_type == "string":
            pattern = config.get("pattern", "generic")
            return self._generate_string_data(pattern, row_count)
        
        else:
            # Default to random strings
            return [
                ''.join(random.choices(string.ascii_letters, k=10))
                for _ in range(row_count)
            ]
    
    def _generate_string_data(self, pattern: str, row_count: int) -> List[str]:
        """Generate string data based on pattern type."""
        if pattern == "name":
            first_names = ["James", "Emma", "Liam", "Olivia", "Noah", "Ava", "William", "Sophia"]
            last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
            return [
                f"{random.choice(first_names)} {random.choice(last_names)}"
                for _ in range(row_count)
            ]
        
        elif pattern == "email":
            domains = ["gmail.com", "yahoo.com", "outlook.com", "company.com"]
            return [
                f"user{i}@{random.choice(domains)}"
                for i in range(row_count)
            ]
        
        elif pattern == "phone":
            return [
                f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"
                for _ in range(row_count)
            ]
        
        elif pattern == "uuid":
            import uuid
            return [str(uuid.uuid4()) for _ in range(row_count)]
        
        elif pattern == "product_name":
            adjectives = ["Premium", "Deluxe", "Classic", "Modern", "Essential"]
            nouns = ["Widget", "Gadget", "Tool", "Device", "Kit"]
            return [
                f"{random.choice(adjectives)} {random.choice(nouns)} {random.randint(100, 999)}"
                for _ in range(row_count)
            ]
        
        else:
            return [
                ''.join(random.choices(string.ascii_letters + string.digits, k=12))
                for _ in range(row_count)
            ]
    
    def generate_incremental_updates(
        self,
        base_data: pd.DataFrame,
        change_percentage: float = 30.0,
        seed: int = None
    ) -> pd.DataFrame:
        """
        Generate incremental updates to existing data.
        
        Args:
            base_data: The base DataFrame to update
            change_percentage: Percentage of rows to modify
            seed: Random seed
            
        Returns:
            DataFrame with updated rows
        """
        if seed:
            np.random.seed(seed)
        
        df = base_data.copy()
        num_changes = int(len(df) * change_percentage / 100)
        
        # Select random rows to update
        change_indices = np.random.choice(df.index, size=num_changes, replace=False)
        
        # Update numeric columns with small variations
        for col in df.select_dtypes(include=[np.number]).columns:
            if col != "id":
                df.loc[change_indices, col] = df.loc[change_indices, col] * np.random.uniform(0.9, 1.1)
        
        return df.loc[change_indices]


class SnowflakeCortexGenerator:
    """
    Generate synthetic data using Snowflake Cortex LLM functions.
    
    This is an alternative to NVIDIA Data Designer when running
    directly in Snowflake.
    """
    
    def __init__(self, session):
        """
        Initialize with a Snowpark session.
        
        Args:
            session: Snowpark Session object
        """
        self.session = session
    
    def generate_with_cortex(
        self,
        table_name: str,
        schema: Dict[str, str],
        row_count: int = 1000
    ) -> pd.DataFrame:
        """
        Generate synthetic data using Cortex LLM functions.
        
        Args:
            table_name: Name for the table
            schema: Column name to type mapping
            row_count: Number of rows to generate
            
        Returns:
            Generated DataFrame
        """
        # Build Cortex SQL for data generation
        cortex_prompt = f"""
        Generate {row_count} rows of realistic synthetic data for a table called {table_name}.
        The schema should have these columns: {schema}.
        Return as JSON array.
        """
        
        sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'llama3-70b',
            '{cortex_prompt}'
        ) as generated_data
        """
        
        try:
            result = self.session.sql(sql).collect()
            import json
            data = json.loads(result[0]["GENERATED_DATA"])
            return pd.DataFrame(data)
        except Exception as e:
            print(f"Cortex generation error: {e}")
            # Fall back to basic generation
            generator = SyntheticDataGenerator(use_data_designer=False)
            return generator._generate_with_pandas(table_name, row_count, len(schema))


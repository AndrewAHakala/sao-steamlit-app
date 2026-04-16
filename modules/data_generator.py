"""
Synthetic Data Generation Module
================================
Generates realistic synthetic data for the SAO simulation using
Claude API (Anthropic) for schema and data generation.
"""

import json
import os
import re
import random
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import anthropic


# Snowflake reserved keywords — columns named with these get suffixed with _COL
SNOWFLAKE_RESERVED_KEYWORDS = {
    'TRIGGER', 'ORDER', 'TABLE', 'INDEX', 'SELECT', 'INSERT', 'UPDATE', 'DELETE',
    'CREATE', 'DROP', 'ALTER', 'FROM', 'WHERE', 'JOIN', 'ON', 'AND', 'OR', 'NOT',
    'NULL', 'TRUE', 'FALSE', 'GROUP', 'BY', 'HAVING', 'UNION', 'ALL', 'AS', 'IN',
    'BETWEEN', 'LIKE', 'IS', 'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
    'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'UNIQUE', 'CHECK', 'DEFAULT',
    'CONSTRAINT', 'CASCADE', 'SET', 'VALUES', 'INTO', 'VIEW', 'PROCEDURE',
    'FUNCTION', 'SCHEMA', 'DATABASE', 'USER', 'ROLE', 'GRANT', 'REVOKE',
    'COMMENT', 'COLUMN', 'ROW', 'ROWS', 'LIMIT', 'OFFSET', 'DATE', 'TIME',
    'TIMESTAMP', 'CURRENT', 'SESSION', 'TRANSACTION', 'START', 'BEGIN',
    'FETCH', 'FIRST', 'NEXT', 'ONLY', 'WITH', 'RECURSIVE', 'WINDOW', 'OVER',
    'PARTITION', 'RANK', 'DENSE_RANK', 'ROW_NUMBER', 'LEAD', 'LAG',
    'FIRST_VALUE', 'LAST_VALUE', 'INTERVAL', 'YEAR', 'MONTH', 'DAY', 'HOUR',
    'MINUTE', 'SECOND', 'ZONE', 'AT', 'LOCAL', 'COMMIT', 'ROLLBACK', 'WORK',
    'ISOLATION', 'LEVEL', 'READ', 'WRITE', 'LOCK', 'SHARE', 'EXCLUSIVE',
    'NOWAIT', 'WAIT', 'SKIP', 'LOCKED', 'VALUE', 'TYPE', 'NAME',
    'NUMBER', 'RESULT', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'POSITION'
}


class SyntheticDataGenerator:
    """
    Generates synthetic data for SAO pipeline simulation using Claude API.

    Two-phase approach:
    1. Claude generates an intelligent schema based on the customer's business description
       (includes column types, sample_values for VARCHAR columns, and relationships)
    2. Local pandas/numpy code generates data rows using the schema's sample_values and types

    Claude is used for the intelligent part (schema design). Data generation is local and instant.
    No fallbacks — if Claude API fails, exceptions propagate with clear error messages.
    """

    # Claude model to use for schema generation (needs strong structured output)
    SCHEMA_MODEL = "claude-sonnet-4-6"

    def __init__(self, anthropic_api_key: str):
        """
        Initialize the data generator with an Anthropic API key.

        Args:
            anthropic_api_key: A valid Anthropic API key. Required.

        Raises:
            ValueError: If the API key is empty or None.
        """
        if not anthropic_api_key or not anthropic_api_key.strip():
            raise ValueError(
                "Anthropic API key is required for synthetic data generation. "
                "Please configure it in the sidebar."
            )
        self.api_key = anthropic_api_key.strip()
        self.client = anthropic.Anthropic(api_key=self.api_key)

    # =========================================================================
    # Public API
    # =========================================================================

    def generate_schema_and_data(
        self,
        customer_description: str,
        num_sources: int,
        rows_per_table: int = 1000,
        num_columns: int = 6,
        sample_ddl: Optional[str] = None,
    ) -> dict:
        """
        Full pipeline: schema generation → data generation → reconciliation.

        Args:
            customer_description: Business description from the user prompt.
            num_sources: Number of source tables to generate.
            rows_per_table: Target rows per table.
            num_columns: Target columns per table.
            sample_ddl: Optional DDL statements to guide schema design.

        Returns:
            {"dataframes": {table_name: DataFrame, ...}, "schema": {...}}

        Raises:
            anthropic.AuthenticationError: Invalid API key.
            anthropic.RateLimitError: Rate limited.
            anthropic.APIError: Other API errors.
            ValueError: Invalid schema response from Claude.
        """
        # Step 1: Generate schema with Claude
        schema = self.generate_schema(
            customer_description, num_sources, num_columns, sample_ddl
        )

        # Sanitize column names to avoid Snowflake reserved keyword conflicts
        schema = self._sanitize_schema_columns(schema)

        # Step 2: Generate data with Claude
        dataframes = self.generate_data(schema, rows_per_table)

        # Step 3: Fix FK referential integrity
        dataframes = self._fix_fk_referential_integrity(dataframes, schema)

        # Step 4: Reconcile schema with actual DataFrame columns
        schema = self._reconcile_schema_with_dataframes(schema, dataframes)

        return {"dataframes": dataframes, "schema": schema}

    def generate_schema(
        self,
        customer_description: str,
        num_sources: int,
        num_columns: int = 6,
        sample_ddl: Optional[str] = None,
    ) -> dict:
        """
        Call Claude to generate a table schema from the customer description.

        Returns:
            {"sources": [...], "relationships": [...]}

        Raises:
            ValueError: If Claude returns invalid/empty schema.
            anthropic.APIError: On API failure.
        """
        prompt = self._build_schema_prompt(
            customer_description, num_sources, num_columns, sample_ddl
        )

        with self.client.messages.stream(
            model=self.SCHEMA_MODEL,
            max_tokens=16000,
            temperature=0.5,
            system="You are a data architect. Return only valid JSON, no markdown formatting.",
            messages=[{"role": "user", "content": prompt}],
        ) as stream:
            content = stream.get_final_text().strip()

        # Strip markdown code fences if present
        if content.startswith("```"):
            content = re.sub(r'^```(?:json)?\n?', '', content)
            content = re.sub(r'\n?```$', '', content)

        try:
            schema = json.loads(content)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Claude returned invalid JSON for schema generation: {e}\n"
                f"Response preview: {content[:500]}"
            )

        sources = schema.get("sources", [])
        if not sources:
            raise ValueError("Claude returned an empty schema with no source tables.")

        # Force all _id columns to INTEGER for reliable joins
        for source in sources:
            for col in source.get("columns", []):
                col_name = col.get("name", "").lower()
                if col_name.endswith("_id") or col_name == "id":
                    col["type"] = "INTEGER"

        return schema

    def generate_data(
        self,
        schema: dict,
        rows_per_table: int = 1000,
    ) -> Dict[str, pd.DataFrame]:
        """
        Generate data for each table in the schema using local pandas/numpy.

        Uses the schema's column types and sample_values (provided by Claude
        during schema generation) to create realistic data instantly without
        any additional API calls.

        Returns:
            {table_name: DataFrame, ...}
        """
        dataframes = {}

        for source in schema.get("sources", []):
            table_name = source["name"]
            columns = source.get("columns", [])
            dataframes[table_name] = self._generate_table_locally(
                table_name, columns, rows_per_table
            )

        return dataframes

    def generate_incremental_updates(
        self,
        base_data: pd.DataFrame,
        change_percentage: float = 30.0,
        seed: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Generate incremental updates to existing data.

        Args:
            base_data: The base DataFrame to update.
            change_percentage: Percentage of rows to modify.
            seed: Random seed.

        Returns:
            DataFrame with updated rows.
        """
        if seed:
            np.random.seed(seed)

        df = base_data.copy()
        num_changes = int(len(df) * change_percentage / 100)
        change_indices = np.random.choice(df.index, size=num_changes, replace=False)

        for col in df.select_dtypes(include=[np.number]).columns:
            if col != "id" and not col.endswith("_id"):
                df.loc[change_indices, col] = (
                    df.loc[change_indices, col] * np.random.uniform(0.9, 1.1)
                )

        return df.loc[change_indices]

    # =========================================================================
    # Schema prompt construction
    # =========================================================================

    def _build_schema_prompt(
        self,
        customer_description: str,
        num_sources: int,
        num_columns: int,
        sample_ddl: Optional[str] = None,
    ) -> str:
        """Build the prompt for Claude to generate a table schema."""
        ddl_section = ""
        if sample_ddl and sample_ddl.strip():
            ddl_section = f"""
EXISTING DDL (use as reference — create matching tables plus additional supporting tables):
{sample_ddl}
"""

        return f"""Generate {num_sources} raw source tables for this business: {customer_description}
{ddl_section}
REQUIREMENTS:
- Exactly {num_sources} tables with {num_columns} columns each
- Use operational table names (customers, orders, events, etc.) - NO dim_/fact_ prefixes
- First column must be a primary key ending in _id (INTEGER type)
- ALL columns ending in _id MUST use INTEGER type (for reliable joins across tables)
- Use Snowflake types: VARCHAR, INTEGER, DECIMAL, TIMESTAMP, DATE, BOOLEAN

CRITICAL - Sample Values:
- For EVERY VARCHAR column, include "sample_values" with 4-6 REALISTIC values specific to this business
- Values must be contextually relevant to "{customer_description}"
- NO generic values like "Type_1", "Value_A", "Option_1"
- ALL values MUST use only plain ASCII characters (a-z, A-Z, 0-9, basic punctuation). NO emoji, NO unicode symbols, NO special characters outside standard ASCII
- For email columns, use only standard domains like example.com, company.com, etc.
- Examples of GOOD sample_values:
  - For a recognition platform: ["peer_recognition", "manager_award", "milestone", "spot_bonus"]
  - For an e-commerce site: ["shipped", "delivered", "processing", "returned", "cancelled"]
  - For a streaming service: ["movie", "series", "documentary", "live_event"]
  - For email: ["john.smith@example.com", "jane.doe@company.com"]

CRITICAL - Relationships:
- Include a "relationships" array showing FK→PK mappings between tables
- Each relationship: {{"from_table": "child_table", "from_column": "fk_column_id", "to_table": "parent_table", "to_column": "pk_column_id"}}
- Ensure there are logical foreign key connections between related tables

JSON format (return ONLY valid JSON, no markdown):
{{"sources":[
  {{"name":"table_name","description":"What this table stores","columns":[
    {{"name":"column_id","type":"INTEGER","is_primary_key":true,"description":"Primary key"}},
    {{"name":"column_name","type":"VARCHAR","description":"Description","sample_values":["realistic","business","specific","values"]}}
  ]}}
],
"relationships":[
  {{"from_table":"child_table","from_column":"parent_id","to_table":"parent_table","to_column":"parent_id"}}
]}}"""

    # =========================================================================
    # Local data generation (uses schema sample_values from Claude)
    # =========================================================================

    # Fallback patterns for VARCHAR columns when no sample_values provided
    _NAME_PATTERNS = {
        "first_name": ["James", "Emma", "Liam", "Olivia", "Noah", "Ava", "William", "Sophia", "Lucas", "Mia"],
        "last_name": ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Wilson", "Taylor"],
        "email": None,  # generated dynamically
        "phone": None,  # generated dynamically
        "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego"],
        "state": ["CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI"],
        "country": ["US", "CA", "UK", "DE", "FR", "AU", "JP", "BR", "IN", "MX"],
        "status": ["active", "inactive", "pending", "archived"],
        "type": ["standard", "premium", "basic", "enterprise"],
        "category": ["electronics", "clothing", "home", "sports", "books", "food", "health"],
        "name": ["Alpha Widget", "Beta Device", "Gamma Tool", "Delta Kit", "Epsilon Pro", "Zeta Plus"],
        "description": ["Standard item", "Premium quality", "Best seller", "New arrival", "Limited edition"],
        "method": ["credit_card", "debit_card", "bank_transfer", "paypal", "cash"],
        "source": ["web", "mobile", "in_store", "partner", "referral"],
        "channel": ["online", "retail", "wholesale", "direct"],
    }

    def _generate_table_locally(
        self,
        table_name: str,
        columns: List[dict],
        num_rows: int,
    ) -> pd.DataFrame:
        """Generate data for a table using local pandas/numpy.

        Uses column types and sample_values from the Claude-generated schema
        to create realistic data without any API calls.
        """
        data = {}

        for col in columns:
            col_name = col["name"]
            col_type = col.get("type", "VARCHAR").upper()
            is_pk = col.get("is_primary_key", False)
            sample_values = col.get("sample_values")

            if is_pk:
                data[col_name] = list(range(1, num_rows + 1))

            elif col_name.upper().endswith("_ID") and col_type in ("INTEGER", "INT"):
                fk_max = max(100, num_rows // 2)
                data[col_name] = np.random.randint(1, fk_max + 1, size=num_rows).tolist()

            elif col_type in ("DECIMAL", "NUMERIC", "NUMBER", "FLOAT", "DOUBLE"):
                # Use realistic ranges based on column name
                low, high = self._guess_numeric_range(col_name)
                data[col_name] = np.round(
                    np.random.uniform(low, high, size=num_rows), 2
                ).tolist()

            elif col_type in ("INTEGER", "INT"):
                low, high = self._guess_integer_range(col_name)
                data[col_name] = np.random.randint(low, high + 1, size=num_rows).tolist()

            elif col_type in ("TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ"):
                start = datetime(2023, 1, 1)
                end = datetime(2025, 12, 31)
                delta = (end - start).total_seconds()
                random_seconds = np.random.uniform(0, delta, size=num_rows)
                data[col_name] = [
                    (start + timedelta(seconds=s)).isoformat()
                    for s in random_seconds
                ]

            elif col_type == "DATE":
                start = datetime(2023, 1, 1)
                end = datetime(2025, 12, 31)
                delta_days = (end - start).days
                random_days = np.random.randint(0, delta_days + 1, size=num_rows)
                data[col_name] = [
                    (start + timedelta(days=int(d))).strftime("%Y-%m-%d")
                    for d in random_days
                ]

            elif col_type == "BOOLEAN":
                data[col_name] = np.random.choice(
                    [True, False], size=num_rows
                ).tolist()

            elif col_type == "VARCHAR":
                data[col_name] = self._generate_varchar_column(
                    col_name, num_rows, sample_values
                )

            else:
                # Default: treat as VARCHAR
                data[col_name] = self._generate_varchar_column(
                    col_name, num_rows, sample_values
                )

        return pd.DataFrame(data)

    # Column name patterns that should always produce unique values per row
    _UNIQUE_COLUMN_PATTERNS = (
        "email", "username", "user_name", "login", "phone", "mobile",
        "account_number", "account_no", "sku", "serial", "license",
        "url", "website", "ip_address",
    )

    def _is_unique_column(self, col_name: str) -> bool:
        """Check if a column should have unique values per row."""
        cl = col_name.lower()
        return any(pat in cl for pat in self._UNIQUE_COLUMN_PATTERNS)

    def _generate_varchar_column(
        self, col_name: str, num_rows: int, sample_values: Optional[List[str]]
    ) -> List[str]:
        """Generate VARCHAR column data using sample_values or name-based heuristics."""
        col_lower = col_name.lower()

        # --- Unique columns: always generate unique values per row ---
        if self._is_unique_column(col_lower):
            return self._generate_unique_varchar(col_lower, num_rows, sample_values)

        # --- Non-unique columns: sample from sample_values or patterns ---
        if sample_values and isinstance(sample_values, list) and len(sample_values) > 0:
            clean_values = [str(v) for v in sample_values if v is not None]
            if clean_values:
                return np.random.choice(clean_values, size=num_rows).tolist()

        if "address" in col_lower:
            streets = ["Main St", "Oak Ave", "Elm Blvd", "Pine Rd", "Cedar Ln", "Maple Dr"]
            return [f"{random.randint(1, 9999)} {random.choice(streets)}" for _ in range(num_rows)]

        # Try matching against known pattern names
        for pattern_key, values in self._NAME_PATTERNS.items():
            if pattern_key in col_lower and values is not None:
                return np.random.choice(values, size=num_rows).tolist()

        # If column name contains "name" generically
        if "name" in col_lower:
            return [f"{col_name.replace('_', ' ').title()} {i}" for i in range(1, num_rows + 1)]

        # Last resort: generic string values
        return [f"{col_lower}_{i}" for i in range(1, num_rows + 1)]

    def _generate_unique_varchar(
        self, col_lower: str, num_rows: int, sample_values: Optional[List[str]]
    ) -> List[str]:
        """Generate unique-per-row values for columns like email, username, phone."""
        if "email" in col_lower:
            # Use sample_values domains if available, otherwise defaults
            domains = ["example.com", "company.com", "business.org", "mail.com"]
            if sample_values:
                # Extract domains from sample email values
                extracted = [v.split("@")[1] for v in sample_values if "@" in str(v)]
                if extracted:
                    domains = list(set(extracted))
            first_names = self._NAME_PATTERNS.get("first_name", ["user"])
            last_names = self._NAME_PATTERNS.get("last_name", ["doe"])
            return [
                f"{random.choice(first_names).lower()}.{random.choice(last_names).lower()}{i}@{random.choice(domains)}"
                for i in range(1, num_rows + 1)
            ]

        if "username" in col_lower or "user_name" in col_lower or "login" in col_lower:
            if sample_values:
                # Use sample values as base patterns with unique suffix
                bases = [str(v) for v in sample_values if v is not None]
                return [f"{random.choice(bases)}_{i}" for i in range(1, num_rows + 1)]
            adjectives = ["Swift", "Brave", "Clever", "Lucky", "Bold", "Keen"]
            nouns = ["Traveler", "Explorer", "Nomad", "Rider", "Seeker", "Ranger"]
            return [
                f"{random.choice(adjectives)}{random.choice(nouns)}{i}"
                for i in range(1, num_rows + 1)
            ]

        if "phone" in col_lower or "mobile" in col_lower:
            # Use sequential component to guarantee uniqueness
            return [
                f"+1-{random.randint(200, 999)}-555-{i:04d}"
                for i in range(1, num_rows + 1)
            ]

        if "ip_address" in col_lower:
            return [
                f"192.168.{(i // 256) % 256}.{i % 256}"
                for i in range(1, num_rows + 1)
            ]

        if "url" in col_lower or "website" in col_lower:
            return [f"https://example.com/page/{i}" for i in range(1, num_rows + 1)]

        # Generic unique fallback
        return [f"{col_lower}_{i}" for i in range(1, num_rows + 1)]

    @staticmethod
    def _guess_numeric_range(col_name: str) -> tuple:
        """Guess a realistic numeric range based on column name."""
        cl = col_name.lower()
        if "price" in cl or "cost" in cl or "amount" in cl or "total" in cl:
            return (0.99, 9999.99)
        if "rate" in cl or "percentage" in cl or "pct" in cl:
            return (0.0, 100.0)
        if "score" in cl or "rating" in cl:
            return (1.0, 5.0)
        if "weight" in cl or "height" in cl:
            return (0.1, 500.0)
        if "lat" in cl:
            return (-90.0, 90.0)
        if "lon" in cl or "lng" in cl:
            return (-180.0, 180.0)
        return (0.01, 10000.0)

    @staticmethod
    def _guess_integer_range(col_name: str) -> tuple:
        """Guess a realistic integer range based on column name."""
        cl = col_name.lower()
        if "quantity" in cl or "qty" in cl or "count" in cl:
            return (1, 500)
        if "age" in cl:
            return (18, 85)
        if "year" in cl:
            return (2020, 2025)
        if "day" in cl:
            return (1, 31)
        if "month" in cl:
            return (1, 12)
        return (0, 10000)

    # =========================================================================
    # Post-processing: referential integrity
    # =========================================================================

    def _fix_fk_referential_integrity(
        self, dataframes: Dict[str, pd.DataFrame], schema: dict
    ) -> Dict[str, pd.DataFrame]:
        """Post-process generated DataFrames so FK columns reference actual PK values.

        Uses schema['relationships'] to identify FK→PK mappings, then replaces
        FK column values in child tables with values sampled from the parent table's
        PK column. Also ensures PK columns have unique sequential integer values.
        """
        # Build case-insensitive lookup for dataframes
        df_lookup = {k.upper(): k for k in dataframes}

        # First pass: ensure all PK columns have unique sequential integer IDs
        # and build a map of PK column name → (table_key, column_name)
        pk_column_map: Dict[str, tuple] = {}
        for source in schema.get("sources", []):
            table_name = source["name"]
            df_key = df_lookup.get(table_name.upper())
            if not df_key:
                continue
            df = dataframes[df_key]
            for col in source.get("columns", []):
                if col.get("is_primary_key"):
                    col_name = col["name"]
                    matching_cols = [c for c in df.columns if c.upper() == col_name.upper()]
                    if matching_cols:
                        actual_col = matching_cols[0]
                        df[actual_col] = range(1, len(df) + 1)
                        pk_column_map[col_name.upper()] = (df_key, actual_col)

        # Build relationships list — use explicit ones from schema, plus infer
        # from shared _id column names
        relationships = list(schema.get("relationships", []))
        explicit_fks = {
            (r["from_table"].upper(), r["from_column"].upper()) for r in relationships
        }

        for source in schema.get("sources", []):
            table_name = source["name"]
            df_key = df_lookup.get(table_name.upper())
            if not df_key:
                continue
            for col in source.get("columns", []):
                col_upper = col["name"].upper()
                if col.get("is_primary_key"):
                    continue
                if (
                    col_upper in pk_column_map
                    and (table_name.upper(), col_upper) not in explicit_fks
                ):
                    parent_key, parent_col = pk_column_map[col_upper]
                    if parent_key != df_key:
                        relationships.append({
                            "from_table": table_name,
                            "from_column": col["name"],
                            "to_table": parent_key,
                            "to_column": parent_col,
                        })

        # Second pass: replace FK values with sampled PK values from parent tables
        for rel in relationships:
            from_table = rel.get("from_table", "").upper()
            from_column = rel.get("from_column", "").upper()
            to_table = rel.get("to_table", "").upper()
            to_column = rel.get("to_column", "").upper()

            child_key = df_lookup.get(from_table)
            parent_key = df_lookup.get(to_table)
            if not child_key or not parent_key:
                continue

            child_df = dataframes[child_key]
            parent_df = dataframes[parent_key]

            child_fk_cols = [c for c in child_df.columns if c.upper() == from_column]
            parent_pk_cols = [c for c in parent_df.columns if c.upper() == to_column]
            if not child_fk_cols or not parent_pk_cols:
                continue

            child_fk_col = child_fk_cols[0]
            parent_pk_col = parent_pk_cols[0]

            parent_pk_values = parent_df[parent_pk_col].dropna().values
            if len(parent_pk_values) == 0:
                continue

            sampled_fks = np.random.choice(
                parent_pk_values, size=len(child_df), replace=True
            )
            child_df[child_fk_col] = sampled_fks

        return dataframes

    # =========================================================================
    # Post-processing: schema ↔ data reconciliation
    # =========================================================================

    def _reconcile_schema_with_dataframes(
        self, schema: dict, dataframes: Dict[str, pd.DataFrame]
    ) -> dict:
        """Update schema to match actual DataFrame columns.

        The Claude-generated schema may differ from what the data generation
        actually produced. This ensures the schema (used for DDL and dbt project
        generation) reflects the real columns in the generated data.
        """
        reconciled_sources = []
        df_lookup = {k.lower(): (k, v) for k, v in dataframes.items()}

        for source in schema.get("sources", []):
            source_name = source["name"]
            key = source_name.lower()

            if key not in df_lookup:
                reconciled_sources.append(source)
                continue

            _, df = df_lookup[key]
            actual_cols = list(df.columns)

            original_col_map = {}
            for col in source.get("columns", []):
                sanitized = _sanitize_col_name(col["name"]).upper()
                original_col_map[sanitized] = col

            reconciled_columns = []
            for col_name in actual_cols:
                col_upper = col_name.upper()
                if col_upper in original_col_map:
                    orig = original_col_map[col_upper].copy()
                    orig["name"] = col_upper
                    reconciled_columns.append(orig)
                else:
                    dtype = str(df[col_name].dtype)
                    if "int" in dtype:
                        col_type = "INTEGER"
                    elif "float" in dtype:
                        col_type = "DECIMAL"
                    elif "datetime" in dtype:
                        col_type = "TIMESTAMP"
                    elif "bool" in dtype:
                        col_type = "BOOLEAN"
                    else:
                        col_type = "VARCHAR"

                    is_pk = col_upper.endswith("_ID") and len(reconciled_columns) == 0
                    reconciled_columns.append({
                        "name": col_upper,
                        "type": col_type,
                        "description": col_upper.replace("_", " ").title(),
                        "is_primary_key": is_pk,
                    })

            reconciled_sources.append({
                "name": source_name,
                "description": source.get("description", ""),
                "columns": reconciled_columns,
            })

        return {
            "sources": reconciled_sources,
            "relationships": schema.get("relationships", []),
        }

    # =========================================================================
    # Column name sanitization (Snowflake reserved keywords)
    # =========================================================================

    def _sanitize_schema_columns(self, schema: dict) -> dict:
        """Sanitize all column names in schema to avoid Snowflake reserved keyword conflicts."""
        sanitized_schema = {
            "sources": [],
            "relationships": schema.get("relationships", []),
        }

        for source in schema.get("sources", []):
            sanitized_source = {
                "name": source["name"],
                "description": source.get("description", ""),
                "columns": [],
            }

            for col in source.get("columns", []):
                sanitized_col = col.copy()
                sanitized_col["name"] = _sanitize_col_name(col["name"])
                sanitized_source["columns"].append(sanitized_col)

            sanitized_schema["sources"].append(sanitized_source)

        return sanitized_schema


# Module-level utility so other modules can use it without instantiating the class
def _sanitize_col_name(col_name: str) -> str:
    """Sanitize column name — append _COL if it's a Snowflake reserved keyword."""
    upper_name = col_name.upper()
    if upper_name in SNOWFLAKE_RESERVED_KEYWORDS:
        return f"{upper_name}_COL"
    return upper_name

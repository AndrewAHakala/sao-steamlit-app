"""
SAO Cost Savings Estimator - Snowflake Streamlit App (Single File Version)
==========================================================================
Helps dbt Labs prospects estimate cost savings from State Aware Orchestration (SAO)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import numpy as np
import random
import requests
import time
import re
import os

# Try to import optional dependencies
try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    SNOWFLAKE_CONNECTOR_AVAILABLE = True
except ImportError:
    SNOWFLAKE_CONNECTOR_AVAILABLE = False

from modules.data_generator import SyntheticDataGenerator


# NOTE: SyntheticDataGenerator is imported from modules.data_generator
# (see import at top of file)


class _LegacyDDLGenerator:
    """Retained DDL generation logic used by SnowflakeDataLoader.

    The schema-to-DDL conversion was part of the old inline SyntheticDataGenerator
    but is only needed for Snowflake table creation, not for data generation.
    """

    DDL_RESERVED_KEYWORDS = {
        'TRIGGER', 'ORDER', 'TABLE', 'INDEX', 'SELECT', 'INSERT', 'UPDATE', 'DELETE',
        'CREATE', 'DROP', 'ALTER', 'FROM', 'WHERE', 'JOIN', 'ON', 'AND', 'OR', 'NOT',
        'NULL', 'TRUE', 'FALSE', 'GROUP', 'BY', 'HAVING', 'UNION', 'ALL', 'AS', 'IN',
        'BETWEEN', 'LIKE', 'IS', 'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
        'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'UNIQUE', 'CHECK', 'DEFAULT',
        'CONSTRAINT', 'CASCADE', 'SET', 'VALUES', 'INTO', 'VIEW', 'PROCEDURE',
        'FUNCTION', 'SCHEMA', 'DATABASE', 'USER', 'ROLE', 'GRANT', 'REVOKE',
        'COMMENT', 'COLUMN', 'ROW', 'ROWS', 'LIMIT', 'OFFSET', 'DATE', 'TIME',
        'TIMESTAMP', 'CURRENT', 'SESSION', 'TRANSACTION', 'START', 'BEGIN', 'END',
        'FETCH', 'FIRST', 'NEXT', 'ONLY', 'WITH', 'RECURSIVE', 'WINDOW', 'OVER',
        'PARTITION', 'RANK', 'DENSE_RANK', 'ROW_NUMBER', 'LEAD', 'LAG',
        'FIRST_VALUE', 'LAST_VALUE', 'INTERVAL', 'YEAR', 'MONTH', 'DAY', 'HOUR',
        'MINUTE', 'SECOND', 'ZONE', 'AT', 'LOCAL', 'COMMIT', 'ROLLBACK', 'WORK',
        'ISOLATION', 'LEVEL', 'READ', 'WRITE', 'LOCK', 'SHARE', 'EXCLUSIVE',
        'NOWAIT', 'WAIT', 'SKIP', 'LOCKED', 'VALUE', 'TYPE', 'NAME',
        'NUMBER', 'RESULT', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'POSITION'
    }

    @classmethod
    def sanitize_ddl_column(cls, col_name: str) -> str:
        upper_name = col_name.upper()
        if upper_name in cls.DDL_RESERVED_KEYWORDS:
            return f"{upper_name}_COL"
        return upper_name

    @classmethod
    def generate_create_tables(cls, schema: dict, database: str = "RAW_DATA", schema_name: str = "PUBLIC") -> str:
        statements = [f"-- Create database and schema\nCREATE DATABASE IF NOT EXISTS {database};\nCREATE SCHEMA IF NOT EXISTS {database}.{schema_name};\nUSE DATABASE {database};\nUSE SCHEMA {schema_name};\n"]
        for source in schema.get("sources", []):
            table_name = source["name"].upper()
            columns = []
            for col in source.get("columns", []):
                col_name = cls.sanitize_ddl_column(col["name"])
                col_lower = col["name"].lower()
                if col_lower.endswith("_id") or col_lower == "id":
                    col_type = "INTEGER"
                else:
                    col_type = col.get("type", "VARCHAR").upper()
                    if col_type == "VARCHAR":
                        col_type = "VARCHAR(500)"
                    elif col_type in ("DECIMAL", "NUMERIC", "NUMBER"):
                        col_type = "DECIMAL(18,2)"
                columns.append(f"    {col_name} {col_type}")
            create_stmt = f"CREATE OR REPLACE TABLE {table_name} (\n" + ",\n".join(columns) + "\n);\n"
            statements.append(create_stmt)
        return "\n".join(statements)


# =============================================================================
# SNOWFLAKE DATA LOADER
# =============================================================================

class SnowflakeDataLoader:
    """Load synthetic data into Snowflake."""
    
    def __init__(self, account: str, user: str, password: str, warehouse: str, 
                 database: str = "RAW_DATA", schema: str = "PUBLIC", role: str = "ACCOUNTADMIN"):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.conn = None
        self.last_error = None
    
    def connect(self, skip_database: bool = False) -> bool:
        """Establish connection to Snowflake.
        
        Args:
            skip_database: If True, connect without specifying database/schema (for creating them)
        """
        if not SNOWFLAKE_CONNECTOR_AVAILABLE:
            self.last_error = "snowflake-connector-python not installed. Run: pip install snowflake-connector-python"
            return False
        
        try:
            # Clean up account identifier - remove .snowflakecomputing.com if present
            account = self.account
            if '.snowflakecomputing.com' in account:
                account = account.replace('.snowflakecomputing.com', '')
            
            conn_params = {
                "user": self.user,
                "password": self.password,
                "account": account,
                "warehouse": self.warehouse,
                "role": self.role
            }
            
            # Only include database/schema if not skipping (for initial setup)
            if not skip_database:
                conn_params["database"] = self.database
                conn_params["schema"] = self.schema
            
            self.conn = snowflake.connector.connect(**conn_params)
            return True
        except Exception as e:
            self.last_error = str(e)
            return False
    
    def setup_database_and_schema(self) -> dict:
        """Create the database and schema if they don't exist."""
        try:
            # Connect without specifying database first
            if self.conn:
                self.conn.close()
            
            if not self.connect(skip_database=True):
                return {"success": False, "error": f"Failed to connect: {self.last_error}"}
            
            cursor = self.conn.cursor()
            
            # Create or replace database
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            
            # Use the database
            cursor.execute(f"USE DATABASE {self.database}")
            
            # Create schema if not PUBLIC
            if self.schema.upper() != "PUBLIC":
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            
            # Use the schema
            cursor.execute(f"USE SCHEMA {self.schema}")
            
            cursor.close()
            
            return {"success": True, "message": f"Database {self.database} and schema {self.schema} ready"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def execute_sql(self, sql: str) -> dict:
        """Execute SQL statement."""
        if not self.conn:
            return {"success": False, "error": "Not connected to Snowflake"}
        
        try:
            cursor = self.conn.cursor()
            # Split by semicolon and execute each statement
            statements = [s.strip() for s in sql.split(';') if s.strip()]
            for stmt in statements:
                cursor.execute(stmt)
            return {"success": True, "message": f"Executed {len(statements)} statements"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def create_tables(self, ddl_sql: str) -> dict:
        """Create tables from DDL statements."""
        return self.execute_sql(ddl_sql)
    
    # Comprehensive Snowflake reserved keywords - must match SyntheticDataGenerator and DDLGenerator
    SNOWFLAKE_RESERVED_KEYWORDS = {
        'TRIGGER', 'ORDER', 'TABLE', 'INDEX', 'SELECT', 'INSERT', 'UPDATE', 'DELETE',
        'CREATE', 'DROP', 'ALTER', 'FROM', 'WHERE', 'JOIN', 'ON', 'AND', 'OR', 'NOT',
        'NULL', 'TRUE', 'FALSE', 'GROUP', 'BY', 'HAVING', 'UNION', 'ALL', 'AS', 'IN',
        'BETWEEN', 'LIKE', 'IS', 'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
        'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'UNIQUE', 'CHECK', 'DEFAULT',
        'CONSTRAINT', 'CASCADE', 'SET', 'VALUES', 'INTO', 'VIEW', 'PROCEDURE',
        'FUNCTION', 'SCHEMA', 'DATABASE', 'USER', 'ROLE', 'GRANT', 'REVOKE',
        'COMMENT', 'COLUMN', 'ROW', 'ROWS', 'LIMIT', 'OFFSET', 'FETCH', 'FIRST',
        'NEXT', 'ONLY', 'WITH', 'RECURSIVE', 'WINDOW', 'OVER', 'PARTITION',
        'RANK', 'DENSE_RANK', 'ROW_NUMBER', 'LEAD', 'LAG', 'FIRST_VALUE', 'LAST_VALUE',
        'DATE', 'TIME', 'TIMESTAMP', 'INTERVAL', 'YEAR', 'MONTH', 'DAY', 'HOUR',
        'MINUTE', 'SECOND', 'ZONE', 'AT', 'LOCAL', 'CURRENT', 'SESSION', 'TRANSACTION',
        'COMMIT', 'ROLLBACK', 'START', 'BEGIN', 'WORK', 'ISOLATION', 'LEVEL', 'READ',
        'WRITE', 'LOCK', 'SHARE', 'EXCLUSIVE', 'NOWAIT', 'WAIT', 'SKIP', 'LOCKED',
        'END', 'VALUE', 'TYPE', 'NAME', 'NUMBER', 'RESULT', 'COUNT', 'SUM', 'AVG',
        'MIN', 'MAX', 'POSITION'
    }
    
    def _sanitize_column_name(self, col_name: str) -> str:
        """Sanitize column name to avoid reserved keyword conflicts."""
        upper_name = col_name.upper()
        if upper_name in self.SNOWFLAKE_RESERVED_KEYWORDS:
            # Append _COL suffix to avoid reserved keyword conflict
            return f"{upper_name}_COL"
        return upper_name
    
    def load_dataframe(self, df: pd.DataFrame, table_name: str) -> dict:
        """Load a pandas DataFrame into a Snowflake table."""
        if not self.conn:
            return {"success": False, "error": "Not connected to Snowflake"}
        
        if not SNOWFLAKE_CONNECTOR_AVAILABLE:
            return {"success": False, "error": "snowflake-connector-python not installed"}
        
        try:
            # Sanitize column names - handle reserved keywords
            df = df.copy()
            df.columns = [self._sanitize_column_name(c) for c in df.columns]
            
            success, nchunks, nrows, _ = write_pandas(
                self.conn, 
                df, 
                table_name.upper(),
                database=self.database,
                schema=self.schema,
                quote_identifiers=False  # We've already sanitized names
            )
            
            if success:
                return {"success": True, "rows": nrows, "chunks": nchunks}
            else:
                return {"success": False, "error": "write_pandas returned False"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def load_all_dataframes(self, dataframes: dict) -> dict:
        """Load multiple DataFrames into Snowflake tables."""
        results = {"success": [], "failed": []}
        
        for table_name, df in dataframes.items():
            result = self.load_dataframe(df, table_name)
            if result.get("success"):
                results["success"].append({
                    "table": table_name,
                    "rows": result.get("rows", 0)
                })
            else:
                results["failed"].append({
                    "table": table_name,
                    "error": result.get("error", "Unknown error")
                })
        
        return results
    
    def close(self):
        """Close the connection."""
        if self.conn:
            self.conn.close()
            self.conn = None


# =============================================================================
# GITHUB CLIENT FOR REPO MANAGEMENT
# =============================================================================

class GitHubClient:
    """GitHub API client for pushing files to repositories."""
    
    def __init__(self, token: str):
        self.token = token
        self.base_url = "https://api.github.com"
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }
        self.last_error = None
        self.timeout = 30  # 30 second timeout
        self.max_retries = 3
    
    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make a request with timeout and retry logic."""
        kwargs.setdefault('timeout', self.timeout)
        kwargs.setdefault('headers', self.headers)
        
        last_error = None
        for attempt in range(self.max_retries):
            try:
                if method == 'get':
                    return requests.get(url, **kwargs)
                elif method == 'put':
                    return requests.put(url, **kwargs)
                elif method == 'delete':
                    return requests.delete(url, **kwargs)
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                    continue
                raise
        raise last_error
    
    def get_repo_info(self, owner: str, repo: str) -> dict:
        """Get repository information."""
        try:
            response = self._request_with_retry('get', f"{self.base_url}/repos/{owner}/{repo}")
            if response.status_code == 200:
                return response.json()
            self.last_error = f"Status {response.status_code}: {response.text[:200]}"
        except Exception as e:
            self.last_error = f"Connection error: {str(e)}"
        return None
    
    def get_default_branch(self, owner: str, repo: str) -> str:
        """Get the default branch name."""
        repo_info = self.get_repo_info(owner, repo)
        if repo_info:
            return repo_info.get("default_branch", "main")
        return "main"
    
    def get_branch_sha(self, owner: str, repo: str, branch: str) -> str:
        """Get the SHA of a branch."""
        try:
            response = self._request_with_retry('get', 
                f"{self.base_url}/repos/{owner}/{repo}/git/refs/heads/{branch}")
            if response.status_code == 200:
                return response.json()["object"]["sha"]
        except Exception:
            pass
        return None
    
    def _ensure_branch_exists(self, owner: str, repo: str, branch: str = "main") -> bool:
        """Ensure the branch exists. For new repos, create initial commit."""
        try:
            # Check if branch exists
            response = self._request_with_retry('get',
                f"{self.base_url}/repos/{owner}/{repo}/branches/{branch}")
            if response.status_code == 200:
                return True
            
            # Branch doesn't exist - check if repo is empty (new)
            # Try to create an initial README to bootstrap the repo
            import base64
            st.info(f"  📝 Initializing empty repository with README...")
            
            init_response = self._request_with_retry('put',
                f"{self.base_url}/repos/{owner}/{repo}/contents/README.md",
                json={
                    "message": "Initial commit - SAO Sandbox Setup",
                    "content": base64.b64encode(b"# SAO Sandbox\n\nGenerated dbt project for State Aware Orchestration testing.\n").decode()
                }
            )
            
            if init_response.status_code in [200, 201]:
                st.success(f"  ✅ Repository initialized successfully!")
                return True
            else:
                st.warning(f"  ⚠️ Could not initialize repo: {init_response.status_code}")
                return False
                
        except Exception as e:
            st.warning(f"  ⚠️ Branch check failed: {str(e)}")
            return False
    
    def create_or_update_file(self, owner: str, repo: str, path: str, content: str, 
                               message: str, branch: str = "main") -> bool:
        """Create or update a file in the repository."""
        import base64
        
        try:
            # Check if file exists to get its SHA
            existing_sha = None
            try:
                response = self._request_with_retry('get',
                    f"{self.base_url}/repos/{owner}/{repo}/contents/{path}?ref={branch}")
                if response.status_code == 200:
                    existing_sha = response.json().get("sha")
            except Exception:
                pass  # File doesn't exist, that's fine
            
            # Prepare the request
            data = {
                "message": message,
                "content": base64.b64encode(content.encode()).decode(),
                "branch": branch
            }
            if existing_sha:
                data["sha"] = existing_sha
            
            # Create/update the file
            response = self._request_with_retry('put',
                f"{self.base_url}/repos/{owner}/{repo}/contents/{path}",
                json=data
            )
            
            if response.status_code in [200, 201]:
                return True
            self.last_error = f"Status {response.status_code}: {response.text[:200]}"
        except Exception as e:
            self.last_error = f"Connection error: {str(e)}"
        return False
    
    def push_multiple_files(self, owner: str, repo: str, files: dict, 
                            message: str, branch: str = "main") -> dict:
        """Push multiple files to the repository."""
        results = {"success": [], "failed": []}
        
        for path, content in files.items():
            if self.create_or_update_file(owner, repo, path, content, message, branch):
                results["success"].append(path)
            else:
                results["failed"].append({"path": path, "error": self.last_error})
        
        return results
    
    def list_repo_contents(self, owner: str, repo: str, path: str = "", branch: str = "main") -> list:
        """Recursively list all files in a repository path."""
        files = []
        try:
            response = self._request_with_retry('get',
                f"{self.base_url}/repos/{owner}/{repo}/contents/{path}?ref={branch}")
            if response.status_code != 200:
                return files
            
            contents = response.json()
            if not isinstance(contents, list):
                contents = [contents]
            
            for item in contents:
                if item['type'] == 'file':
                    files.append({'path': item['path'], 'sha': item['sha']})
                elif item['type'] == 'dir':
                    # Recursively get contents of subdirectory
                    files.extend(self.list_repo_contents(owner, repo, item['path'], branch))
        except Exception as e:
            self.last_error = f"Connection error listing contents: {str(e)}"
        
        return files
    
    def delete_file(self, owner: str, repo: str, path: str, sha: str, 
                    message: str, branch: str = "main") -> bool:
        """Delete a file from the repository."""
        try:
            data = {
                "message": message,
                "sha": sha,
                "branch": branch
            }
            response = self._request_with_retry('delete',
                f"{self.base_url}/repos/{owner}/{repo}/contents/{path}",
                json=data
            )
            return response.status_code == 200
        except Exception as e:
            self.last_error = f"Connection error deleting file: {str(e)}"
            return False
    
    def replace_repo_contents(self, owner: str, repo: str, files: dict, 
                              message: str, branch: str = "main",
                              folders_to_clean: list = None) -> dict:
        """Replace repository contents - delete old files in specified folders, then push new files."""
        results = {"deleted": [], "created": [], "failed": []}
        
        # Ensure the branch exists (handles empty/new repos)
        self._ensure_branch_exists(owner, repo, branch)
        
        # Default folders to clean (dbt project structure)
        if folders_to_clean is None:
            folders_to_clean = ["models", "analyses", "macros", "seeds", "snapshots", "tests"]
        
        # Also delete root-level dbt files
        root_files_to_delete = ["dbt_project.yml", "packages.yml", "profiles.yml"]
        
        # Step 1: Delete old files in the specified folders
        for folder in folders_to_clean:
            existing_files = self.list_repo_contents(owner, repo, folder, branch)
            for file_info in existing_files:
                if self.delete_file(owner, repo, file_info['path'], file_info['sha'], 
                                   f"Clean up: removing {file_info['path']}", branch):
                    results["deleted"].append(file_info['path'])
                time.sleep(0.3)  # Small delay to avoid rate limiting
        
        # Also delete root-level dbt config files if they exist
        for root_file in root_files_to_delete:
            existing = self.list_repo_contents(owner, repo, root_file, branch)
            for file_info in existing:
                if self.delete_file(owner, repo, file_info['path'], file_info['sha'],
                                   f"Clean up: removing {file_info['path']}", branch):
                    results["deleted"].append(file_info['path'])
                time.sleep(0.3)  # Small delay to avoid rate limiting
        
        # Step 2: Push new files with delay between each
        for path, content in files.items():
            if self.create_or_update_file(owner, repo, path, content, message, branch):
                results["created"].append(path)
            else:
                results["failed"].append({"path": path, "error": self.last_error})
            time.sleep(0.3)  # Small delay to avoid rate limiting
        
        return results


# =============================================================================
# DBT CLOUD API CLIENT
# =============================================================================

class DBTCloudAPI:
    """Client for dbt Cloud API."""
    
    def __init__(self, account_id: str, api_token: str, base_url: str = None):
        self.account_id = account_id
        self.api_token = api_token
        # Support different dbt Cloud regions
        self.base_url = base_url or "https://cloud.getdbt.com/api/v2"
        self.headers = {
            "Authorization": f"Token {api_token}",
            "Content-Type": "application/json",
        }
        self.last_error = None
    
    def test_connection(self):
        """Test the API connection."""
        try:
            # Try listing projects first (more reliable endpoint)
            response = requests.get(
                f"{self.base_url}/accounts/{self.account_id}/projects/",
                headers=self.headers,
                timeout=15
            )
            self.last_error = f"Status: {response.status_code}"
            if response.status_code != 200:
                self.last_error = f"Status {response.status_code}: {response.text[:200]}"
            return response.status_code == 200
        except requests.exceptions.Timeout:
            self.last_error = "Connection timed out"
            return False
        except requests.exceptions.ConnectionError as e:
            self.last_error = f"Connection error: {str(e)[:100]}"
            return False
        except Exception as e:
            self.last_error = f"Error: {str(e)[:100]}"
            return False
    
    def list_projects(self):
        """List all projects in the account."""
        response = requests.get(
            f"{self.base_url}/accounts/{self.account_id}/projects/",
            headers=self.headers
        )
        if response.status_code == 200:
            return response.json().get("data", [])
        return []
    
    def list_environments(self, project_id: int):
        """List environments for a project."""
        response = requests.get(
            f"{self.base_url}/accounts/{self.account_id}/environments/",
            headers=self.headers,
            params={"project_id": project_id}
        )
        if response.status_code == 200:
            return response.json().get("data", [])
        return []
    
    def list_jobs(self, project_id: int = None):
        """List jobs."""
        params = {}
        if project_id:
            params["project_id"] = project_id
        response = requests.get(
            f"{self.base_url}/accounts/{self.account_id}/jobs/",
            headers=self.headers,
            params=params
        )
        if response.status_code == 200:
            return response.json().get("data", [])
        return []
    
    def create_job(self, project_id: int, environment_id: int, name: str, execute_steps: list,
                   dbt_version: str = None):
        """Create a new job.
        
        Args:
            project_id: The dbt Cloud project ID
            environment_id: The dbt Cloud environment ID
            name: Job name
            execute_steps: List of dbt commands to run
            dbt_version: dbt version string. Valid options:
                - "latest": dbt Core latest version
                - "latest-fusion": Fusion engine (supports SAO)
                - "compatible": Compatible version
                - "extended": Extended support version
                - "fallback": Fallback version
        
        Note: SAO and Efficient Testing must be enabled manually in dbt Cloud UI after job creation.
        
        Returns:
            Tuple of (job_data, error_message) - job_data is dict if successful, None if failed
        """
        data = {
            "account_id": int(self.account_id),
            "project_id": project_id,
            "environment_id": environment_id,
            "name": name,
            "execute_steps": execute_steps,
            "triggers": {"github_webhook": False, "schedule": False, "git_provider_webhook": False},
            "settings": {"threads": 4, "target_name": "default"},
            "state": 1,  # Active
            "generate_docs": True,  # Generate dbt docs after run
            "run_generate_sources": True,  # Also generate source freshness
        }
        
        # Set dbt version
        # - For dbt Core: use specific version like "1.9.0-latest"
        # - For Fusion: use "versionless" 
        if dbt_version:
            data["dbt_version"] = dbt_version
        
        try:
            response = requests.post(
                f"{self.base_url}/accounts/{self.account_id}/jobs/",
                headers=self.headers,
                json=data,
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                self.last_error = None
                job_data = response.json().get("data", {})
                return (job_data, None)
            else:
                error_msg = f"Status {response.status_code}: {response.text[:500]}"
                self.last_error = error_msg
                return (None, error_msg)
        except requests.exceptions.Timeout:
            error_msg = "Request timed out after 30 seconds"
            self.last_error = error_msg
            return (None, error_msg)
        except Exception as e:
            error_msg = f"Request failed: {str(e)}"
            self.last_error = error_msg
            return (None, error_msg)
    
    def trigger_run(self, job_id: int, cause: str = "Triggered via SAO Estimator",
                   comparison_tag: str = None, schema_override: dict = None):
        """Trigger a job run with optional schema override for query tagging.
        
        Args:
            job_id: The dbt Cloud job ID to run
            cause: Description of why this run was triggered
            comparison_tag: Tag to identify this run for comparison (e.g., 'core_run_123' or 'fusion_run_123')
            schema_override: Optional dict to override schema vars (for passing custom tags to dbt)
            
        Returns:
            Run data dict with run_id and other metadata
        """
        data = {"cause": cause}
        
        # Add schema override if provided (allows passing vars to dbt for query tagging)
        if schema_override:
            data["schema_override"] = schema_override
            
        response = requests.post(
            f"{self.base_url}/accounts/{self.account_id}/jobs/{job_id}/run/",
            headers=self.headers,
            json=data
        )
        if response.status_code in [200, 201]:
            run_data = response.json().get("data", {})
            # Add our comparison tag for tracking
            if comparison_tag:
                run_data["_comparison_tag"] = comparison_tag
            return run_data
        return None
    
    def get_run(self, run_id: int):
        """Get run details."""
        response = requests.get(
            f"{self.base_url}/accounts/{self.account_id}/runs/{run_id}/",
            headers=self.headers
        )
        if response.status_code == 200:
            return response.json().get("data", {})
        return None
    
    def wait_for_run(self, run_id: int, timeout: int = 600, poll_interval: int = 10):
        """Wait for a run to complete."""
        start_time = time.time()
        while True:
            run = self.get_run(run_id)
            if not run:
                return None
            
            status = run.get("status")
            # 10 = Success, 20 = Error, 30 = Cancelled
            if status in [10, 20, 30]:
                return run
            
            if time.time() - start_time > timeout:
                return run
            
            time.sleep(poll_interval)
    
    def get_run_artifacts(self, run_id: int):
        """Get run artifacts including timing and model stats."""
        response = requests.get(
            f"{self.base_url}/accounts/{self.account_id}/runs/{run_id}/artifacts/",
            headers=self.headers
        )
        if response.status_code == 200:
            return response.json().get("data", {})
        return None
    
    def get_run_timing(self, run_id: int):
        """Get detailed timing information for a run."""
        run = self.get_run(run_id)
        if run:
            def parse_duration(val, default=0):
                """Parse duration from dbt Cloud API which may be seconds, HH:MM:SS, or ISO format."""
                if val is None:
                    return default
                # Already numeric
                if isinstance(val, (int, float)):
                    return float(val)
                val_str = str(val).strip()
                if not val_str:
                    return default
                # Try plain float first (e.g. "123.45")
                try:
                    return float(val_str)
                except ValueError:
                    pass
                # Try HH:MM:SS or HH:MM:SS.ffffff format
                for fmt in ["%H:%M:%S.%f", "%H:%M:%S"]:
                    try:
                        t = datetime.strptime(val_str, fmt)
                        return t.hour * 3600 + t.minute * 60 + t.second + t.microsecond / 1_000_000
                    except ValueError:
                        continue
                return default
            
            duration = parse_duration(run.get("run_duration"))
            
            # Fallback: compute from created_at / finished_at if duration is 0
            if duration == 0 and run.get("created_at") and run.get("finished_at"):
                try:
                    fmt_options = ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                                   "%Y-%m-%d %H:%M:%S.%f+00:00", "%Y-%m-%d %H:%M:%S+00:00"]
                    created = finished = None
                    for fmt in fmt_options:
                        try:
                            created = datetime.strptime(run["created_at"], fmt)
                            break
                        except ValueError:
                            continue
                    for fmt in fmt_options:
                        try:
                            finished = datetime.strptime(run["finished_at"], fmt)
                            break
                        except ValueError:
                            continue
                    if created and finished:
                        duration = max((finished - created).total_seconds(), 0)
                except Exception:
                    pass
            
            return {
                "run_id": run_id,
                "status": run.get("status"),
                "status_humanized": run.get("status_humanized"),
                "duration_seconds": round(duration, 2),
                "queued_duration": parse_duration(run.get("queued_duration"), 0),
                "run_duration": round(duration, 2),
                "created_at": run.get("created_at"),
                "finished_at": run.get("finished_at"),
                "job_id": run.get("job_id"),
                "is_complete": run.get("is_complete", False),
                "is_success": run.get("is_success", False),
                "is_error": run.get("is_error", False),
            }
        return None
    
    def get_run_results(self, run_id: int) -> dict:
        """Get detailed run results including model execution stats.
        
        Returns:
            Dictionary with model counts, execution times, and status breakdown
        """
        try:
            # Get run steps/results
            response = requests.get(
                f"{self.base_url}/accounts/{self.account_id}/runs/{run_id}/",
                headers=self.headers,
                params={"include_related": "run_steps"}
            )
            if response.status_code != 200:
                return {}
            
            run_data = response.json().get("data", {})
            run_steps = run_data.get("run_steps", [])
            
            # Parse dbt output to count models
            models_run = 0
            models_skipped = 0
            tests_run = 0
            total_models = 0
            
            for step in run_steps:
                step_logs = step.get("logs", "") or ""
                
                # Count models from dbt output
                # Look for patterns like "Completed successfully 50 of 50"
                import re
                completed_match = re.search(r"Completed successfully (\d+)", step_logs)
                if completed_match:
                    models_run = max(models_run, int(completed_match.group(1)))
                
                # Look for SAO skip messages
                skip_matches = re.findall(r"SKIP relation", step_logs, re.IGNORECASE)
                models_skipped += len(skip_matches)
                
                # Look for total model count
                total_match = re.search(r"Found (\d+) models", step_logs)
                if total_match:
                    total_models = int(total_match.group(1))
            
            return {
                "run_id": run_id,
                "models_run": models_run,
                "models_skipped": models_skipped,
                "tests_run": tests_run,
                "total_models": total_models,
                "duration_seconds": run_data.get("run_duration", 0),
                "status": run_data.get("status_humanized", "Unknown"),
            }
        except Exception as e:
            return {"error": str(e)}


# =============================================================================
# QUERY HISTORY ANALYZER - Analyzes Snowflake compute metrics
# =============================================================================

class QueryHistoryAnalyzer:
    """Analyze Snowflake query history to extract compute metrics for dbt jobs."""
    
    def __init__(self, snowflake_connection):
        self.conn = snowflake_connection
    
    def get_job_metrics(self, job_identifier: str, time_window_hours: int = 24) -> dict:
        """Get aggregated metrics for queries matching a job identifier.
        
        Args:
            job_identifier: String to match in query tags (e.g., 'core_baseline' or 'fusion_sao')
            time_window_hours: How far back to look for queries
            
        Returns:
            Dictionary with aggregated metrics
        """
        query = f"""
        SELECT 
            COUNT(*) as query_count,
            SUM(total_elapsed_time) / 1000 as total_seconds,
            SUM(bytes_scanned) as total_bytes_scanned,
            SUM(bytes_written_to_result) as total_bytes_written,
            SUM(compilation_time) / 1000 as total_compile_seconds,
            SUM(execution_time) / 1000 as total_exec_seconds,
            AVG(total_elapsed_time) / 1000 as avg_query_seconds,
            MAX(total_elapsed_time) / 1000 as max_query_seconds,
            SUM(credits_used_cloud_services) as credits_used,
            SUM(rows_produced) as total_rows_produced
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE query_tag LIKE '%{job_identifier}%'
          AND start_time >= DATEADD(hour, -{time_window_hours}, CURRENT_TIMESTAMP())
          AND query_type IN ('CREATE_TABLE_AS_SELECT', 'INSERT', 'MERGE', 'SELECT', 'CREATE_VIEW')
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            
            if row:
                return {
                    "query_count": row[0] or 0,
                    "total_seconds": round(row[1] or 0, 2),
                    "total_bytes_scanned": row[2] or 0,
                    "total_bytes_written": row[3] or 0,
                    "compile_seconds": round(row[4] or 0, 2),
                    "execution_seconds": round(row[5] or 0, 2),
                    "avg_query_seconds": round(row[6] or 0, 2),
                    "max_query_seconds": round(row[7] or 0, 2),
                    "credits_used": round(row[8] or 0, 4),
                    "total_rows_produced": row[9] or 0,
                }
        except Exception as e:
            print(f"Error querying history: {e}")
        
        return {
            "query_count": 0, "total_seconds": 0, "total_bytes_scanned": 0,
            "credits_used": 0, "error": str(e) if 'e' in locals() else "No data"
        }
    
    def get_metrics_by_dbt_run_id(self, dbt_run_id: int, time_window_hours: int = 4) -> dict:
        """Get Snowflake metrics for a specific dbt Cloud run using the run_id in query tags.
        
        dbt Cloud automatically adds run metadata to Snowflake query tags.
        
        Args:
            dbt_run_id: The dbt Cloud run ID
            time_window_hours: How far back to look for queries
            
        Returns:
            Dictionary with aggregated compute metrics
        """
        # dbt tags queries with JSON containing run_id
        query = f"""
        SELECT 
            COUNT(*) as query_count,
            SUM(total_elapsed_time) / 1000 as total_seconds,
            SUM(bytes_scanned) as total_bytes_scanned,
            SUM(bytes_written_to_result) as total_bytes_written,
            SUM(compilation_time) / 1000 as total_compile_seconds,
            SUM(execution_time) / 1000 as total_exec_seconds,
            AVG(total_elapsed_time) / 1000 as avg_query_seconds,
            MAX(total_elapsed_time) / 1000 as max_query_seconds,
            COALESCE(SUM(credits_used_cloud_services), 0) as credits_used,
            SUM(rows_produced) as total_rows_produced,
            COUNT(DISTINCT query_id) as distinct_queries,
            MIN(start_time) as first_query_time,
            MAX(end_time) as last_query_time
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE (
            query_tag LIKE '%"run_id": {dbt_run_id}%' 
            OR query_tag LIKE '%"run_id":{dbt_run_id}%'
            OR query_tag LIKE '%run_id={dbt_run_id}%'
        )
          AND start_time >= DATEADD(hour, -{time_window_hours}, CURRENT_TIMESTAMP())
          AND query_type IN ('CREATE_TABLE_AS_SELECT', 'INSERT', 'MERGE', 'SELECT', 'CREATE_VIEW', 'CREATE TABLE')
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            
            if row and row[0] > 0:
                return {
                    "dbt_run_id": dbt_run_id,
                    "query_count": row[0] or 0,
                    "total_seconds": round(row[1] or 0, 2),
                    "total_bytes_scanned": row[2] or 0,
                    "total_gb_scanned": round((row[2] or 0) / (1024**3), 3),
                    "total_bytes_written": row[3] or 0,
                    "compile_seconds": round(row[4] or 0, 2),
                    "execution_seconds": round(row[5] or 0, 2),
                    "avg_query_seconds": round(row[6] or 0, 3),
                    "max_query_seconds": round(row[7] or 0, 2),
                    "credits_used": round(row[8] or 0, 6),
                    "total_rows_produced": row[9] or 0,
                    "distinct_queries": row[10] or 0,
                    "first_query_time": str(row[11]) if row[11] else None,
                    "last_query_time": str(row[12]) if row[12] else None,
                }
            return {"dbt_run_id": dbt_run_id, "query_count": 0, "note": "No queries found for this run_id"}
        except Exception as e:
            return {"dbt_run_id": dbt_run_id, "error": str(e)}
    
    def get_warehouse_usage_for_period(self, warehouse_name: str, hours_back: int = 24) -> dict:
        """Get warehouse usage metrics for a time period.
        
        Args:
            warehouse_name: Name of the Snowflake warehouse
            hours_back: Number of hours to look back
            
        Returns:
            Dictionary with warehouse usage metrics
        """
        query = f"""
        SELECT 
            SUM(credits_used) as total_credits,
            AVG(credits_used) as avg_credits_per_hour,
            COUNT(*) as hours_active
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE warehouse_name = '{warehouse_name.upper()}'
          AND start_time >= DATEADD(hour, -{hours_back}, CURRENT_TIMESTAMP())
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            
            if row:
                return {
                    "warehouse": warehouse_name,
                    "total_credits": round(row[0] or 0, 4),
                    "avg_credits_per_hour": round(row[1] or 0, 4),
                    "hours_active": row[2] or 0,
                }
        except Exception as e:
            return {"error": str(e)}
        
        return {"warehouse": warehouse_name, "total_credits": 0}
    
    def get_run_comparison(self, core_run_id: int, fusion_run_id: int, 
                          time_window_hours: int = 2) -> dict:
        """Compare metrics between a Core run and a Fusion run.
        
        Uses get_metrics_by_dbt_run_id which matches multiple query tag formats.
        """
        core_metrics = self.get_metrics_by_dbt_run_id(core_run_id, time_window_hours)
        fusion_metrics = self.get_metrics_by_dbt_run_id(fusion_run_id, time_window_hours)
        
        # Calculate savings
        core_time = core_metrics.get("total_seconds", 0)
        fusion_time = fusion_metrics.get("total_seconds", 0)
        time_saved_pct = ((core_time - fusion_time) / core_time * 100) if core_time > 0 else 0
        
        core_queries = core_metrics.get("query_count", 0)
        fusion_queries = fusion_metrics.get("query_count", 0)
        queries_saved_pct = ((core_queries - fusion_queries) / core_queries * 100) if core_queries > 0 else 0
        
        core_bytes = core_metrics.get("total_bytes_scanned", 0)
        fusion_bytes = fusion_metrics.get("total_bytes_scanned", 0)
        bytes_saved_pct = ((core_bytes - fusion_bytes) / core_bytes * 100) if core_bytes > 0 else 0
        
        return {
            "core": core_metrics,
            "fusion": fusion_metrics,
            "savings": {
                "time_saved_seconds": round(core_time - fusion_time, 2),
                "time_saved_pct": round(time_saved_pct, 1),
                "queries_saved": core_queries - fusion_queries,
                "queries_saved_pct": round(queries_saved_pct, 1),
                "bytes_saved": core_bytes - fusion_bytes,
                "bytes_saved_pct": round(bytes_saved_pct, 1),
            }
        }


# =============================================================================
# SOURCE DATA INJECTOR - Injects new data into source tables
# =============================================================================

class SourceDataInjector:
    """Inject new rows into source tables to simulate data changes."""
    
    def __init__(self, snowflake_connection, database: str, schema_name: str):
        self.conn = snowflake_connection
        self.database = database.upper()
        self.schema = schema_name.upper()
    
    def get_source_tables(self) -> list:
        """Get list of all source tables in the schema."""
        query = f"""
        SELECT table_name 
        FROM {self.database}.INFORMATION_SCHEMA.TABLES 
        WHERE table_schema = '{self.schema}'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return tables
        except Exception as e:
            print(f"Error getting tables: {e}")
            return []
    
    def get_table_columns(self, table_name: str) -> list:
        """Get column information for a table."""
        query = f"""
        SELECT column_name, data_type, is_nullable
        FROM {self.database}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{self.schema}'
          AND table_name = '{table_name.upper()}'
        ORDER BY ordinal_position
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            columns = [(row[0], row[1], row[2]) for row in cursor.fetchall()]
            cursor.close()
            return columns
        except Exception as e:
            print(f"Error getting columns: {e}")
            return []
    
    def inject_rows(self, table_name: str, num_rows: int, 
                    generated_schema: dict = None) -> dict:
        """Inject new rows into a source table.
        
        Args:
            table_name: Name of the table to insert into
            num_rows: Number of rows to insert
            generated_schema: Original schema with sample values (optional)
            
        Returns:
            Dictionary with success status and row count
        """
        columns = self.get_table_columns(table_name)
        if not columns:
            return {"success": False, "error": f"Could not get columns for {table_name}"}
        
        # Build INSERT statement with generated values
        col_names = [col[0] for col in columns]
        values_list = []
        
        for i in range(num_rows):
            row_values = []
            for col_name, data_type, nullable in columns:
                value = self._generate_value(col_name, data_type, i, generated_schema, table_name)
                row_values.append(value)
            values_list.append(f"({', '.join(row_values)})")
        
        # Insert in batches of 1000
        batch_size = 1000
        total_inserted = 0
        
        try:
            cursor = self.conn.cursor()
            
            for batch_start in range(0, len(values_list), batch_size):
                batch = values_list[batch_start:batch_start + batch_size]
                insert_sql = f"""
                INSERT INTO {self.database}.{self.schema}.{table_name.upper()} 
                ({', '.join(col_names)})
                VALUES {', '.join(batch)}
                """
                cursor.execute(insert_sql)
                total_inserted += len(batch)
            
            cursor.close()
            return {"success": True, "rows_inserted": total_inserted, "table": table_name}
        except Exception as e:
            return {"success": False, "error": str(e), "table": table_name}
    
    def _generate_value(self, col_name: str, data_type: str, row_index: int,
                       schema: dict, table_name: str) -> str:
        """Generate a value for a column based on its type."""
        col_name_upper = col_name.upper()
        data_type_upper = data_type.upper()
        
        # Generate unique IDs for primary/foreign keys (always VARCHAR in our schema)
        if col_name_upper.endswith('_ID') or col_name_upper == 'ID':
            unique_id = int(time.time() * 1000) + row_index
            return f"'{unique_id}'"
        
        # Handle different data types
        if 'INT' in data_type_upper or 'NUMBER' in data_type_upper:
            return str(random.randint(1, 100000))
        
        elif 'DECIMAL' in data_type_upper or 'FLOAT' in data_type_upper or 'DOUBLE' in data_type_upper:
            return str(round(random.uniform(1.0, 10000.0), 2))
        
        elif 'BOOL' in data_type_upper:
            return random.choice(["TRUE", "FALSE"])
        
        elif 'TIMESTAMP' in data_type_upper:
            # Generate timestamp within last 7 days
            days_ago = random.randint(0, 7)
            hours_ago = random.randint(0, 23)
            ts = datetime.now() - timedelta(days=days_ago, hours=hours_ago)
            return f"'{ts.strftime('%Y-%m-%d %H:%M:%S')}'"
        
        elif 'DATE' in data_type_upper:
            days_ago = random.randint(0, 30)
            dt = datetime.now() - timedelta(days=days_ago)
            return f"'{dt.strftime('%Y-%m-%d')}'"
        
        elif 'TIME' in data_type_upper:
            return f"'{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}'"
        
        else:
            # VARCHAR/TEXT - try to get sample values from schema
            sample_values = self._get_sample_values(schema, table_name, col_name)
            if sample_values:
                val = str(random.choice(sample_values))
                val = self._sanitize_sql_string(val)
                return f"'{val}'"
            else:
                return f"'injected_{col_name.lower()}_{row_index}'"
    
    @staticmethod
    def _sanitize_sql_string(val: str) -> str:
        """Strip non-ASCII characters and escape single quotes for safe SQL insertion."""
        val = val.encode('ascii', 'ignore').decode('ascii').strip()
        val = val.replace("'", "''")
        return val
    
    def _get_sample_values(self, schema: dict, table_name: str, col_name: str) -> list:
        """Get sample values from schema for a column."""
        if not schema or 'sources' not in schema:
            return []
        
        for source in schema['sources']:
            if source['name'].upper() == table_name.upper():
                for col in source.get('columns', []):
                    if col['name'].upper() == col_name.upper():
                        raw_values = col.get('sample_values', [])
                        if raw_values:
                            cleaned = []
                            for v in raw_values:
                                s = str(v).encode('ascii', 'ignore').decode('ascii').strip()
                                if s:
                                    cleaned.append(s)
                            return cleaned if cleaned else []
                        return []
        return []
    
    def inject_into_random_sources(self, num_sources_to_change: int, 
                                   rows_per_source: int,
                                   generated_schema: dict = None) -> dict:
        """Inject data into a random subset of source tables.
        
        Args:
            num_sources_to_change: Number of source tables to modify
            rows_per_source: Number of rows to inject per table
            generated_schema: Original schema for realistic values
            
        Returns:
            Summary of injection results
        """
        all_tables = self.get_source_tables()
        if not all_tables:
            return {"success": False, "error": "No source tables found"}
        
        # Select random tables
        num_to_change = min(num_sources_to_change, len(all_tables))
        tables_to_change = random.sample(all_tables, num_to_change)
        
        results = {
            "tables_changed": [],
            "tables_unchanged": [t for t in all_tables if t not in tables_to_change],
            "total_rows_injected": 0,
            "errors": []
        }
        
        for table in tables_to_change:
            result = self.inject_rows(table, rows_per_source, generated_schema)
            if result.get("success"):
                results["tables_changed"].append({
                    "table": table,
                    "rows": result.get("rows_inserted", 0)
                })
                results["total_rows_injected"] += result.get("rows_inserted", 0)
            else:
                results["errors"].append({
                    "table": table,
                    "error": result.get("error")
                })
        
        results["success"] = len(results["errors"]) == 0
        return results


# =============================================================================
# SAO COMPARISON ENGINE - Orchestrates Core vs Fusion comparisons
# =============================================================================

class SAOComparisonEngine:
    """Orchestrate live comparisons between dbt Core and dbt Fusion jobs."""
    
    def __init__(self, dbt_api: DBTCloudAPI, snowflake_loader: 'SnowflakeDataLoader',
                 database: str, schema_name: str):
        self.dbt_api = dbt_api
        self.sf_loader = snowflake_loader
        self.database = database
        self.schema = schema_name
        self.data_injector = None
        self.query_analyzer = None
        
        # Initialize helpers if we have a connection
        if snowflake_loader and snowflake_loader.conn:
            self.data_injector = SourceDataInjector(
                snowflake_loader.conn, database, schema_name
            )
            self.query_analyzer = QueryHistoryAnalyzer(snowflake_loader.conn)
    
    def run_single_comparison(self, core_job_id: int, fusion_job_id: int,
                             rows_to_inject: int, sources_to_change: int,
                             generated_schema: dict = None,
                             timeout: int = 1800) -> dict:
        """Run a single comparison cycle: inject data, run both jobs, collect metrics.
        
        Args:
            core_job_id: dbt Cloud job ID for Core baseline
            fusion_job_id: dbt Cloud job ID for Fusion with SAO
            rows_to_inject: Number of rows to inject per source
            sources_to_change: Number of source tables to modify
            generated_schema: Schema for generating realistic values
            timeout: Max time to wait for jobs (seconds)
            
        Returns:
            Comparison results dictionary
        """
        result = {
            "timestamp": datetime.now().isoformat(),
            "injection": None,
            "core_run": None,
            "fusion_run": None,
            "metrics": None,
            "error": None
        }
        
        try:
            # Step 1: Inject new data into source tables
            if self.data_injector and sources_to_change > 0 and rows_to_inject > 0:
                injection_result = self.data_injector.inject_into_random_sources(
                    num_sources_to_change=sources_to_change,
                    rows_per_source=rows_to_inject,
                    generated_schema=generated_schema
                )
                result["injection"] = injection_result
            else:
                result["injection"] = {"tables_changed": [], "total_rows_injected": 0}
            
            # Step 2: Trigger Core job and wait
            core_run = self.dbt_api.trigger_run(
                core_job_id, 
                cause=f"SAO Comparison - Core baseline ({datetime.now().strftime('%H:%M')})"
            )
            if not core_run:
                result["error"] = "Failed to trigger Core job"
                return result
            
            core_run_id = core_run.get("id")
            result["core_run"] = {"run_id": core_run_id, "status": "running"}
            
            # Wait for Core job
            core_complete = self.dbt_api.wait_for_run(core_run_id, timeout=timeout, poll_interval=15)
            if core_complete:
                core_timing = self.dbt_api.get_run_timing(core_run_id)
                result["core_run"] = core_timing
                print(f"[SAO] Core run {core_run_id} timing: duration={core_timing.get('duration_seconds') if core_timing else 'None'}s, "
                      f"status={core_timing.get('status_humanized') if core_timing else 'None'}")
            else:
                print(f"[SAO] Core run {core_run_id} did not complete within timeout")
            
            # Step 3: Trigger Fusion job and wait
            fusion_run = self.dbt_api.trigger_run(
                fusion_job_id,
                cause=f"SAO Comparison - Fusion SAO ({datetime.now().strftime('%H:%M')})"
            )
            if not fusion_run:
                result["error"] = "Failed to trigger Fusion job"
                return result
            
            fusion_run_id = fusion_run.get("id")
            result["fusion_run"] = {"run_id": fusion_run_id, "status": "running"}
            
            # Wait for Fusion job
            fusion_complete = self.dbt_api.wait_for_run(fusion_run_id, timeout=timeout, poll_interval=15)
            if fusion_complete:
                fusion_timing = self.dbt_api.get_run_timing(fusion_run_id)
                result["fusion_run"] = fusion_timing
                print(f"[SAO] Fusion run {fusion_run_id} timing: duration={fusion_timing.get('duration_seconds') if fusion_timing else 'None'}s, "
                      f"status={fusion_timing.get('status_humanized') if fusion_timing else 'None'}")
            else:
                print(f"[SAO] Fusion run {fusion_run_id} did not complete within timeout")
            
            # Step 4: Calculate comparison metrics
            def safe_val(data, key, default=0):
                if not data:
                    return default
                val = data.get(key, default)
                if val is None:
                    return default
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return default
            
            # dbt Cloud durations (wall-clock time including compilation, artifacts, etc.)
            core_duration = safe_val(result["core_run"], "duration_seconds")
            fusion_duration = safe_val(result["fusion_run"], "duration_seconds")
            
            print(f"[SAO] dbt Cloud durations: core={core_duration}s, fusion={fusion_duration}s")
            
            # Get injection info safely
            injection = result.get("injection") or {}
            tables_changed = injection.get("tables_changed") or []
            
            # Try to get Snowflake query metrics (actual compute time + credits)
            sf_core_exec = 0
            sf_fusion_exec = 0
            sf_available = False
            
            if self.query_analyzer:
                try:
                    sf_core = self.query_analyzer.get_metrics_by_dbt_run_id(core_run_id, time_window_hours=4)
                    sf_fusion = self.query_analyzer.get_metrics_by_dbt_run_id(fusion_run_id, time_window_hours=4)
                    
                    result["snowflake_metrics"] = {
                        "core": sf_core,
                        "fusion": sf_fusion,
                    }
                    
                    core_has_data = sf_core.get("query_count", 0) > 0
                    fusion_has_data = sf_fusion.get("query_count", 0) > 0
                    
                    if core_has_data and fusion_has_data:
                        sf_core_exec = sf_core.get("execution_seconds", 0) or sf_core.get("total_seconds", 0)
                        sf_fusion_exec = sf_fusion.get("execution_seconds", 0) or sf_fusion.get("total_seconds", 0)
                        sf_available = True
                        print(f"[SAO] Snowflake execution: core={sf_core_exec}s ({sf_core.get('query_count', 0)} queries), "
                              f"fusion={sf_fusion_exec}s ({sf_fusion.get('query_count', 0)} queries)")
                    else:
                        print(f"[SAO] Snowflake metrics not yet available (ACCOUNT_USAGE lag). "
                              f"Core queries: {sf_core.get('query_count', 0)}, Fusion queries: {sf_fusion.get('query_count', 0)}")
                except Exception as e:
                    result["snowflake_metrics"] = {"error": str(e)}
                    print(f"[SAO] Snowflake metrics error: {e}")
            
            time_saved = core_duration - fusion_duration
            time_saved_pct = (time_saved / core_duration * 100) if core_duration > 0 else 0
            
            result["metrics"] = {
                "core_duration_seconds": core_duration,
                "fusion_duration_seconds": fusion_duration,
                "time_saved_seconds": time_saved,
                "time_saved_percentage": round(time_saved_pct, 1),
                "sources_changed": len(tables_changed),
                "rows_injected": injection.get("total_rows_injected", 0),
                "sf_core_execution_seconds": sf_core_exec,
                "sf_fusion_execution_seconds": sf_fusion_exec,
                "sf_metrics_available": sf_available,
                "metrics_source": "snowflake" if sf_available else "dbt_cloud_estimate",
            }
            
        except Exception as e:
            result["error"] = str(e)
        
        return result
    
    def run_comparison_series(self, core_job_id: int, fusion_job_id: int,
                             num_runs: int, rows_per_run: int,
                             sources_per_run: int, generated_schema: dict = None,
                             progress_callback=None) -> dict:
        """Run multiple comparison cycles to build statistical confidence.
        
        Args:
            core_job_id: dbt Cloud job ID for Core baseline
            fusion_job_id: dbt Cloud job ID for Fusion with SAO
            num_runs: Number of comparison cycles to run
            rows_per_run: Rows to inject per source per run
            sources_per_run: Sources to modify per run
            generated_schema: Schema for realistic values
            progress_callback: Function to call with progress updates
            
        Returns:
            Aggregated comparison results
        """
        all_results = []
        
        for run_num in range(num_runs):
            if progress_callback:
                progress_callback(run_num + 1, num_runs, "running")
            
            run_result = self.run_single_comparison(
                core_job_id=core_job_id,
                fusion_job_id=fusion_job_id,
                rows_to_inject=rows_per_run,
                sources_to_change=sources_per_run,
                generated_schema=generated_schema
            )
            run_result["run_number"] = run_num + 1
            all_results.append(run_result)
            
            if progress_callback:
                progress_callback(run_num + 1, num_runs, "complete")
        
        return {
            "aggregate": _calculate_comparison_aggregate(all_results),
            "runs": all_results
        }
    
    def simulate_roi_projection(self, comparison_results: dict, 
                               runs_per_day: int, 
                               warehouse_size: str) -> dict:
        """Project ROI based on observed comparison results as percent time savings.
        
        Uses Snowflake execution time when available, falls back to dbt Cloud duration.
        """
        agg = comparison_results.get("aggregate", {})
        
        # Prefer Snowflake execution time over dbt Cloud wall-clock duration
        has_sf = agg.get("sf_avg_core_execution", 0) > 0
        if has_sf:
            avg_core_seconds = agg.get("sf_avg_core_execution", 0)
            avg_fusion_seconds = agg.get("sf_avg_fusion_execution", 0)
            metrics_source = "snowflake"
        else:
            avg_core_seconds = agg.get("avg_core_duration", 0)
            avg_fusion_seconds = agg.get("avg_fusion_duration", 0)
            metrics_source = "dbt_cloud_estimate"
        
        time_saved_seconds = avg_core_seconds - avg_fusion_seconds
        time_saved_pct = (time_saved_seconds / avg_core_seconds * 100) if avg_core_seconds > 0 else 0
        
        daily_time_saved = time_saved_seconds * runs_per_day
        monthly_time_saved = daily_time_saved * 30
        
        return {
            "per_run": {
                "core_duration_seconds": round(avg_core_seconds, 1),
                "fusion_duration_seconds": round(avg_fusion_seconds, 1),
                "time_saved_seconds": round(time_saved_seconds, 1),
                "time_saved_pct": round(time_saved_pct, 1),
            },
            "daily": {
                "runs": runs_per_day,
                "time_saved_seconds": round(daily_time_saved, 0),
                "time_saved_minutes": round(daily_time_saved / 60, 1),
            },
            "monthly": {
                "time_saved_hours": round(monthly_time_saved / 3600, 1),
            },
            "assumptions": {
                "warehouse_size": warehouse_size,
                "runs_per_day": runs_per_day,
            },
            "metrics_source": metrics_source,
            "sf_runs_with_data": agg.get("sf_runs_with_data", 0),
        }
    
    def refresh_snowflake_metrics(self, comparison_results: dict) -> dict:
        """Re-query ACCOUNT_USAGE.QUERY_HISTORY to update metrics for all runs.
        
        ACCOUNT_USAGE can lag up to 45 minutes. Call this after the initial comparison
        to backfill Snowflake execution times and recalculate credits from real data.
        
        Args:
            comparison_results: Existing comparison results dict with 'runs' list
            
        Returns:
            Updated comparison_results with refreshed Snowflake metrics
        """
        if not self.query_analyzer:
            return comparison_results
        
        runs = comparison_results.get("runs", [])
        updated_count = 0
        
        for run in runs:
            if run.get("error"):
                continue
            
            core_run = run.get("core_run") or {}
            fusion_run = run.get("fusion_run") or {}
            core_run_id = core_run.get("run_id")
            fusion_run_id = fusion_run.get("run_id")
            
            if not core_run_id or not fusion_run_id:
                continue
            
            try:
                sf_core = self.query_analyzer.get_metrics_by_dbt_run_id(core_run_id, time_window_hours=24)
                sf_fusion = self.query_analyzer.get_metrics_by_dbt_run_id(fusion_run_id, time_window_hours=24)
                
                run["snowflake_metrics"] = {"core": sf_core, "fusion": sf_fusion}
                
                core_has_data = sf_core.get("query_count", 0) > 0
                fusion_has_data = sf_fusion.get("query_count", 0) > 0
                
                if core_has_data and fusion_has_data and run.get("metrics"):
                    sf_core_exec = sf_core.get("execution_seconds", 0) or sf_core.get("total_seconds", 0)
                    sf_fusion_exec = sf_fusion.get("execution_seconds", 0) or sf_fusion.get("total_seconds", 0)
                    run["metrics"]["sf_core_execution_seconds"] = sf_core_exec
                    run["metrics"]["sf_fusion_execution_seconds"] = sf_fusion_exec
                    run["metrics"]["sf_metrics_available"] = True
                    run["metrics"]["metrics_source"] = "snowflake"
                    updated_count += 1
            except Exception as e:
                print(f"[SAO] Error refreshing metrics for run {core_run_id}/{fusion_run_id}: {e}")
        
        # Recalculate aggregate with updated Snowflake data
        comparison_results["aggregate"] = _calculate_comparison_aggregate(runs)
        comparison_results["sf_refresh_time"] = datetime.now().isoformat()
        comparison_results["sf_refresh_updated"] = updated_count
        
        return comparison_results


# =============================================================================
# SAO SIMULATION ENGINE - Projects ROI without running actual jobs
# =============================================================================

class SAOSimulationEngine:
    """Simulate SAO savings based on model metadata without running actual dbt jobs.
    
    This engine calculates theoretical savings by analyzing:
    1. DAG structure and dependencies
    2. Source change patterns
    3. Expected model skip rates with SAO
    """
    
    # Typical execution time estimates by materialization type (seconds)
    MATERIALIZATION_TIME_ESTIMATES = {
        "view": 2,           # Views are fast (just DDL)
        "table": 15,         # Tables require full data movement
        "incremental": 8,    # Incremental is between view and table
    }
    
    # Warehouse credits per hour by size
    WAREHOUSE_CREDITS = {
        "X-Small": 1, "Small": 2, "Medium": 4,
        "Large": 8, "X-Large": 16, "2X-Large": 32,
        "3X-Large": 64, "4X-Large": 128
    }
    
    def __init__(self, schema: dict = None, pipeline_config: dict = None):
        """Initialize with schema and pipeline configuration.
        
        Args:
            schema: Generated schema with source tables
            pipeline_config: Pipeline configuration with model counts and materializations
        """
        self.schema = schema or {}
        self.pipeline_config = pipeline_config or {}
        self.dag_analysis = None
    
    def analyze_dag(self, dbt_models: dict = None) -> dict:
        """Analyze the DAG structure to understand dependencies.
        
        Args:
            dbt_models: Dictionary of model files from DBTProjectGenerator
            
        Returns:
            DAG analysis with layer counts and dependencies
        """
        num_sources = len(self.schema.get('sources', []))
        num_models = self.pipeline_config.get('num_models', 100)
        materializations = self.pipeline_config.get('materialization', {'incremental': 30, 'table': 40, 'view': 30})
        dag_depth = self.pipeline_config.get('dag_depth', 4)
        
        # Calculate layer distribution based on typical DAG structure
        staging_count = num_sources  # 1:1 with sources
        remaining = max(0, num_models - staging_count)
        
        # Layer distribution
        int_layer1 = int(remaining * 0.25)
        int_layer2 = int(remaining * 0.20)
        int_layer3 = int(remaining * 0.15)
        marts_count = int(remaining * 0.25)
        reports_count = remaining - int_layer1 - int_layer2 - int_layer3 - marts_count
        
        # Calculate materializations per layer
        incremental_count = int(num_models * materializations.get('incremental', 30) / 100)
        table_count = int(num_models * materializations.get('table', 40) / 100)
        view_count = num_models - incremental_count - table_count
        
        self.dag_analysis = {
            "total_sources": num_sources,
            "total_models": num_models,
            "dag_depth": dag_depth,
            "layers": {
                "staging": {"count": staging_count, "materialization": "view"},
                "intermediate_1": {"count": int_layer1, "depends_on": "staging"},
                "intermediate_2": {"count": int_layer2, "depends_on": "intermediate_1"},
                "intermediate_3": {"count": int_layer3, "depends_on": "intermediate_2"},
                "marts": {"count": marts_count, "depends_on": "intermediate"},
                "reports": {"count": reports_count, "depends_on": "marts"},
            },
            "materializations": {
                "incremental": incremental_count,
                "table": table_count,
                "view": view_count,
            },
            "dependency_chain_length": dag_depth + 2,  # sources -> staging -> ... -> reports
        }
        
        return self.dag_analysis
    
    def simulate_single_run(self, sources_changed_pct: float) -> dict:
        """Simulate a single run comparing Core vs Fusion.
        
        Args:
            sources_changed_pct: Percentage of sources with new data (0-100)
            
        Returns:
            Simulated run results
        """
        if not self.dag_analysis:
            self.analyze_dag()
        
        dag = self.dag_analysis
        total_models = dag["total_models"]
        total_sources = dag["total_sources"]
        
        # How many sources have new data
        sources_changed = max(1, int(total_sources * sources_changed_pct / 100))
        sources_unchanged = total_sources - sources_changed
        
        # ======================================================================
        # CORE BEHAVIOR: Runs ALL models regardless of source changes
        # ======================================================================
        core_models_run = total_models
        
        # ======================================================================
        # FUSION SAO BEHAVIOR: Only runs models affected by changed sources
        # ======================================================================
        # Calculate affected models through the DAG
        # Staging: 1:1 with changed sources
        affected_staging = sources_changed
        
        # Each subsequent layer: models that depend on affected upstream
        # Approximation: affected models cascade through layers proportionally
        staging_affected_pct = sources_changed / total_sources if total_sources > 0 else 0
        
        # Layer cascade with some amplification (dependencies spread)
        layers = dag["layers"]
        
        int1_affected = int(layers["intermediate_1"]["count"] * staging_affected_pct * 1.2)
        int2_affected = int(layers["intermediate_2"]["count"] * staging_affected_pct * 1.1)
        int3_affected = int(layers["intermediate_3"]["count"] * staging_affected_pct * 1.05)
        marts_affected = int(layers["marts"]["count"] * staging_affected_pct * 1.0)
        reports_affected = int(layers["reports"]["count"] * staging_affected_pct * 1.0)
        
        # Cap at layer size
        int1_affected = min(int1_affected, layers["intermediate_1"]["count"])
        int2_affected = min(int2_affected, layers["intermediate_2"]["count"])
        int3_affected = min(int3_affected, layers["intermediate_3"]["count"])
        marts_affected = min(marts_affected, layers["marts"]["count"])
        reports_affected = min(reports_affected, layers["reports"]["count"])
        
        fusion_models_run = (affected_staging + int1_affected + int2_affected + 
                           int3_affected + marts_affected + reports_affected)
        fusion_models_skipped = total_models - fusion_models_run
        
        # ======================================================================
        # ESTIMATE EXECUTION TIME
        # ======================================================================
        mats = dag["materializations"]
        avg_time_per_model = (
            (mats["view"] * self.MATERIALIZATION_TIME_ESTIMATES["view"] +
             mats["table"] * self.MATERIALIZATION_TIME_ESTIMATES["table"] +
             mats["incremental"] * self.MATERIALIZATION_TIME_ESTIMATES["incremental"])
            / total_models if total_models > 0 else 5
        )
        
        core_duration = core_models_run * avg_time_per_model
        fusion_duration = fusion_models_run * avg_time_per_model
        
        time_saved = core_duration - fusion_duration
        time_saved_pct = (time_saved / core_duration * 100) if core_duration > 0 else 0
        
        return {
            "sources_changed": sources_changed,
            "sources_unchanged": sources_unchanged,
            "sources_changed_pct": round(sources_changed_pct, 1),
            "core": {
                "models_run": core_models_run,
                "models_skipped": 0,
                "estimated_duration_seconds": round(core_duration, 1),
            },
            "fusion": {
                "models_run": fusion_models_run,
                "models_skipped": fusion_models_skipped,
                "estimated_duration_seconds": round(fusion_duration, 1),
            },
            "savings": {
                "models_skipped": fusion_models_skipped,
                "models_skipped_pct": round((fusion_models_skipped / total_models * 100) if total_models > 0 else 0, 1),
                "time_saved_seconds": round(time_saved, 1),
                "time_saved_pct": round(time_saved_pct, 1),
            }
        }
    
    def simulate_daily_runs(self, runs_per_day: int, source_change_rate: float) -> dict:
        """Simulate a full day of runs to project daily savings.
        
        Args:
            runs_per_day: Number of job runs per day
            source_change_rate: Average percentage of sources with new data per run
            
        Returns:
            Daily simulation results with aggregated savings
        """
        if not self.dag_analysis:
            self.analyze_dag()
        
        daily_results = []
        
        for run_num in range(runs_per_day):
            # Vary source change rate slightly for realism
            actual_change_rate = max(0, min(100, source_change_rate + random.uniform(-10, 10)))
            run_result = self.simulate_single_run(actual_change_rate)
            run_result["run_number"] = run_num + 1
            daily_results.append(run_result)
        
        # Aggregate daily results
        total_core_models = sum(r["core"]["models_run"] for r in daily_results)
        total_fusion_models = sum(r["fusion"]["models_run"] for r in daily_results)
        total_core_time = sum(r["core"]["estimated_duration_seconds"] for r in daily_results)
        total_fusion_time = sum(r["fusion"]["estimated_duration_seconds"] for r in daily_results)
        
        return {
            "runs_per_day": runs_per_day,
            "source_change_rate": source_change_rate,
            "runs": daily_results,
            "daily_totals": {
                "core_models_run": total_core_models,
                "fusion_models_run": total_fusion_models,
                "models_saved": total_core_models - total_fusion_models,
                "core_duration_seconds": round(total_core_time, 1),
                "fusion_duration_seconds": round(total_fusion_time, 1),
                "time_saved_seconds": round(total_core_time - total_fusion_time, 1),
                "time_saved_pct": round(((total_core_time - total_fusion_time) / total_core_time * 100) if total_core_time > 0 else 0, 1),
            }
        }
    
    def project_roi(self, runs_per_day: int, source_change_rate: float,
                   warehouse_size: str, credit_cost: float = 3.0) -> dict:
        """Project ROI over time based on simulation results.
        
        Args:
            runs_per_day: Number of job runs per day
            source_change_rate: Average percentage of sources with new data per run
            warehouse_size: Snowflake warehouse size
            credit_cost: Cost per Snowflake credit (default $3)
            
        Returns:
            Comprehensive ROI projection
        """
        # Run daily simulation
        daily_sim = self.simulate_daily_runs(runs_per_day, source_change_rate)
        daily_totals = daily_sim["daily_totals"]
        
        # Get credits per hour for warehouse
        credits_per_hour = self.WAREHOUSE_CREDITS.get(warehouse_size, 4)
        
        # Calculate credits consumed
        core_hours_per_day = daily_totals["core_duration_seconds"] / 3600
        fusion_hours_per_day = daily_totals["fusion_duration_seconds"] / 3600
        
        core_credits_per_day = core_hours_per_day * credits_per_hour
        fusion_credits_per_day = fusion_hours_per_day * credits_per_hour
        credits_saved_per_day = core_credits_per_day - fusion_credits_per_day
        
        # Cost calculations
        cost_saved_per_day = credits_saved_per_day * credit_cost
        cost_saved_per_month = cost_saved_per_day * 30
        cost_saved_per_year = cost_saved_per_day * 365
        
        # Time calculations
        time_saved_per_day_hours = daily_totals["time_saved_seconds"] / 3600
        time_saved_per_month_hours = time_saved_per_day_hours * 30
        time_saved_per_year_hours = time_saved_per_day_hours * 365
        
        return {
            "simulation_params": {
                "runs_per_day": runs_per_day,
                "source_change_rate": source_change_rate,
                "warehouse_size": warehouse_size,
                "credits_per_hour": credits_per_hour,
                "credit_cost": credit_cost,
            },
            "dag_analysis": self.dag_analysis,
            "per_run_average": {
                "core_models_run": self.dag_analysis["total_models"],
                "fusion_models_run": round(daily_totals["fusion_models_run"] / runs_per_day),
                "models_skipped": round((daily_totals["core_models_run"] - daily_totals["fusion_models_run"]) / runs_per_day),
                "time_saved_pct": daily_totals["time_saved_pct"],
            },
            "daily": {
                "core_duration_hours": round(core_hours_per_day, 2),
                "fusion_duration_hours": round(fusion_hours_per_day, 2),
                "time_saved_hours": round(time_saved_per_day_hours, 2),
                "core_credits": round(core_credits_per_day, 2),
                "fusion_credits": round(fusion_credits_per_day, 2),
                "credits_saved": round(credits_saved_per_day, 2),
                "cost_saved": round(cost_saved_per_day, 2),
            },
            "monthly": {
                "time_saved_hours": round(time_saved_per_month_hours, 1),
                "credits_saved": round(credits_saved_per_day * 30, 1),
                "cost_saved": round(cost_saved_per_month, 2),
            },
            "annual": {
                "time_saved_hours": round(time_saved_per_year_hours, 0),
                "credits_saved": round(credits_saved_per_day * 365, 0),
                "cost_saved": round(cost_saved_per_year, 2),
            },
        }


# =============================================================================
# DBT PROJECT GENERATOR
# =============================================================================

class DBTProjectGenerator:
    """Generates dbt model SQL files based on configuration."""

    ENTITIES = ["customers", "orders", "products", "transactions", "events", "users", "accounts", "payments"]
    VERBS = ["joined", "filtered", "enriched", "aggregated", "pivoted"]

    def __init__(self):
        self._model_columns = {}  # model_name -> list of {"name": str, "type": str}
        self._source_schemas = {}  # lowercase source name -> schema entry with columns
        self._relationships = []  # FK relationships from schema

    # ---- Column metadata helpers ----

    def _get_columns_for_source(self, entity: str) -> list:
        """Get column list for a source table from the schema."""
        entity_lower = entity.lower()
        if entity_lower in self._source_schemas:
            return self._source_schemas[entity_lower].get('columns', [])
        return []

    def _get_primary_key(self, entity: str) -> str:
        """Return the primary key column name for a source table."""
        for col in self._get_columns_for_source(entity):
            if col.get('is_primary_key'):
                return col['name'].upper()
        # Fallback: first column ending in _id
        for col in self._get_columns_for_source(entity):
            if col['name'].lower().endswith('_id'):
                return col['name'].upper()
        return None

    def _get_timestamp_column(self, columns: list) -> str:
        """Find the first timestamp/date column in a column list."""
        for col in columns:
            ctype = col.get('type', '').upper()
            cname = col['name'].lower()
            if ctype in ('TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'DATE') or \
               cname in ('created_at', 'updated_at', 'order_date', 'event_timestamp',
                         'transaction_date', 'payment_date', 'last_updated'):
                return col['name'].lower()
        return None

    def _get_numeric_columns(self, columns: list) -> list:
        """Filter for numeric columns, excluding ID columns (aggregating IDs is meaningless)."""
        numeric_types = ('DECIMAL', 'INTEGER', 'INT', 'NUMBER', 'FLOAT', 'DOUBLE', 'NUMERIC')
        result = []
        for col in columns:
            ctype = col.get('type', '').upper()
            cname = col['name'].lower()
            # Skip ID columns — sum/avg on IDs makes no sense
            if cname.endswith('_id') or cname == 'id':
                continue
            if ctype in numeric_types or cname in ('amount', 'total_amount', 'price',
                'unit_price', 'quantity', 'quantity_on_hand', 'inventory_count', 'rating'):
                result.append(col)
        return result

    def _get_categorical_columns(self, columns: list) -> list:
        """Filter for categorical/status columns."""
        cat_names = ('status', 'type', 'category', 'tier', 'shipping_method',
                     'payment_method', 'device_type', 'event_type', 'transaction_type')
        return [c for c in columns if c['name'].lower() in cat_names]

    def _get_varchar_columns(self, columns: list) -> list:
        """Filter for VARCHAR columns excluding IDs."""
        return [c for c in columns if c.get('type', '').upper() == 'VARCHAR'
                and not c['name'].lower().endswith('_id')]

    def _find_join_key(self, model_a_cols: list, model_b_cols: list) -> str:
        """Find a shared _id column between two models for joining.

        Only returns _id columns to avoid many-to-many joins on non-unique
        columns like 'country' or 'status' which cause duplicate rows.
        Prefers columns that are a PK on at least one side.
        Returns None if no safe join key exists.
        """
        a_names = {c['name'].lower() for c in model_a_cols}
        b_names = {c['name'].lower() for c in model_b_cols}
        shared_id_cols = sorted(
            name for name in (a_names & b_names) if name.endswith('_id')
        )
        if not shared_id_cols:
            return None

        # Prefer columns that are a PK on at least one side (guarantees uniqueness)
        a_pks = {c['name'].lower() for c in model_a_cols if c.get('is_primary_key')}
        b_pks = {c['name'].lower() for c in model_b_cols if c.get('is_primary_key')}
        for name in shared_id_cols:
            if name in a_pks or name in b_pks:
                return name

        # Fall back to first shared _id column (still safer than non-_id columns)
        return shared_id_cols[0]

    def _get_model_columns(self, model_name: str) -> list:
        """Get the output columns for a previously generated model."""
        return self._model_columns.get(model_name, [])

    def generate_models(self, pipeline_config: dict, schema: dict = None,
                        target_database: str = "RAW_DATA", target_schema: str = "PUBLIC") -> dict:
        """Generate dbt model SQL files respecting user-specified counts and materializations.
        
        IMPORTANT: Sources are derived from the actual schema (generated data), NOT from pipeline_config.
        This ensures dbt models reference only tables that actually exist.
        """
        num_models = pipeline_config['num_models']
        materialization = pipeline_config['materialization']
        dag_depth = pipeline_config.get('dag_depth', 4)
        
        # Store target for sources.yml
        self.target_database = target_database.upper()
        self.target_schema = target_schema.upper()
        
        models = {}
        
        # CRITICAL: Use schema sources ONLY - these are the actual tables that exist
        if schema and schema.get('sources'):
            source_tables = [s['name'] for s in schema['sources']]
            print(f"✅ Using {len(source_tables)} sources from generated schema: {source_tables}")
        else:
            # This is an error state - we should always have a schema
            print(f"❌ ERROR: No schema provided! Cannot generate dbt project without knowing actual sources.")
            raise ValueError("Schema is required to generate dbt models. Generate data first.")
        
        # Store source tables for use in all model generation
        # These must match the actual table names in Snowflake (uppercase)
        self.available_entities = [t.lower() for t in source_tables]
        self.source_table_names = [t.upper() for t in source_tables]  # Actual Snowflake table names

        # Build column metadata registries
        self._model_columns = {}
        self._source_schemas = {s['name'].lower(): s for s in schema.get('sources', [])}
        self._relationships = schema.get('relationships', [])
        
        # Calculate target model counts by materialization type
        target_incremental_count = int(num_models * materialization['incremental'] / 100)
        target_table_count = int(num_models * materialization['table'] / 100)
        target_view_count = num_models - target_incremental_count - target_table_count
        
        # Build a list of materializations to assign
        materialization_list = (
            ['incremental'] * target_incremental_count +
            ['table'] * target_table_count +
            ['view'] * target_view_count
        )
        random.shuffle(materialization_list)  # Randomize to spread across model types
        mat_index = 0
        
        def get_next_materialization():
            nonlocal mat_index
            if mat_index < len(materialization_list):
                mat = materialization_list[mat_index]
                mat_index += 1
                return mat
            return 'view'  # Default fallback
        
        # ----- GENERATE STAGING MODELS (one per source) -----
        # Staging models are always views - they're just simple selects from source
        staging_models = []
        for entity in source_tables:
            entity_lower = entity.lower()
            model_name = f"stg_{entity_lower}"
            sql = self._generate_staging_sql(model_name, entity, schema)
            models[f"models/staging/{model_name}.sql"] = sql
            staging_models.append(model_name)
        
        num_staging = len(staging_models)
        
        # ----- CALCULATE REMAINING MODELS TO GENERATE -----
        remaining_models = num_models - num_staging
        generate_semantic_early = pipeline_config.get('generate_semantic_models', False)
        if remaining_models <= 0:
            # Just staging models
            models["models/sources.yml"] = self._generate_sources_yml(schema)  # Use schema only
            models["dbt_project.yml"] = self._generate_project_yml(include_semantic_layer=generate_semantic_early)
            return models
        
        # =====================================================================
        # REALISTIC DAG GENERATION WITH MULTIPLE LAYERS
        # =====================================================================
        # Structure: source -> staging -> int_layer1 -> int_layer2 -> int_layer3 -> marts -> reports
        # This creates realistic depth with multiple dependency chains
        
        # Calculate layer distribution for realistic DAG depth
        # Layer 1 intermediate: 25% (references staging)
        # Layer 2 intermediate: 20% (references layer 1 + staging)
        # Layer 3 intermediate: 15% (references layer 2 + layer 1)
        # Marts: 25% (references all intermediate layers)
        # Reports/Aggregates: 15% (references marts + intermediate)
        
        int_layer1_count = max(1, int(remaining_models * 0.25))
        int_layer2_count = max(1, int(remaining_models * 0.20))
        int_layer3_count = max(1, int(remaining_models * 0.15))
        marts_count = max(1, int(remaining_models * 0.25))
        reports_count = remaining_models - int_layer1_count - int_layer2_count - int_layer3_count - marts_count
        
        # ----- GENERATE INTERMEDIATE LAYER 1 (references staging) -----
        int_layer1_models = []
        used_int_names = set()
        layer1_verbs = ["cleaned", "filtered", "validated", "enriched"]
        
        for i in range(int_layer1_count):
            entity = self.available_entities[i % len(self.available_entities)]
            verb = layer1_verbs[i % len(layer1_verbs)]
            
            model_name = f"int_{entity}_{verb}"
            if model_name in used_int_names:
                model_name = f"int_{entity}_{verb}_{i}"
            used_int_names.add(model_name)
            
            # Layer 1 references 1-3 staging models
            num_deps = min(3, len(staging_models))
            deps = random.sample(staging_models, num_deps) if staging_models else []
            mat_type = get_next_materialization()
            
            sql = self._generate_intermediate_sql(model_name, deps, mat_type)
            models[f"models/intermediate/{model_name}.sql"] = sql
            int_layer1_models.append(model_name)
        
        # ----- GENERATE INTERMEDIATE LAYER 2 (references layer 1 + staging) -----
        int_layer2_models = []
        layer2_verbs = ["joined", "aggregated", "pivoted", "mapped"]
        
        for i in range(int_layer2_count):
            entity = self.available_entities[i % len(self.available_entities)]
            verb = layer2_verbs[i % len(layer2_verbs)]
            
            model_name = f"int_{entity}_{verb}"
            if model_name in used_int_names:
                model_name = f"int_{entity}_{verb}_{i}"
            used_int_names.add(model_name)
            
            # Layer 2 references layer 1 models + optionally staging
            deps = []
            if int_layer1_models:
                deps.extend(random.sample(int_layer1_models, min(2, len(int_layer1_models))))
            if staging_models and random.random() > 0.5:
                deps.append(random.choice(staging_models))
            mat_type = get_next_materialization()
            
            sql = self._generate_intermediate_sql(model_name, deps, mat_type)
            models[f"models/intermediate/{model_name}.sql"] = sql
            int_layer2_models.append(model_name)
        
        # ----- GENERATE INTERMEDIATE LAYER 3 (references layer 2 + layer 1) -----
        int_layer3_models = []
        layer3_verbs = ["combined", "transformed", "calculated", "derived"]
        
        for i in range(int_layer3_count):
            entity = self.available_entities[i % len(self.available_entities)]
            verb = layer3_verbs[i % len(layer3_verbs)]
            
            model_name = f"int_{entity}_{verb}"
            if model_name in used_int_names:
                model_name = f"int_{entity}_{verb}_{i}"
            used_int_names.add(model_name)
            
            # Layer 3 references layer 2 + optionally layer 1
            deps = []
            if int_layer2_models:
                deps.extend(random.sample(int_layer2_models, min(2, len(int_layer2_models))))
            if int_layer1_models and random.random() > 0.3:
                deps.append(random.choice(int_layer1_models))
            mat_type = get_next_materialization()
            
            sql = self._generate_intermediate_sql(model_name, deps, mat_type)
            models[f"models/intermediate/{model_name}.sql"] = sql
            int_layer3_models.append(model_name)
        
        # Combine all intermediate models
        intermediate_models = int_layer1_models + int_layer2_models + int_layer3_models
        
        # ----- GENERATE MARTS MODELS (references all intermediate layers) -----
        all_intermediate = int_layer1_models + int_layer2_models + int_layer3_models
        used_marts_names = set()
        prefixes = ["fct", "dim", "dim", "fct", "dim"]  # More dims than facts typically
        suffixes = ["core", "base", "main", "detail", "history", "current"]
        
        marts_models = []
        for i in range(marts_count):
            entity = self.available_entities[i % len(self.available_entities)]
            prefix = prefixes[i % len(prefixes)]
            
            model_name = f"{prefix}_{entity}"
            if model_name in used_marts_names:
                suffix = suffixes[(i // len(self.available_entities)) % len(suffixes)]
                model_name = f"{prefix}_{entity}_{suffix}"
            if model_name in used_marts_names:
                model_name = f"{prefix}_{entity}_{i}"
            used_marts_names.add(model_name)
            
            # Marts reference 2-4 models from various intermediate layers
            deps = []
            if int_layer3_models:
                deps.append(random.choice(int_layer3_models))
            if int_layer2_models:
                deps.append(random.choice(int_layer2_models))
            if int_layer1_models and random.random() > 0.5:
                deps.append(random.choice(int_layer1_models))
            mat_type = get_next_materialization()
            
            sql = self._generate_marts_sql(model_name, deps if deps else [staging_models[0]], mat_type)
            models[f"models/marts/{model_name}.sql"] = sql
            marts_models.append(model_name)
        
        # ----- GENERATE REPORT/AGGREGATE MODELS (references marts + intermediate) -----
        report_prefixes = ["rpt", "agg", "report", "summary", "kpi"]
        report_suffixes = ["daily", "weekly", "monthly", "quarterly", "ytd", "metrics", "dashboard"]
        
        for i in range(reports_count):
            entity = self.available_entities[i % len(self.available_entities)]
            prefix = report_prefixes[i % len(report_prefixes)]
            suffix = report_suffixes[i % len(report_suffixes)]
            
            model_name = f"{prefix}_{entity}_{suffix}"
            if model_name in used_marts_names:
                model_name = f"{prefix}_{entity}_{suffix}_{i}"
            used_marts_names.add(model_name)
            
            # Reports reference marts + optionally intermediate
            deps = []
            if marts_models:
                deps.extend(random.sample(marts_models, min(2, len(marts_models))))
            if all_intermediate and random.random() > 0.6:
                deps.append(random.choice(all_intermediate))
            mat_type = get_next_materialization()
            
            sql = self._generate_marts_sql(model_name, deps if deps else [staging_models[0]], mat_type)
            models[f"models/marts/{model_name}.sql"] = sql
        
        # Generate sources.yml from actual schema (not num_sources parameter)
        models["models/sources.yml"] = self._generate_sources_yml(schema)
        
        # Get test types and semantic model settings from config
        test_types = pipeline_config.get('test_types', ['not_null', 'unique'])
        generate_semantic = pipeline_config.get('generate_semantic_models', False)
        
        # Generate dbt_project.yml (with semantic layer config if enabled)
        models["dbt_project.yml"] = self._generate_project_yml(include_semantic_layer=generate_semantic)
        
        # ----- GENERATE SCHEMA YML FILES WITH TESTS AND DOCUMENTATION -----
        # Generate schema.yml for staging models
        staging_schema = self._generate_schema_yml("staging", staging_models, "Staging models - clean, renamed source data", test_types, schema)
        models["models/staging/schema.yml"] = staging_schema
        
        # Generate schema.yml for intermediate models (all layers)
        int_schema = self._generate_schema_yml("intermediate", intermediate_models, "Intermediate models - multi-layer business logic transformations", test_types, schema)
        models["models/intermediate/schema.yml"] = int_schema
        
        # Generate schema.yml for marts models (including reports/aggregates)
        all_marts = list(used_marts_names)  # This includes both marts and reports
        marts_schema = self._generate_schema_yml("marts", all_marts, "Marts models - final consumption layer including facts, dimensions, and reports", test_types, schema)
        models["models/marts/schema.yml"] = marts_schema
        
        # Log DAG structure summary
        print(f"📊 DAG Structure: {len(staging_models)} staging → {len(int_layer1_models)} int_L1 → {len(int_layer2_models)} int_L2 → {len(int_layer3_models)} int_L3 → {len(marts_models)} marts → {reports_count} reports")
        
        # ----- GENERATE GENERIC TESTS (one per file) -----
        models["tests/generic/test_not_null_if_incremental.sql"] = self._generate_generic_test_not_null_incremental()
        models["tests/generic/test_positive_values.sql"] = self._generate_generic_test_positive_values()
        models["tests/generic/test_not_in_future.sql"] = self._generate_generic_test_not_in_future()

        # ----- GENERATE SINGULAR TESTS -----
        models["tests/singular/assert_positive_values.sql"] = self._generate_singular_test_positive(marts_models)
        models["tests/singular/assert_row_counts.sql"] = self._generate_singular_test_row_count(staging_models)
        
        # ----- GENERATE SEMANTIC MODELS (if enabled) -----
        if generate_semantic and all_marts:
            # Add time spine model (required for semantic layer)
            models["models/utilities/time_spine.sql"] = self._generate_time_spine_simple_sql()
            models["models/utilities/schema.yml"] = self._generate_time_spine_yml()
            
            # Add semantic models and metrics (use marts_models, not reports)
            semantic_yml = self._generate_semantic_models(marts_models if marts_models else all_marts[:5], schema)
            models["models/semantic_models.yml"] = semantic_yml
        
        # Log model count summary
        total = len([k for k in models.keys() if k.endswith('.sql') and not k.startswith('tests/')])
        print(f"Generated {total} models: {num_staging} staging, {len(intermediate_models)} intermediate, {len(used_marts_names)} marts")
        
        return models
    
    def _generate_schema_yml(self, layer: str, model_names: list, layer_description: str,
                              test_types: list = None, schema: dict = None) -> str:
        """Generate schema.yml with tests on actual columns from the model registry.

        Test strategy by layer:
        - staging: not_null on PKs, FKs, and timestamps (source data should be complete)
        - intermediate/marts: not_null only on PKs (LEFT JOINs can produce NULLs in other columns)
        """
        if test_types is None:
            test_types = ['not_null', 'unique']

        models_yaml = []

        for model_name in model_names:
            cols = self._model_columns.get(model_name, [])
            display_name = model_name.replace('_', ' ').title()

            columns_yaml = []
            for col in cols:
                cname = col['name']
                ctype = col.get('type', 'VARCHAR').upper()
                is_pk = col.get('is_primary_key', False)

                col_tests = []
                if is_pk:
                    # Primary key: unique + not_null in all layers
                    col_tests.append("          - not_null")
                    col_tests.append("          - unique")
                elif layer == "staging":
                    # In staging, source data should be complete — test FKs and timestamps
                    if cname.endswith('_id'):
                        col_tests.append("          - not_null")
                    elif ctype in ('TIMESTAMP', 'TIMESTAMP_NTZ', 'DATE') or \
                         cname in ('created_at', 'updated_at', 'order_date', 'event_timestamp'):
                        col_tests.append("          - not_null")

                # Intermediate and marts: no not_null tests on non-PK columns
                # because LEFT JOINs and aggregations can produce NULLs

                col_entry = f"      - name: {cname}\n        description: \"{cname.replace('_', ' ').title()}\""
                if col_tests:
                    col_entry += f"\n        data_tests:\n" + chr(10).join(col_tests)
                columns_yaml.append(col_entry)

            if not columns_yaml:
                columns_yaml.append(f"      - name: id\n        description: \"Primary key\"")

            model_entry = f"""  - name: {model_name}
    description: "{layer.capitalize()} model: {display_name}"
    config:
      tags: ['{layer}']
    columns:
{chr(10).join(columns_yaml)}"""

            models_yaml.append(model_entry)

        return f"""version: 2

# {layer_description}

models:
{chr(10).join(models_yaml)}
"""

    def _generate_generic_test_not_null_incremental(self) -> str:
        """Generate generic test: not_null_if_incremental."""
        return """{% test not_null_if_incremental(model, column_name) %}

{% if is_incremental() %}
select *
from {{ model }}
where {{ column_name }} is null
{% else %}
select 1 where false
{% endif %}

{% endtest %}
"""

    def _generate_generic_test_positive_values(self) -> str:
        """Generate generic test: positive_values."""
        return """{% test positive_values(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
"""

    def _generate_generic_test_not_in_future(self) -> str:
        """Generate generic test: not_in_future."""
        return """{% test not_in_future(model, column_name) %}

select *
from {{ model }}
where cast({{ column_name }} as timestamp_ntz) > current_timestamp()

{% endtest %}
"""

    def _generate_singular_test_positive(self, marts_models: list) -> str:
        """Generate singular test that checks numeric columns in marts models are non-negative."""
        checks = []
        for m in marts_models:
            cols = self._model_columns.get(m, [])
            num_cols = self._get_numeric_columns(cols)
            for nc in num_cols[:2]:
                checks.append(
                    f"select\n    '{m}.{nc['name']}' as test_name,\n"
                    f"    {nc['name']} as tested_value\n"
                    f"from {{{{ ref('{m}') }}}}\n"
                    f"where {nc['name']} < 0"
                )

        if not checks:
            return "-- No numeric columns found in marts models to test\nselect 1 where false\n"

        return "-- Assert numeric columns in marts models are non-negative\n\n" + \
               "\nunion all\n".join(checks) + "\n"

    def _generate_singular_test_row_count(self, staging_models: list) -> str:
        """Generate singular test that validates staging models have rows."""
        checks = []
        for m in staging_models[:5]:
            checks.append(
                f"select\n    '{m}' as model_name,\n"
                f"    count(*) as row_count\n"
                f"from {{{{ ref('{m}') }}}}\n"
                f"having count(*) = 0"
            )

        if not checks:
            return "-- No staging models to test\nselect 1 where false\n"

        return "-- Assert staging models are not empty\n\n" + \
               "\nunion all\n".join(checks) + "\n"

    def _generate_time_spine_sql(self) -> str:
        """Generate time spine model for semantic layer."""
        return '''{{ config(materialized='table') }}

-- Time Spine model for dbt Semantic Layer
-- Generates one row per day for the past 3 years and next 1 year

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="dateadd(year, -3, current_date())",
        end_date="dateadd(year, 1, current_date())"
    ) }}
)

select
    date_day as date_day,
    date_trunc('week', date_day) as date_week,
    date_trunc('month', date_day) as date_month,
    date_trunc('quarter', date_day) as date_quarter,
    date_trunc('year', date_day) as date_year,
    dayofweek(date_day) as day_of_week,
    dayofmonth(date_day) as day_of_month,
    dayofyear(date_day) as day_of_year,
    weekofyear(date_day) as week_of_year,
    month(date_day) as month_num,
    quarter(date_day) as quarter_num,
    year(date_day) as year_num,
    case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend
from date_spine
'''

    def _generate_time_spine_simple_sql(self) -> str:
        """Generate simple time spine without dbt_utils dependency."""
        return '''{{ config(materialized='table') }}

-- Time Spine model for dbt Semantic Layer
-- Generates one row per day using Snowflake generator

with date_range as (
    select 
        dateadd(day, seq4(), dateadd(year, -3, current_date())) as date_day
    from table(generator(rowcount => 1461))  -- ~4 years of days
    where dateadd(day, seq4(), dateadd(year, -3, current_date())) <= dateadd(year, 1, current_date())
)

select
    date_day,
    date_trunc('week', date_day) as date_week,
    date_trunc('month', date_day) as date_month,
    date_trunc('quarter', date_day) as date_quarter,
    date_trunc('year', date_day) as date_year,
    dayofweek(date_day) as day_of_week,
    dayofmonth(date_day) as day_of_month,
    month(date_day) as month_num,
    quarter(date_day) as quarter_num,
    year(date_day) as year_num,
    case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend
from date_range
'''

    def _generate_time_spine_yml(self) -> str:
        """Generate time spine schema with Fusion-compatible time_spine configuration."""
        return '''version: 2

models:
  - name: time_spine
    description: "Daily time spine used by the semantic layer."

    time_spine:
      standard_granularity_column: date_day

    columns:
      - name: date_day
        description: "Base date column for daily grain."
        data_type: date
        granularity: day
'''

    def _generate_semantic_models(self, marts_models: list, schema: dict = None) -> str:
        """Generate semantic_models.yml using actual columns from model registry."""
        semantic_models = []
        used_entity_names = set()
        entity_name_map = {}

        for model_name in marts_models[:5]:
            base_entity = model_name.replace('fct_', '').replace('agg_', '').replace('dim_', '').replace('rpt_', '')
            entity_name = base_entity
            counter = 1
            while entity_name in used_entity_names:
                entity_name = f"{base_entity}_{counter}"
                counter += 1
            used_entity_names.add(entity_name)
            entity_name_map[model_name] = entity_name

            display_name = model_name.replace('_', ' ').title()
            cols = self._model_columns.get(model_name, [])

            # Find PK column for entity
            pk_col = None
            for c in cols:
                if c.get('is_primary_key') or c['name'].endswith('_id'):
                    pk_col = c['name']
                    break
            pk_col = pk_col or 'id'

            # Build measures from actual numeric columns
            num_cols = self._get_numeric_columns(cols)
            measures = [f"""      - name: {entity_name}_count
        description: "Count of {entity_name} records"
        agg: count
        expr: "1"
"""]
            for nc in num_cols[:3]:
                measures.append(f"""      - name: total_{nc['name']}
        description: "Sum of {nc['name']}"
        agg: sum
        expr: {nc['name']}
""")

            measures_yaml = "\n".join(measures)

            semantic_model = f"""  - name: sem_{model_name}
    description: "Semantic model for {display_name}"
    model: ref('{model_name}')

    entities:
      - name: {entity_name}_entity
        type: primary
        expr: {pk_col}

    measures:
{measures_yaml}"""
            semantic_models.append(semantic_model)

        return f"""version: 2

# Semantic Models for dbt Semantic Layer
# Compatible with dbt Fusion engine

semantic_models:
{chr(10).join(semantic_models)}
"""

    def _generate_advanced_metrics(self, marts_models: list, entity_name_map: dict = None) -> str:
        """Generate simple metrics for Fusion compatibility using actual measure names."""
        metrics = []
        used_names = set()

        if entity_name_map is None:
            entity_name_map = {}

        for model_name in marts_models[:3]:
            if model_name in entity_name_map:
                entity_name = entity_name_map[model_name]
            else:
                entity_name = model_name.replace('fct_', '').replace('agg_', '').replace('dim_', '').replace('rpt_', '')

            metric_base = entity_name
            counter = 1
            while f"total_{metric_base}" in used_names:
                metric_base = f"{entity_name}_{counter}"
                counter += 1

            used_names.add(f"total_{metric_base}")
            display_name = metric_base.replace('_', ' ').title()

            metrics.append(f"""  - name: total_{metric_base}
    description: "Total count of {metric_base}"
    label: "Total {display_name}"
    type: simple
    measure: {entity_name}_count
""")

        if metrics:
            return f"""metrics:
{chr(10).join(metrics)}"""
        return ""

    def _pick_materialization(self, index, inc_count, table_count, view_count):
        """Legacy method - kept for compatibility."""
        if index < inc_count:
            return "incremental"
        elif index < inc_count + table_count:
            return "table"
        return "view"
    
    def _generate_staging_sql(self, model_name, entity, schema=None):
        """Generate staging model SQL with explicit column selection and light transformations."""
        entity_upper = entity.upper()
        columns = self._get_columns_for_source(entity)

        if not columns:
            # Fallback if no column metadata available
            self._model_columns[model_name] = [{"name": "id", "type": "INTEGER"}]
            return f'''{{{{ config(materialized='view') }}}}

-- Staging model for {entity_upper}

select
    *
from {{{{ source('raw_data', '{entity_upper}') }}}}
'''

        select_lines = []
        output_cols = []
        for col in columns:
            col_upper = col['name'].upper()
            col_lower = col['name'].lower()
            ctype = col.get('type', 'VARCHAR').upper()

            if ctype in ('TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ'):
                select_lines.append(f"    cast({col_upper} as timestamp_ntz) as {col_lower}")
                output_cols.append({"name": col_lower, "type": "TIMESTAMP_NTZ",
                                    "is_primary_key": col.get('is_primary_key', False)})
            elif ctype == 'DATE':
                select_lines.append(f"    cast({col_upper} as date) as {col_lower}")
                output_cols.append({"name": col_lower, "type": "DATE",
                                    "is_primary_key": col.get('is_primary_key', False)})
            elif ctype in ('DECIMAL', 'NUMERIC', 'NUMBER'):
                select_lines.append(f"    cast({col_upper} as decimal(18,2)) as {col_lower}")
                output_cols.append({"name": col_lower, "type": "DECIMAL",
                                    "is_primary_key": col.get('is_primary_key', False)})
            elif ctype == 'INTEGER' or ctype == 'INT':
                select_lines.append(f"    {col_upper} as {col_lower}")
                output_cols.append({"name": col_lower, "type": "INTEGER",
                                    "is_primary_key": col.get('is_primary_key', False)})
            elif col_lower.endswith('_id'):
                # ID columns — keep as integer for reliable joins
                select_lines.append(f"    {col_upper} as {col_lower}")
                output_cols.append({"name": col_lower, "type": "INTEGER",
                                    "is_primary_key": col.get('is_primary_key', False)})
            elif col_lower in ('email',):
                select_lines.append(f"    lower(trim({col_upper})) as {col_lower}")
                output_cols.append({"name": col_lower, "type": "VARCHAR"})
            elif col_lower in ('name', 'first_name', 'last_name', 'product_name'):
                select_lines.append(f"    trim({col_upper}) as {col_lower}")
                output_cols.append({"name": col_lower, "type": "VARCHAR"})
            else:
                select_lines.append(f"    {col_upper} as {col_lower}")
                output_cols.append({"name": col_lower, "type": ctype,
                                    "is_primary_key": col.get('is_primary_key', False)})

        self._model_columns[model_name] = output_cols
        cols_sql = ",\n".join(select_lines)

        return f'''{{{{ config(materialized='view') }}}}

-- Staging model for {entity_upper}
-- Renames, casts, and cleans raw source columns

select
{cols_sql}
from {{{{ source('raw_data', '{entity_upper}') }}}}
'''
    
    def _generate_intermediate_sql(self, model_name, deps, mat_type):
        """Generate intermediate model SQL with realistic transformations based on verb."""
        if not deps:
            deps = [list(self._model_columns.keys())[0]] if self._model_columns else []

        primary_dep = deps[0] if deps else None
        primary_cols = self._get_model_columns(primary_dep) if primary_dep else []

        # Determine the unique key for incremental models
        pk_col = None
        for c in primary_cols:
            if c.get('is_primary_key') or c['name'].endswith('_id'):
                pk_col = c['name']
                break

        ts_col = self._get_timestamp_column(primary_cols)

        config = f"materialized='{mat_type}'"
        if mat_type == "incremental":
            uk = pk_col or 'id'
            config += f", unique_key='{uk}', incremental_strategy='merge', on_schema_change='append_new_columns'"

        # Determine verb from model name to choose transformation pattern
        verb = None
        for v in ["cleaned", "filtered", "validated", "enriched", "joined", "aggregated",
                   "pivoted", "mapped", "combined", "transformed", "calculated", "derived"]:
            if v in model_name:
                verb = v
                break

        # Build CTE lookup — we'll filter to only deps actually used in the body
        _all_cte_parts = {}
        for dep in deps:
            dep_cols = self._get_model_columns(dep)
            col_select = ", ".join([c['name'] for c in dep_cols]) if dep_cols else "*"
            _all_cte_parts[dep] = (
                f"    {dep} as (\n        select {col_select}\n"
                f"        from {{{{ ref('{dep}') }}}}\n    )"
            )

        # Build SELECT based on verb
        output_cols = []
        if verb in ("filtered", "validated", "cleaned") and primary_cols:
            select_cols = [c['name'] for c in primary_cols]
            output_cols = list(primary_cols)
            # Add a WHERE clause for filtering
            cat_cols = self._get_categorical_columns(primary_cols)
            where_clause = ""
            if verb == "filtered" and cat_cols:
                where_clause = f"\nwhere {cat_cols[0]['name']} is not null"
            elif verb == "validated" and pk_col:
                where_clause = f"\nwhere {pk_col} is not null"
            elif verb == "cleaned":
                where_clause = ""
                # Just select cleaned columns
            select_sql = ",\n    ".join(select_cols)
            body = f"select\n    {select_sql}\nfrom {primary_dep}{where_clause}"

        elif verb == "enriched" and primary_cols:
            select_cols = [c['name'] for c in primary_cols]
            output_cols = list(primary_cols)
            # Add computed columns
            enrichments = []
            if ts_col:
                enrichments.append(f"datediff('day', {ts_col}, current_timestamp()) as days_since_{ts_col}")
                output_cols.append({"name": f"days_since_{ts_col}", "type": "INTEGER"})
            num_cols = self._get_numeric_columns(primary_cols)
            if num_cols:
                nc = num_cols[0]['name']
                enrichments.append(f"case when {nc} > 0 then 'positive' else 'non_positive' end as {nc}_category")
                output_cols.append({"name": f"{nc}_category", "type": "VARCHAR"})
            all_cols = select_cols + enrichments
            select_sql = ",\n    ".join(all_cols)
            body = f"select\n    {select_sql}\nfrom {primary_dep}"

        elif verb in ("joined",) and len(deps) >= 2:
            dep_a = deps[0]
            dep_b = deps[1]
            a_cols = self._get_model_columns(dep_a)
            b_cols = self._get_model_columns(dep_b)
            join_key = self._find_join_key(a_cols, b_cols)

            if join_key and a_cols and b_cols:
                # Select columns from both, prefixing the second table's non-key cols
                a_select = [f"{dep_a}.{c['name']}" for c in a_cols]
                b_select = [f"{dep_b}.{c['name']} as {dep_b.replace('stg_', '').replace('int_', '')}_{c['name']}"
                            for c in b_cols if c['name'] != join_key]
                output_cols = list(a_cols)
                for c in b_cols:
                    if c['name'] != join_key:
                        alias = f"{dep_b.replace('stg_', '').replace('int_', '')}_{c['name']}"
                        output_cols.append({"name": alias, "type": c.get('type', 'VARCHAR')})
                all_select = a_select + b_select
                select_sql = ",\n    ".join(all_select)

                # Check if join key is PK on the right side — if not, the LEFT JOIN
                # can fan out rows. Add dedup to preserve uniqueness on the left PK.
                b_pks = {c['name'].lower() for c in b_cols if c.get('is_primary_key')}
                needs_dedup = join_key.lower() not in b_pks and pk_col

                if needs_dedup:
                    inner_select = ",\n        ".join(all_select)
                    body = (
                        f"select * from (\n"
                        f"    select\n        {inner_select},\n"
                        f"        row_number() over (partition by {dep_a}.{pk_col} order by {dep_a}.{pk_col}) as _dedup_rn\n"
                        f"    from {dep_a}\n"
                        f"    left join {dep_b}\n"
                        f"        on {dep_a}.{join_key} = {dep_b}.{join_key}\n"
                        f") where _dedup_rn = 1"
                    )
                else:
                    body = f"select\n    {select_sql}\nfrom {dep_a}\nleft join {dep_b}\n    on {dep_a}.{join_key} = {dep_b}.{join_key}"
            else:
                # No shared key — fall back to just selecting from primary
                select_cols = [c['name'] for c in primary_cols] if primary_cols else ["*"]
                output_cols = list(primary_cols) if primary_cols else []
                select_sql = ",\n    ".join(select_cols)
                body = f"select\n    {select_sql}\nfrom {primary_dep}"

        elif verb == "aggregated" and primary_cols:
            cat_cols = self._get_categorical_columns(primary_cols)
            num_cols = self._get_numeric_columns(primary_cols)
            group_col = cat_cols[0]['name'] if cat_cols else (primary_cols[0]['name'] if primary_cols else 'id')
            agg_exprs = [group_col, f"count(*) as record_count"]
            output_cols = [{"name": group_col, "type": "VARCHAR"}, {"name": "record_count", "type": "INTEGER"}]
            for nc in num_cols[:3]:
                agg_exprs.append(f"sum({nc['name']}) as total_{nc['name']}")
                agg_exprs.append(f"avg({nc['name']}) as avg_{nc['name']}")
                output_cols.append({"name": f"total_{nc['name']}", "type": "DECIMAL"})
                output_cols.append({"name": f"avg_{nc['name']}", "type": "DECIMAL"})
            select_sql = ",\n    ".join(agg_exprs)
            body = f"select\n    {select_sql}\nfrom {primary_dep}\ngroup by {group_col}"

        elif verb in ("pivoted", "mapped") and primary_cols:
            cat_cols = self._get_categorical_columns(primary_cols)
            select_cols = [c['name'] for c in primary_cols]
            output_cols = list(primary_cols)
            if cat_cols:
                cc = cat_cols[0]['name']
                select_cols.append(
                    f"case\n        when {cc} in ('active', 'completed', 'delivered') then 'success'\n"
                    f"        when {cc} in ('pending', 'shipped') then 'in_progress'\n"
                    f"        else 'other'\n    end as {cc}_group"
                )
                output_cols.append({"name": f"{cc}_group", "type": "VARCHAR"})
            select_sql = ",\n    ".join(select_cols)
            body = f"select\n    {select_sql}\nfrom {primary_dep}"

        else:
            # Default: select columns from primary dep
            select_cols = [c['name'] for c in primary_cols] if primary_cols else ["*"]
            output_cols = list(primary_cols) if primary_cols else []
            select_sql = ",\n    ".join(select_cols)
            body = f"select\n    {select_sql}\nfrom {primary_dep}"

        self._model_columns[model_name] = output_cols

        # Only include CTEs that are actually referenced in the body
        used_ctes = [_all_cte_parts[dep] for dep in deps if dep in _all_cte_parts and dep in body]
        ctes_sql = ",\n\n".join(used_ctes) if used_ctes else f"    {primary_dep} as (select * from {{{{ ref('{primary_dep}') }}}})"

        return f'''{{{{ config({config}) }}}}

-- Intermediate transformation: {model_name}
-- Dependencies: {', '.join(deps)}

with
{ctes_sql}

{body}
'''

    def _generate_marts_sql(self, model_name, deps, mat_type):
        """Generate marts model SQL with prefix-aware patterns (fct/dim/rpt)."""
        if not deps:
            self._model_columns[model_name] = [{"name": "id", "type": "INTEGER"}]
            return f'''{{{{ config(materialized='{mat_type}') }}}}

-- Marts model: {model_name}

select
    1 as id,
    current_timestamp() as created_at
'''

        primary_dep = deps[0]
        primary_cols = self._get_model_columns(primary_dep)
        all_deps_comment = ', '.join(deps)

        # Find PK and timestamp for incremental config
        pk_col = None
        for c in primary_cols:
            if c.get('is_primary_key') or c['name'].endswith('_id'):
                pk_col = c['name']
                break
        ts_col = self._get_timestamp_column(primary_cols)

        config = f"materialized='{mat_type}'"
        if mat_type == "incremental":
            uk = pk_col or 'id'
            config += f", unique_key='{uk}', incremental_strategy='merge', on_schema_change='append_new_columns'"

        # Build CTEs — no incremental WHERE filter (same rationale as intermediate)
        # Build CTE lookup — we'll filter to only deps actually used in the body
        _all_cte_parts = {}
        for dep in deps:
            dep_cols = self._get_model_columns(dep)
            col_select = ", ".join([c['name'] for c in dep_cols]) if dep_cols else "*"
            _all_cte_parts[dep] = (
                f"    {dep} as (\n        select {col_select}\n"
                f"        from {{{{ ref('{dep}') }}}}\n    )"
            )

        # Determine model type from prefix
        output_cols = []
        if model_name.startswith("fct_"):
            # Fact table: select keys, measures, timestamps from primary; join if multiple deps
            select_exprs = []
            id_cols = [c for c in primary_cols if c['name'].endswith('_id')]
            num_cols = self._get_numeric_columns(primary_cols)
            ts_cols = [c for c in primary_cols if c.get('type', '').upper() in
                       ('TIMESTAMP', 'TIMESTAMP_NTZ', 'DATE') or c['name'] in
                       ('created_at', 'order_date', 'transaction_date', 'event_timestamp', 'payment_date')]

            for c in id_cols:
                select_exprs.append(f"{primary_dep}.{c['name']}")
                output_cols.append(c)
            for c in ts_cols:
                if c not in id_cols:
                    select_exprs.append(f"{primary_dep}.{c['name']}")
                    if c not in output_cols:
                        output_cols.append(c)
            for c in num_cols:
                if c not in id_cols and c not in ts_cols:
                    select_exprs.append(f"{primary_dep}.{c['name']}")
                    if c not in output_cols:
                        output_cols.append(c)

            # If we have a second dep, try to join and pull extra columns
            join_clause = ""
            needs_dedup = False
            if len(deps) >= 2:
                dep_b = deps[1]
                b_cols = self._get_model_columns(dep_b)
                join_key = self._find_join_key(primary_cols, b_cols)
                if join_key and b_cols:
                    join_clause = f"\nleft join {dep_b}\n    on {primary_dep}.{join_key} = {dep_b}.{join_key}"
                    for c in b_cols[:3]:
                        if c['name'] != join_key and c['name'] not in [oc['name'] for oc in output_cols]:
                            alias = c['name']
                            select_exprs.append(f"{dep_b}.{alias}")
                            output_cols.append({"name": alias, "type": c.get('type', 'VARCHAR')})
                    # Check if join key is PK on right side — if not, dedup needed
                    b_pks = {c['name'].lower() for c in b_cols if c.get('is_primary_key')}
                    needs_dedup = join_key.lower() not in b_pks and pk_col

            if not select_exprs:
                select_exprs = [f"{primary_dep}.*"]
                output_cols = list(primary_cols)

            select_sql = ",\n    ".join(select_exprs)
            if needs_dedup and join_clause:
                inner_select = ",\n        ".join(select_exprs)
                body = (
                    f"select * from (\n"
                    f"    select\n        {inner_select},\n"
                    f"        row_number() over (partition by {primary_dep}.{pk_col} order by {primary_dep}.{pk_col}) as _dedup_rn\n"
                    f"    from {primary_dep}{join_clause}\n"
                    f") where _dedup_rn = 1"
                )
            else:
                body = f"select\n    {select_sql}\nfrom {primary_dep}{join_clause}"

        elif model_name.startswith("dim_"):
            # Dimension table: select descriptive attributes, deduplicate if possible
            select_cols = [c['name'] for c in primary_cols]
            output_cols = list(primary_cols)

            if pk_col and ts_col and len(primary_cols) > 2:
                # SCD Type 1: keep latest row per PK
                inner_select = ",\n        ".join(select_cols)
                body = (
                    f"select\n    {', '.join(select_cols)}\n"
                    f"from (\n"
                    f"    select\n        {inner_select},\n"
                    f"        row_number() over (partition by {pk_col} order by {ts_col} desc) as _rn\n"
                    f"    from {primary_dep}\n"
                    f")\nwhere _rn = 1"
                )
            else:
                select_sql = ",\n    ".join(select_cols)
                body = f"select distinct\n    {select_sql}\nfrom {primary_dep}"

        else:
            # Report/agg/summary/kpi: aggregate by time and/or category
            cat_cols = self._get_categorical_columns(primary_cols)
            num_cols = self._get_numeric_columns(primary_cols)

            group_cols = []
            select_exprs = []

            if ts_col:
                select_exprs.append(f"date_trunc('day', {ts_col}) as report_date")
                group_cols.append(f"date_trunc('day', {ts_col})")
                output_cols.append({"name": "report_date", "type": "DATE"})
            if cat_cols:
                cc = cat_cols[0]['name']
                select_exprs.append(cc)
                group_cols.append(cc)
                output_cols.append({"name": cc, "type": "VARCHAR"})

            select_exprs.append("count(*) as record_count")
            output_cols.append({"name": "record_count", "type": "INTEGER"})
            for nc in num_cols[:3]:
                select_exprs.append(f"sum({nc['name']}) as total_{nc['name']}")
                select_exprs.append(f"avg({nc['name']}) as avg_{nc['name']}")
                output_cols.append({"name": f"total_{nc['name']}", "type": "DECIMAL"})
                output_cols.append({"name": f"avg_{nc['name']}", "type": "DECIMAL"})

            if not group_cols:
                # No grouping possible — just select from primary
                select_cols = [c['name'] for c in primary_cols] if primary_cols else ["*"]
                output_cols = list(primary_cols)
                select_sql = ",\n    ".join(select_cols)
                body = f"select\n    {select_sql}\nfrom {primary_dep}"
            else:
                select_sql = ",\n    ".join(select_exprs)
                group_sql = ", ".join(group_cols)
                body = f"select\n    {select_sql}\nfrom {primary_dep}\ngroup by {group_sql}"

        self._model_columns[model_name] = output_cols

        # Only include CTEs that are actually referenced in the body
        used_ctes = [_all_cte_parts[dep] for dep in deps if dep in _all_cte_parts and dep in body]
        ctes_sql = ",\n\n".join(used_ctes) if used_ctes else f"    {primary_dep} as (select * from {{{{ ref('{primary_dep}') }}}})"

        return f'''{{{{ config({config}) }}}}

-- Marts model: {model_name}
-- Dependencies: {all_deps_comment}

with
{ctes_sql}

{body}
'''

    def _generate_sources_yml(self, schema=None):
        """Generate sources.yml from the ACTUAL schema - no num_sources parameter.
        
        This ensures sources.yml only contains tables that actually exist in the generated data.
        """
        # Get database and schema from instance or use defaults
        database = getattr(self, 'target_database', 'RAW_DATA')
        schema_name = getattr(self, 'target_schema', 'PUBLIC')
        
        if schema and schema.get('sources'):
            # Use ALL sources from the schema - these are the actual tables that were created
            tables_yaml = []
            for source in schema['sources']:  # No slicing - use all sources in schema
                # Table names in Snowflake are uppercase
                table_name = source['name'].upper()
                table_yaml = f"      - name: {table_name}"
                if source.get('description'):
                    table_yaml += f"\n        description: \"{source['description']}\""
                
                if source.get('columns'):
                    table_yaml += "\n        columns:"
                    for col in source['columns']:
                        col_name = col['name'].upper()
                        table_yaml += f"\n          - name: {col_name}"
                        if col.get('description'):
                            table_yaml += f"\n            description: \"{col['description']}\""
                
                tables_yaml.append(table_yaml)
            
            tables = "\n".join(tables_yaml)
            print(f"✅ Generated sources.yml with {len(schema['sources'])} tables from schema")
        else:
            # Use stored source table names if available
            if hasattr(self, 'source_table_names') and self.source_table_names:
                tables = "\n".join([f"      - name: {name}" for name in self.source_table_names])
                print(f"⚠️ Generated sources.yml from stored table names: {self.source_table_names}")
            else:
                print("❌ ERROR: No schema or source_table_names available for sources.yml!")
                tables = "      - name: MISSING_SOURCE  # ERROR: No schema provided"
        
        return f'''version: 2

sources:
  - name: raw_data
    database: {database}
    schema: {schema_name}
    tables:
{tables}
'''
    
    def _generate_project_yml(self, include_semantic_layer: bool = False):
        """Generate dbt_project.yml.
        
        NOTE: Materializations are NOT set at the project level to avoid conflicts
        with model-level configs. Each model SQL file contains its own 
        {{ config(materialized='...') }} block, which allows for the user's 
        configured mix of materializations across all layers.
        
        This approach:
        - Honors the exact materialization mix configured by the user
        - Avoids conflicts between project and model-level settings
        - Makes materialization explicit and visible in each model file
        - Works correctly with dbt Fusion engine
        """
        base_config = '''name: 'sao_sandbox'
version: '1.0.0'
config-version: 2

profile: 'sao_sandbox'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

clean-targets:
  - "target"
  - "dbt_packages"

# Materializations are set at the model level (in each .sql file's config block)
# to support flexible materialization mixes across all layers.
# This avoids conflicts between project-level and model-level settings.

models:
  sao_sandbox:
    # Handle materialization changes automatically (drop and recreate if type changes)
    +on_configuration_change: apply
    staging:
      +tags: ['staging']
    intermediate:
      +tags: ['intermediate']
    marts:
      +tags: ['marts']'''
        
        # Only add utilities section if semantic models are enabled
        if include_semantic_layer:
            base_config += '''
    utilities:
      +materialized: table'''
        
        base_config += '\n'
        return base_config

# =============================================================================
# VISUALIZATION FUNCTIONS
# =============================================================================

COLORS = {
    'primary': '#FF694A',
    'secondary': '#FF8F6B',
    'accent': '#FFB347',
    'success': '#4ade80',
    'info': '#60a5fa',
    'background': '#1e1e3f',
    'surface': '#2a2a5a',
    'text': '#ffffff',
    'text_muted': '#a0a0c0',
}


def create_cost_comparison_chart(savings_analysis):
    """Create a cost comparison bar chart."""
    categories = ['Monthly Cost', 'Annual Cost']
    before = [
        savings_analysis.get('cost_before', 0),
        savings_analysis.get('annual_cost_before', 0)
    ]
    after = [
        savings_analysis.get('cost_after', 0),
        savings_analysis.get('annual_cost_after', 0)
    ]
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Without SAO (dbt Core)',
        x=categories,
        y=before,
        marker_color=COLORS['text_muted'],
        text=[f'${v:,.0f}' for v in before],
        textposition='outside',
    ))
    
    fig.add_trace(go.Bar(
        name='With SAO (dbt Fusion)',
        x=categories,
        y=after,
        marker_color=COLORS['success'],
        text=[f'${v:,.0f}' for v in after],
        textposition='outside',
    ))
    
    fig.update_layout(
        barmode='group',
        title=dict(text='Cost Comparison: Core vs Fusion', font=dict(color=COLORS['text'], size=16)),
        xaxis=dict(title='', tickfont=dict(color=COLORS['text'])),
        yaxis=dict(title=dict(text='Cost (USD)', font=dict(color=COLORS['text'])), tickfont=dict(color=COLORS['text']), gridcolor='rgba(255,255,255,0.1)'),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1, font=dict(color=COLORS['text'])),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=80, b=40),
    )
    
    return fig


def create_sao_impact_chart(simulation_results):
    """Create a chart showing SAO impact on model execution."""
    run_details = simulation_results.get('run_details', [])
    
    if not run_details:
        return go.Figure()
    
    runs = [r['run'] for r in run_details]
    core_models = [r['models_run_core'] for r in run_details]
    fusion_models = [r['models_run_fusion'] for r in run_details]
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=runs, y=core_models,
        mode='lines+markers',
        name='dbt Core (All Models)',
        line=dict(color=COLORS['text_muted'], width=2),
        marker=dict(size=6),
    ))
    
    fig.add_trace(go.Scatter(
        x=runs, y=fusion_models,
        mode='lines+markers',
        name='dbt Fusion (SAO Optimized)',
        line=dict(color=COLORS['primary'], width=2),
        marker=dict(size=6),
        fill='tonexty',
        fillcolor='rgba(255,105,74,0.2)',
    ))
    
    fig.update_layout(
        title=dict(text='Models Executed per Run', font=dict(color=COLORS['text'], size=16)),
        xaxis=dict(title=dict(text='Run Number', font=dict(color=COLORS['text'])), tickfont=dict(color=COLORS['text']), gridcolor='rgba(255,255,255,0.1)'),
        yaxis=dict(title=dict(text='Models Executed', font=dict(color=COLORS['text'])), tickfont=dict(color=COLORS['text']), gridcolor='rgba(255,255,255,0.1)'),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1, font=dict(color=COLORS['text'])),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=80, b=40),
        hovermode='x unified',
    )
    
    return fig


# =============================================================================
# COST CALCULATOR
# =============================================================================

class SAOCostCalculator:
    """Calculate cost savings from SAO."""
    
    def __init__(self, credit_cost=3.0):
        self.credit_cost = credit_cost
        self.warehouse_credits = {
            "X-Small": 1.0, "Small": 2.0, "Medium": 4.0,
            "Large": 8.0, "X-Large": 16.0, "2X-Large": 32.0,
        }
    
    def simulate_pipeline_runs(self, pipeline_config, num_simulation_runs=24, seed=42):
        """Simulate pipeline runs with and without SAO."""
        np.random.seed(seed)
        random.seed(seed)
        
        num_models = pipeline_config['num_models']
        num_sources = pipeline_config['num_sources']
        source_change_rate = pipeline_config['source_change_rate'] / 100.0
        dag_depth = pipeline_config['dag_depth']
        materialization = pipeline_config['materialization']
        
        base_time_per_model = self._estimate_model_execution_time(
            pipeline_config['avg_row_count'],
            pipeline_config['avg_column_count'],
            pipeline_config['warehouse_size']
        )
        
        run_details = []
        total_models_skipped = 0
        total_time_saved = 0
        
        for run_num in range(num_simulation_runs):
            sources_changed = np.random.binomial(num_sources, source_change_rate)
            models_affected = self._calculate_affected_models(
                num_models, sources_changed, num_sources, dag_depth, materialization
            )
            
            core_models_run = num_models
            core_time = core_models_run * base_time_per_model
            fusion_models_run = models_affected
            fusion_time = fusion_models_run * base_time_per_model
            
            core_time *= np.random.uniform(0.9, 1.1)
            fusion_time *= np.random.uniform(0.9, 1.1)
            
            models_skipped = core_models_run - fusion_models_run
            time_saved = core_time - fusion_time
            
            total_models_skipped += models_skipped
            total_time_saved += time_saved
            
            run_details.append({
                'run': run_num + 1,
                'sources_changed': sources_changed,
                'models_run_core': core_models_run,
                'models_run_fusion': fusion_models_run,
                'models_skipped': models_skipped,
                'time_core_sec': round(core_time, 2),
                'time_fusion_sec': round(fusion_time, 2),
                'time_saved_sec': round(time_saved, 2),
            })
        
        avg_models_skipped = total_models_skipped / num_simulation_runs
        avg_core_time = sum(r['time_core_sec'] for r in run_details) / num_simulation_runs
        avg_fusion_time = sum(r['time_fusion_sec'] for r in run_details) / num_simulation_runs
        
        time_saved_percentage = ((avg_core_time - avg_fusion_time) / avg_core_time * 100) if avg_core_time > 0 else 0
        
        return {
            'run_details': run_details,
            'total_runs': num_simulation_runs,
            'avg_models_skipped': round(avg_models_skipped, 1),
            'avg_core_time_sec': round(avg_core_time, 2),
            'avg_fusion_time_sec': round(avg_fusion_time, 2),
            'time_saved_percentage': round(time_saved_percentage, 1),
            'skip_rate': round(avg_models_skipped / pipeline_config['num_models'] * 100, 1),
        }
    
    def _estimate_model_execution_time(self, avg_row_count, avg_column_count, warehouse_size):
        row_factor = np.log10(max(avg_row_count, 1000)) / 6
        column_factor = avg_column_count / 100
        warehouse_multipliers = {"X-Small": 1.0, "Small": 0.6, "Medium": 0.35, "Large": 0.2, "X-Large": 0.12, "2X-Large": 0.07}
        warehouse_mult = warehouse_multipliers.get(warehouse_size, 0.35)
        base_time = 5 + (row_factor * 20) + (column_factor * 5)
        return base_time * warehouse_mult
    
    def _calculate_affected_models(self, num_models, sources_changed, total_sources, dag_depth, materialization):
        if sources_changed == 0:
            view_pct = materialization.get('view', 0) / 100.0
            return max(1, int(num_models * view_pct * 0.1))
        
        source_impact = sources_changed / total_sources
        depth_factor = 1 + (dag_depth - 1) * 0.1
        
        incremental_pct = materialization.get('incremental', 0) / 100.0
        table_pct = materialization.get('table', 0) / 100.0
        view_pct = materialization.get('view', 0) / 100.0
        
        incremental_affected = incremental_pct * source_impact * 0.3
        table_affected = table_pct * source_impact * depth_factor
        view_affected = view_pct * 0.8
        
        total_affected_pct = incremental_affected + table_affected + view_affected
        total_affected_pct *= np.random.uniform(0.8, 1.2)
        total_affected_pct = min(max(total_affected_pct, 0.05), 1.0)
        
        return max(1, int(num_models * total_affected_pct))
    
    def calculate_savings(self, simulation_results, warehouse_size, current_monthly_cost):
        credits_per_hour = self.warehouse_credits.get(warehouse_size, 4.0)
        time_saved_percentage = simulation_results['time_saved_percentage']
        
        if current_monthly_cost > 0:
            monthly_savings = current_monthly_cost * (time_saved_percentage / 100)
            cost_after = current_monthly_cost - monthly_savings
        else:
            monthly_savings = 0
            cost_after = 0
            current_monthly_cost = 0
        
        annual_savings = monthly_savings * 12
        estimated_upgrade_cost = current_monthly_cost * 0.3
        roi = annual_savings / (estimated_upgrade_cost * 12) if estimated_upgrade_cost > 0 else 0
        
        return {
            'monthly_savings': round(monthly_savings, 2),
            'annual_savings': round(annual_savings, 2),
            'cost_before': round(current_monthly_cost, 2),
            'cost_after': round(cost_after, 2),
            'savings_percentage': round(time_saved_percentage, 1),
            'annual_cost_before': round(current_monthly_cost * 12, 2),
            'annual_cost_after': round(cost_after * 12, 2),
            'roi_estimate': round(roi, 1),
        }


# =============================================================================
# PAGE CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="SAO Cost Savings Estimator",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #FF6D00;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        color: #a0a0c0;
        text-align: center;
        font-size: 1.2rem;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(145deg, #1e1e3f 0%, #2a2a5a 100%);
        border: 1px solid #3a3a6a;
        border-radius: 16px;
        padding: 1.5rem;
        margin: 0.5rem 0;
    }
    .savings-highlight {
        background: linear-gradient(145deg, #1a3a2a 0%, #2a5a4a 100%);
        border: 2px solid #4ade80;
        border-radius: 20px;
        padding: 2rem;
        text-align: center;
        margin: 1rem 0;
    }
    .savings-amount {
        font-size: 3rem;
        font-weight: 700;
        color: #4ade80;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# SESSION STATE
# =============================================================================

def init_session_state():
    defaults = {
        'pipeline_config': get_default_pipeline_config(),
        'simulation_complete': False,
        'simulation_results': None,
        'savings_analysis': None,
        'dbt_connected': False,
        'dbt_api': None,
        'generated_models': None,
        'selected_project': None,
        'selected_environment': None,
        'generated_schema': None,
        'generated_ddl': None,
        'generated_dataframes': None,
        'core_job_id': None,
        'fusion_job_id': None,
        'sf_connected': False,
        'sf_loader': None,
        'sf_account': '',
        'sf_user': '',
        'sf_password': '',
        'sf_warehouse': 'COMPUTE_WH',
        'github_token': '',
        'github_repo': '',
        'customer_description': '',
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


# =============================================================================
# SIDEBAR - DBT CLOUD CONFIG
# =============================================================================

def render_sidebar():
    """Render sidebar with collapsible configuration sections."""
    with st.sidebar:
        # =================================================================
        # dbt Platform Configuration
        # =================================================================
        with st.expander("🔑 dbt Platform Configuration", expanded=not st.session_state.get('dbt_connected')):
            account_id = st.text_input(
                "Account ID",
                value="",
                help="Your dbt Cloud account ID (found in URL)",
                key="dbt_account_id"
            )
            
            api_token = st.text_input(
                "API Token",
                value="",
                type="password",
                help="Service token or personal access token",
                key="dbt_api_token"
            )
            
            # dbt Cloud URL input
            dbt_host = st.text_input(
                "dbt Cloud Host",
                value="cloud.getdbt.com",
                help="Your dbt Cloud hostname (e.g., cloud.getdbt.com, vb239.us1.dbt.com)"
            )
            
            if st.button("🔗 Connect to dbt Cloud"):
                if account_id and api_token:
                    # Build the API URL from the host
                    base_url = f"https://{dbt_host}/api/v2"
                    api = DBTCloudAPI(account_id, api_token, base_url)
                    if api.test_connection():
                        st.session_state['dbt_api'] = api
                        st.session_state['dbt_connected'] = True
                        st.success("✅ Connected!")
                    else:
                        st.error(f"❌ Connection failed: {api.last_error}")
                else:
                    st.warning("Please enter both Account ID and API Token")
            
            if st.session_state.get('dbt_connected'):
                st.success("✅ Connected to dbt Cloud")
        
        # =================================================================
        # Project Selection (only show if connected)
        # =================================================================
        if st.session_state.get('dbt_connected'):
            with st.expander("📁 Project Selection", expanded=True):
                api = st.session_state['dbt_api']
                projects = api.list_projects()
                
                if projects:
                    project_options = {p['name']: p['id'] for p in projects}
                    selected_project_name = st.selectbox(
                        "Select Project",
                        options=list(project_options.keys())
                    )
                    
                    if selected_project_name:
                        project_id = project_options[selected_project_name]
                        st.session_state['selected_project'] = project_id
                        
                        environments = api.list_environments(project_id)
                        if environments:
                            # Filter for deployment environments only (type='deployment')
                            # and show environment type in the name
                            env_options = {}
                            for e in environments:
                                env_type = e.get('type', 'unknown')
                                env_label = f"{e['name']} ({env_type})"
                                env_options[env_label] = e['id']
                            
                            # Sort to show deployment envs first
                            sorted_envs = sorted(env_options.keys(), key=lambda x: '0' if 'deployment' in x else '1')
                            
                            selected_env_name = st.selectbox(
                                "Select Environment",
                                options=sorted_envs,
                                help="Select a 'deployment' type environment for production jobs"
                            )
                            if selected_env_name:
                                st.session_state['selected_environment'] = env_options[selected_env_name]
                                if 'development' in selected_env_name:
                                    st.warning("⚠️ Development env selected")
        
        # =================================================================
        # Anthropic Configuration
        # =================================================================
        with st.expander("🤖 Anthropic Configuration", expanded=not st.session_state.get('anthropic_api_key')):
            st.caption("For generating realistic synthetic data with Claude")

            anthropic_key = st.text_input(
                "Anthropic API Key",
                value="",
                type="password",
                help="Required for Claude-powered synthetic data generation",
                key="anthropic_api_key"
            )

            if anthropic_key:
                st.success("✅ Anthropic key set")
        
        # =================================================================
        # GitHub Configuration
        # =================================================================
        with st.expander("🐙 GitHub Configuration", expanded=False):
            st.caption("For pushing models to your repo (optional)")
            
            github_token = st.text_input(
                "GitHub Token",
                type="password",
                help="Personal access token with repo write access",
                key="github_token"
            )
            
            github_repo = st.text_input(
                "Repository",
                placeholder="owner/repo-name",
                help="e.g., myorg/dbt-project",
                key="github_repo"
            )
            
            if github_token and github_repo:
                if '/' in github_repo:
                    st.success("✅ GitHub configured")
                else:
                    st.warning("Use format: owner/repo-name")
        
        # =================================================================
        # Snowflake Connection
        # =================================================================
        with st.expander("❄️ Snowflake Connection", expanded=not st.session_state.get('sf_connected')):
            st.caption("For loading synthetic data directly")
            
            sf_account = st.text_input(
                "Snowflake Account",
                placeholder="AIDCFUE-SK41274",
                help="Account identifier (e.g., AIDCFUE-SK41274). Don't include .snowflakecomputing.com",
                key="sf_account"
            )
            
            sf_user = st.text_input(
                "Username",
                help="Snowflake username",
                key="sf_user"
            )
            
            sf_password = st.text_input(
                "Password",
                type="password",
                help="Snowflake password",
                key="sf_password"
            )
            
            sf_warehouse = st.text_input(
                "Warehouse",
                value="COMPUTE_WH",
                help="Snowflake warehouse name",
                key="sf_warehouse"
            )
            
            if sf_account and sf_user and sf_password:
                if st.button("🔗 Test Snowflake Connection"):
                    with st.spinner("Connecting..."):
                        loader = SnowflakeDataLoader(
                            account=sf_account,
                            user=sf_user,
                            password=sf_password,
                            warehouse=sf_warehouse
                        )
                        # Test connection without requiring database to exist
                        if loader.connect(skip_database=True):
                            st.session_state['sf_connected'] = True
                            st.session_state['sf_loader'] = loader
                            st.success("✅ Connected to Snowflake!")
                        else:
                            st.error(f"❌ Connection failed: {loader.last_error}")
            
            if st.session_state.get('sf_connected'):
                st.success("✅ Snowflake ready")
        
        # =================================================================
        # About
        # =================================================================
        st.markdown("---")
        st.markdown("### ℹ️ About")
        st.caption("This tool helps estimate cost savings from dbt Fusion's State Aware Orchestration (SAO).")


# =============================================================================
# UI COMPONENTS
# =============================================================================

def render_header():
    # Official dbt Labs logo icon (orange) - extracted from https://www.getdbt.com brand assets
    dbt_logo_svg = '''<svg viewBox="0 0 90 90" xmlns="http://www.w3.org/2000/svg" style="height: 52px; vertical-align: middle; margin-right: 16px;">
        <path d="M85.41 4.32986C87.49 6.33986 88.8599 8.97987 89.1799 11.8699C89.1799 13.0699 88.86 13.8799 88.13 15.3899C87.4 16.9099 78.51 32.2999 75.85 36.5599C74.33 39.0599 73.53 42.0199 73.53 44.8999C73.53 47.7799 74.34 50.7499 75.85 53.2399C78.49 57.4999 87.4 72.9599 88.13 74.5099C88.86 76.0299 89.1799 76.7599 89.1799 77.9599C88.8599 80.8499 87.59 83.4899 85.48 85.4199C83.47 87.4999 80.8299 88.8699 78.0199 89.1199C76.8199 89.1199 76.01 88.7999 74.57 88.0699C73.13 87.3399 31.09 63.0399 31.09 63.0399C31.58 67.0499 33.34 70.8999 36.23 73.7099C36.79 74.2699 37.3599 74.7599 37.9899 75.2299C37.4999 75.4699 16.75 87.5899 15.21 88.3199C13.69 89.0499 12.96 89.3699 11.69 89.3699C8.79996 89.0499 6.15995 87.7799 4.22995 85.6699C2.14995 83.6599 0.779961 81.0199 0.459961 78.1299C0.529961 76.9299 0.879949 75.7299 1.50995 74.6799C2.23995 73.1599 11.1299 57.6699 13.7899 53.4099C15.3099 50.9099 16.11 48.0299 16.11 45.0699C16.11 42.1099 15.2999 39.2199 13.7899 36.7299C11.1499 32.3299 2.16995 16.8299 1.50995 15.3199C0.869949 14.2699 0.559961 13.0699 0.459961 11.8699C0.779961 8.97987 2.04996 6.33986 4.15996 4.32986C6.16996 2.24986 8.80995 0.949883 11.7 0.629883C12.9 0.699883 14.1 1.04987 15.22 1.67987C16.49 2.23987 58.44 26.9799 58.44 26.9799C57.95 22.1799 55.59 17.8399 51.72 14.8799C52.02 14.7199 72.8499 2.36987 74.3999 1.70987C75.4499 1.06987 76.65 0.759882 77.92 0.659882C80.73 0.979882 83.38 2.24986 85.38 4.35986L85.4299 4.33987L85.41 4.32986ZM46.31 52.4899L52.31 46.4699C53.14 45.6299 53.14 44.3799 52.31 43.5099L46.31 37.4899C45.48 36.6499 44.2299 36.6499 43.3599 37.4899L37.3599 43.5099C36.5299 44.3499 36.5299 45.5999 37.3599 46.4699L43.3599 52.4899C44.0999 53.2299 45.44 53.2299 46.31 52.4899Z" fill="#FF6D00"/>
    </svg>'''
    
    st.markdown(f'''
    <div style="display: flex; align-items: center; justify-content: center; margin-bottom: 0.5rem;">
        {dbt_logo_svg}
        <h1 class="main-header" style="margin: 0;">SAO Cost Savings Estimator</h1>
    </div>
    <p style="text-align: center; color: #888; margin-top: 0;">Estimate your savings from dbt Fusion's State Aware Orchestration</p>
    ''', unsafe_allow_html=True)


def get_default_pipeline_config():
    """Return default pipeline configuration for simulations."""
    return {
        'num_models': 200,
        'materialization': {'incremental': 30, 'table': 40, 'view': 30},
        'num_sources': 6,
        'runs_per_day': 24,
        'avg_row_count': 1000000,
        'avg_column_count': 30,
        'dag_depth': 5,
        'source_change_rate': 30,
        'warehouse_size': 'Medium',
        'current_monthly_cost': 5000,
        'generate_semantic_models': False,  # Disabled - Fusion semantic layer format differs significantly
        'test_types': ['not_null', 'unique'],
    }


def render_pipeline_config():
    """Render pipeline configuration inputs and return the config dict."""
    st.markdown("### 📊 Pipeline Configuration")
    st.markdown("Configure your dbt pipeline structure:")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Model Structure**")
        num_models = st.number_input("Total Number of Models", min_value=10, max_value=10000, value=200, step=10, key="config_num_models")
        num_sources = st.number_input("Number of Source Tables", min_value=1, max_value=100, value=6, step=1, key="config_num_sources")
        
        st.markdown("**Materialization Distribution**")
        use_exact_counts = st.checkbox("Specify exact model counts (instead of percentages)", key="config_use_exact_counts")
        
        if use_exact_counts:
            # Exact count mode
            st.caption("Specify exact number of each model type:")
            mat_col1, mat_col2, mat_col3 = st.columns(3)
            with mat_col1:
                view_count = st.number_input("Views", min_value=0, max_value=num_models, value=int(num_models * 0.3), step=1, key="config_view_count")
            with mat_col2:
                incremental_count = st.number_input("Incremental", min_value=0, max_value=num_models, value=int(num_models * 0.3), step=1, key="config_incr_count")
            with mat_col3:
                table_count = st.number_input("Tables", min_value=0, max_value=num_models, value=int(num_models * 0.4), step=1, key="config_table_count")
            
            total_specified = view_count + incremental_count + table_count
            if total_specified != num_models:
                st.warning(f"⚠️ Total: {total_specified} models specified (target: {num_models}). Will generate {total_specified} models.")
            else:
                st.success(f"✅ Total: {total_specified} models")
            
            # Convert counts to percentages for internal use
            if total_specified > 0:
                view_pct = int((view_count / total_specified) * 100)
                incremental_pct = int((incremental_count / total_specified) * 100)
                table_pct = 100 - view_pct - incremental_pct  # Ensure they add to 100
            else:
                view_pct, incremental_pct, table_pct = 30, 30, 40
            
            # Override num_models with actual total
            num_models = total_specified if total_specified > 0 else num_models
        else:
            # Percentage mode (original)
            st.caption("Specify percentage distribution (must total 100%):")
            mat_col1, mat_col2, mat_col3 = st.columns(3)
            with mat_col1:
                view_pct = st.number_input("Views %", min_value=0, max_value=100, value=30, step=5, key="config_view_pct")
            with mat_col2:
                incremental_pct = st.number_input("Incr %", min_value=0, max_value=100, value=30, step=5, key="config_incr_pct")
            with mat_col3:
                table_pct = st.number_input("Table %", min_value=0, max_value=100, value=40, step=5, key="config_table_pct")
            
            total_pct = view_pct + incremental_pct + table_pct
            if total_pct != 100:
                st.error(f"⚠️ Total: {total_pct}% (must be 100%)")
            else:
                st.success(f"✅ Total: {total_pct}%")
    
    with col2:
        st.markdown("**Execution Patterns**")
        runs_per_day = st.number_input("Runs Per Day", min_value=1, max_value=96, value=24, key="config_runs_per_day")
        
        st.markdown("**Data Characteristics**")
        row_count_options = ["1K", "10K", "100K", "1M", "10M", "100M", "1B"]
        avg_row_count = st.selectbox("Avg Row Count per Table", options=row_count_options, index=3, key="config_avg_rows")
        avg_column_count = st.number_input("Avg Column Count", min_value=5, max_value=200, value=30, step=5, key="config_avg_cols")
    
    with st.expander("🔧 Advanced Configuration"):
        col3, col4 = st.columns(2)
        with col3:
            dag_depth = st.number_input("Average DAG Depth", min_value=2, max_value=15, value=5, step=1, key="config_dag_depth")
            source_change_rate = st.number_input("Source Change Rate (%)", min_value=5, max_value=100, value=30, step=5, key="config_change_rate")
        with col4:
            warehouse_size = st.selectbox("Snowflake Warehouse Size", options=["X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large"], index=2, key="config_warehouse")
            current_monthly_cost = st.number_input("Current Monthly Compute Cost ($)", min_value=0, max_value=1000000, value=5000, key="config_cost")
    
    with st.expander("🧪 dbt Project Features"):
        col5, col6 = st.columns(2)
        with col5:
            st.markdown("**Semantic Layer**")
            generate_semantic_models = st.checkbox("Generate Semantic Models", value=False, key="config_semantic_models",
                                                   help="Create semantic_models.yml (Note: Fusion requires manual configuration for semantic layer)")
        with col6:
            st.markdown("**Test Types**")
            test_types = st.multiselect(
                "Include Test Types",
                options=["not_null", "unique"],
                default=["not_null", "unique"],
                key="config_test_types",
                help="Tests are applied to actual model columns (primary keys, foreign keys, timestamps) at each layer."
            )
    
    row_count_map = {"1K": 1000, "10K": 10000, "100K": 100000, "1M": 1000000, "10M": 10000000, "100M": 100000000, "1B": 1000000000}
    
    return {
        'num_models': num_models,
        'materialization': {'incremental': incremental_pct, 'table': table_pct, 'view': view_pct},
        'num_sources': num_sources,
        'runs_per_day': runs_per_day,
        'avg_row_count': row_count_map[avg_row_count],
        'avg_column_count': avg_column_count,
        'dag_depth': dag_depth,
        'source_change_rate': source_change_rate,
        'warehouse_size': warehouse_size,
        'current_monthly_cost': current_monthly_cost,
        'generate_semantic_models': generate_semantic_models,
        'test_types': test_types,
    }


def run_simulation(pipeline_config):
    calculator = SAOCostCalculator()
    
    with st.spinner("Running SAO simulation..."):
        simulation_results = calculator.simulate_pipeline_runs(
            pipeline_config=pipeline_config,
            num_simulation_runs=pipeline_config['runs_per_day']
        )
        
        savings_analysis = calculator.calculate_savings(
            simulation_results=simulation_results,
            warehouse_size=pipeline_config['warehouse_size'],
            current_monthly_cost=pipeline_config['current_monthly_cost']
        )
    
    return simulation_results, savings_analysis


def render_results(simulation_results, savings_analysis):
    st.markdown("### 📈 SAO Impact Analysis")
    
    st.markdown(f"""
    <div class="savings-highlight">
        <p style="color: #a0a0c0; font-size: 0.9rem; text-transform: uppercase;">Estimated Monthly Savings with SAO</p>
        <p class="savings-amount">${savings_analysis.get('monthly_savings', 0):,.0f}</p>
        <p style="color: #a0a0c0;">{savings_analysis.get('savings_percentage', 0):.1f}% reduction in compute costs</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Models Skipped (Avg)", f"{simulation_results.get('avg_models_skipped', 0):.0f}")
    col2.metric("Execution Time Saved", f"{simulation_results.get('time_saved_percentage', 0):.0f}%")
    col3.metric("Annual Savings", f"${savings_analysis.get('annual_savings', 0):,.0f}")
    col4.metric("ROI Estimate", f"{savings_analysis.get('roi_estimate', 0):.1f}x")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Cost Comparison")
        fig1 = create_cost_comparison_chart(savings_analysis)
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        st.markdown("#### Model Execution Analysis")
        fig2 = create_sao_impact_chart(simulation_results)
        st.plotly_chart(fig2, use_container_width=True)
    
    with st.expander("📊 Detailed Run Analysis"):
        if 'run_details' in simulation_results:
            df = pd.DataFrame(simulation_results['run_details'])
            st.dataframe(df, use_container_width=True)


# =============================================================================
# DEPLOY TAB
# =============================================================================

def render_deploy_tab():
    """Render the deploy to dbt Cloud tab."""
    st.markdown("### 🚀 Deploy to dbt Cloud Sandbox")
    
    if not st.session_state.get('dbt_connected'):
        st.warning("⚠️ Please connect to dbt Cloud using the sidebar on the left.")
        return
    
    # ==========================================================================
    # Customer Context - describe the business for realistic data generation
    # ==========================================================================
    st.markdown("#### 🏢 Describe the Customer's Business")
    st.text_area(
        "Customer Description",
        placeholder="e.g., E-commerce company selling electronics, with 2M customers, 50K products, processes 10K orders daily. They track inventory, customer behavior, marketing campaigns, and supply chain logistics.",
        help="This description helps Claude generate realistic synthetic data matching the customer's domain",
        height=120,
        key="customer_description"
    )
    
    customer_description = st.session_state.get('customer_description', '')
    pipeline_config = st.session_state.get('pipeline_config', get_default_pipeline_config())
    
    # Check for Anthropic and GitHub configuration
    has_anthropic = bool(st.session_state.get('anthropic_api_key'))
    has_github = bool(st.session_state.get('github_token') and st.session_state.get('github_repo'))
    
    st.markdown("---")
    
    # Configuration status
    st.markdown("#### 📋 Configuration Status")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("dbt Cloud", "✅ Connected")
    with col2:
        st.metric("Anthropic", "✅ Ready" if has_anthropic else "⚠️ Required")
    with col3:
        st.metric("GitHub", "✅ Ready" if has_github else "⚠️ Optional")
    with col4:
        st.metric("Customer Context", "✅ Set" if customer_description else "⚠️ Missing")
    
    st.markdown("---")
    
    # ==========================================================================
    # STEP 1: Generate Schema & Synthetic Data (Combined)
    # ==========================================================================
    st.markdown("#### Step 1: Generate Schema & Synthetic Data")
    st.info("🚀 **Fast 2-step process:** Claude designs the schema → Claude generates the data")
        
    # Data generation settings
    col1, col2, col3 = st.columns(3)
    with col1:
        raw_database = st.text_input("Target Database", value="RAW_DATA", help="Database for source tables (will be created if it doesn't exist)", key="target_db")
    with col2:
        raw_schema = st.text_input("Target Schema", value="PUBLIC", help="Schema for source tables (will be created if it doesn't exist)", key="target_schema")
    with col3:
        row_count_options = {
            "100": 100, "500": 500, "1,000": 1000, "5,000": 5000, "10,000": 10000,
            "50,000": 50000, "100,000": 100000, "500,000": 500000, "1,000,000": 1000000
        }
        selected_row_count = st.selectbox(
            "Rows per Table", 
            options=list(row_count_options.keys()), 
            index=2,  # Default to 1,000
            help="Number of rows per table (Claude creates schema and generates data)"
        )
        rows_per_table = row_count_options[selected_row_count]
    
    # Optional: Sample DDL input
    with st.expander("📋 Optional: Provide Sample DDL", expanded=False):
        st.caption("If you have existing DDL from the customer, paste it here. Claude will use it to create matching tables with additional supporting tables.")
        sample_ddl = st.text_area(
            "Sample DDL Statements",
            placeholder="""-- Paste customer DDL here (optional)
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2)
);""",
            height=150,
            key="sample_ddl"
        )
    
    # Show status
    if st.session_state.get('anthropic_api_key'):
        st.success("✅ Claude API ready")
    else:
        st.error("❌ Anthropic API key required - configure in sidebar")

    # Generate button
    can_generate = bool(st.session_state.get('anthropic_api_key'))

    if st.button("🎨 Generate Schema & Data", type="primary", use_container_width=True, disabled=not can_generate):
        if not can_generate:
            st.error("❌ Please configure Anthropic API key in the sidebar")
        else:
            try:
                generator = SyntheticDataGenerator(
                    anthropic_api_key=st.session_state.get('anthropic_api_key')
                )

                # Get sample DDL if provided
                ddl_input = st.session_state.get('sample_ddl', '').strip() or None

                # Use Claude for schema and data generation
                result = generator.generate_schema_and_data(
                    customer_description=customer_description,
                    num_sources=pipeline_config['num_sources'],
                    rows_per_table=rows_per_table,
                    sample_ddl=ddl_input,
                    num_columns=pipeline_config.get('avg_column_count', 30)
                )

                st.session_state['generated_schema'] = result['schema']
                st.session_state['generated_dataframes'] = result['dataframes']
                total_rows = sum(len(df) for df in result['dataframes'].values())
                st.success(f"✅ Generated {total_rows:,} total rows across {len(result['dataframes'])} tables!")

            except ValueError as e:
                st.error(f"❌ Data generation error: {e}")
            except Exception as e:
                error_type = type(e).__name__
                st.error(f"❌ Claude API error ({error_type}): {e}")

        # Generate DDL after data generation
        if st.session_state.get('generated_schema'):
            ddl = _LegacyDDLGenerator.generate_create_tables(st.session_state['generated_schema'], raw_database, raw_schema)
            st.session_state['generated_ddl'] = ddl
        
    # Show generated schema
    if st.session_state.get('generated_schema'):
        schema = st.session_state['generated_schema']
        
        with st.expander("📋 View Generated Schema", expanded=False):
            for source in schema.get("sources", []):
                st.markdown(f"**{source['name']}** - {source.get('description', '')}")
                cols_df = pd.DataFrame(source.get("columns", []))
                if not cols_df.empty:
                    display_cols = [c for c in ['name', 'type', 'description'] if c in cols_df.columns]
                    if display_cols:
                        st.dataframe(cols_df[display_cols], use_container_width=True, hide_index=True)
    
    # Show generated data preview
    if st.session_state.get('generated_dataframes'):
        dataframes = st.session_state['generated_dataframes']
        
        with st.expander("📊 Preview Generated Data", expanded=True):
            table_to_preview = st.selectbox(
                "Select table to preview:",
                options=list(dataframes.keys())
            )
            if table_to_preview:
                df = dataframes[table_to_preview]
                st.write(f"**{table_to_preview}** - {len(df):,} rows, {len(df.columns)} columns")
                st.dataframe(df.head(20), use_container_width=True)
        
        # Show DDL
        if st.session_state.get('generated_ddl'):
            with st.expander("📝 View DDL Statements", expanded=False):
                st.code(st.session_state['generated_ddl'], language="sql")
            
            st.download_button(
                label="📥 Download DDL",
                data=st.session_state['generated_ddl'],
                file_name="create_tables.sql",
                mime="text/sql"
            )
        
        # Option to load directly to Snowflake
        st.markdown("---")
        st.markdown("##### 🚀 Load Data to Snowflake")
        
        if st.session_state.get('sf_connected'):
            st.success("✅ Snowflake connection ready")
            
            if st.button("📤 Load All Data to Snowflake", type="primary"):
                loader = st.session_state.get('sf_loader')
                
                # Update loader to use the target database/schema from Step 1
                target_database = st.session_state.get('target_db', 'RAW_DATA')
                target_schema = st.session_state.get('target_schema', 'PUBLIC')
                loader.database = target_database
                loader.schema = target_schema
                
                # Step 1: Setup database and schema (creates if not exists)
                with st.spinner(f"Setting up database '{target_database}' and schema '{target_schema}'..."):
                    setup_result = loader.setup_database_and_schema()
                    if not setup_result.get('success', False):
                        st.error(f"❌ Failed to setup database: {setup_result.get('error')}")
                        st.stop()
                    st.success(f"✅ {setup_result.get('message')}")
                
                # Step 2: Create the tables using DDL
                with st.spinner("Creating tables in Snowflake..."):
                    ddl_result = loader.execute_sql(st.session_state['generated_ddl'])
                    if not ddl_result.get('success', False):
                        st.error(f"❌ Failed to create tables: {ddl_result.get('error')}")
                        st.stop()
                    st.success("✅ Tables created successfully")
                
                # Step 3: Load the data
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                results = {"success": [], "failed": []}
                tables = list(dataframes.keys())
                
                for i, (table_name, df) in enumerate(dataframes.items()):
                    status_text.text(f"Loading {table_name}... ({i+1}/{len(tables)})")
                    result = loader.load_dataframe(df, table_name)
                    
                    if result.get('success'):
                        results['success'].append({
                            'table': table_name,
                            'rows': result.get('rows', len(df))
                        })
                    else:
                        results['failed'].append({
                            'table': table_name,
                            'error': result.get('error', 'Unknown error')
                        })
                    
                    progress_bar.progress((i + 1) / len(tables))
                
                status_text.empty()
                progress_bar.empty()
                
                if results['success']:
                    total_loaded = sum(r['rows'] for r in results['success'])
                    st.success(f"✅ Successfully loaded {total_loaded:,} rows into {len(results['success'])} tables!")
                    with st.expander("View loaded tables"):
                        for r in results['success']:
                            st.write(f"✅ **{r['table']}**: {r['rows']:,} rows")
                
                if results['failed']:
                    st.error(f"❌ Failed to load {len(results['failed'])} tables")
                    with st.expander("View errors"):
                        for r in results['failed']:
                            st.write(f"❌ **{r['table']}**: {r['error']}")
        else:
            st.warning("⚠️ Configure Snowflake connection in the sidebar to load data directly")
            st.info("Alternatively, download the DDL and data files to run manually")
            
            # Generate CSV downloads
            for table_name, df in dataframes.items():
                csv_data = df.to_csv(index=False)
                st.download_button(
                    label=f"📥 Download {table_name}.csv",
                    data=csv_data,
                    file_name=f"{table_name}.csv",
                    mime="text/csv",
                    key=f"download_{table_name}"
                )
    
    # ==========================================================================
    # STEP 3: Generate dbt Models
    # ==========================================================================
    if st.session_state.get('generated_schema'):
        st.markdown("---")
        st.markdown("#### Step 3: Generate dbt Models")
        
        st.info(f"Generating {pipeline_config['num_models']} dbt models based on your schema and configuration.")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Incremental", f"{pipeline_config['materialization']['incremental']}%")
        with col2:
            st.metric("Table", f"{pipeline_config['materialization']['table']}%")
        with col3:
            st.metric("View", f"{pipeline_config['materialization']['view']}%")
        
        # Check if schema exists
        schema = st.session_state.get('generated_schema')
        if not schema:
            st.warning("⚠️ No generated schema found. Generate data first (Step 1) to ensure dbt sources match actual tables.")
        
        if st.button("📦 Generate dbt Project"):
            with st.spinner("Generating dbt models..."):
                generator = DBTProjectGenerator()
                # Pass schema and target database/schema for source references
                target_db = st.session_state.get('target_db', 'RAW_DATA')
                target_schema = st.session_state.get('target_schema', 'PUBLIC')
                
                if not schema:
                    st.error("❌ Cannot generate dbt project without schema. Please generate data first (Step 1).")
                else:
                    models = generator.generate_models(
                        pipeline_config, 
                        schema,
                        target_database=target_db,
                        target_schema=target_schema
                    )
                    st.session_state['generated_models'] = models
                    st.success(f"✅ Generated {len(models)} files!")
    
    # Show generated models
    if st.session_state.get('generated_models'):
        models = st.session_state['generated_models']
        
        with st.expander("📁 View Generated dbt Models", expanded=False):
            model_file = st.selectbox(
                "Select a file to view:",
                options=list(models.keys())
            )
            if model_file:
                lang = "sql" if model_file.endswith(".sql") else "yaml"
                st.code(models[model_file], language=lang)
    
    # ==========================================================================
    # STEP 4: Push to GitHub
    # ==========================================================================
    if st.session_state.get('generated_models') and has_github:
        st.markdown("---")
        st.markdown("#### Step 4: Push Models to GitHub")
        
        github_repo = st.session_state.get('github_repo', '')
        if '/' in github_repo:
            owner, repo = github_repo.split('/', 1)
            
            # Verify repo access first
            github = GitHubClient(st.session_state['github_token'])
            repo_info = github.get_repo_info(owner, repo)
            
            if repo_info:
                default_branch = repo_info.get('default_branch', 'main')
                st.success(f"✅ Connected to **{owner}/{repo}** (default branch: `{default_branch}`)")
                
                branch = st.text_input("Branch Name", value=default_branch, help="Branch to push to")
                commit_message = st.text_input("Commit Message", value="Add SAO comparison dbt models")
                
                # Option to clean old files
                clean_repo = st.checkbox(
                    "🧹 Clean old dbt files first (recommended)", 
                    value=True,
                    help="Delete existing models/, dbt_project.yml, etc. before pushing new files. This ensures no old/conflicting files remain."
                )
                
                if st.button("🚀 Push to GitHub"):
                    if clean_repo:
                        with st.spinner(f"Cleaning old files and pushing {len(models)} new files to GitHub..."):
                            results = github.replace_repo_contents(
                                owner=owner,
                                repo=repo,
                                files=st.session_state['generated_models'],
                                message=commit_message,
                                branch=branch
                            )
                            
                            if results['deleted']:
                                st.info(f"🧹 Deleted {len(results['deleted'])} old files")
                            
                            if results['created']:
                                st.success(f"✅ Successfully pushed {len(results['created'])} files!")
                                with st.expander("View pushed files"):
                                    for f in results['created']:
                                        st.write(f"✅ {f}")
                            
                            if results['failed']:
                                st.error(f"❌ Failed to push {len(results['failed'])} files")
                                with st.expander("View errors"):
                                    for f in results['failed']:
                                        st.write(f"❌ {f['path']}: {f['error']}")
                    else:
                        with st.spinner(f"Pushing {len(models)} files to GitHub..."):
                            results = github.push_multiple_files(
                                owner=owner,
                                repo=repo,
                                files=st.session_state['generated_models'],
                                message=commit_message,
                                branch=branch
                            )
                            
                            if results['success']:
                                st.success(f"✅ Successfully pushed {len(results['success'])} files!")
                                with st.expander("View pushed files"):
                                    for f in results['success']:
                                        st.write(f"✅ {f}")
                            
                            if results['failed']:
                                st.error(f"❌ Failed to push {len(results['failed'])} files")
                                with st.expander("View errors"):
                                    for f in results['failed']:
                                        st.write(f"❌ {f['path']}: {f['error']}")
            else:
                st.error(f"❌ Cannot access repository **{owner}/{repo}**")
                st.warning(f"""
                **Possible causes:**
                1. Repository doesn't exist - [Create it on GitHub](https://github.com/new)
                2. Token doesn't have access - Ensure your token has `repo` scope
                3. Repository is private and token lacks permissions
                
                **Error:** {github.last_error}
                """)
    elif st.session_state.get('generated_models'):
        st.markdown("---")
        st.markdown("#### Step 4: Download & Push to GitHub")
        st.info("Configure GitHub in the sidebar to push directly, or download and push manually:")
        
        # Download models
        models = st.session_state['generated_models']
        all_files = "\n\n".join([f"=== {path} ===\n{content}" for path, content in models.items()])
        
        st.download_button(
            label="📥 Download dbt Project Files",
            data=all_files,
            file_name="sao_sandbox_project.txt",
            mime="text/plain"
        )
    
    # ==========================================================================
    # STEP 5: Create Jobs in dbt Cloud
    # ==========================================================================
    if st.session_state.get('generated_models'):
        st.markdown("---")
        st.markdown("#### Step 5: Create Comparison Jobs in dbt Cloud")
        
        project_id = st.session_state.get('selected_project')
        environment_id = st.session_state.get('selected_environment')
        
        if project_id and environment_id:
            st.success(f"✅ Project and Environment selected")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**dbt Core Baseline Job**")
                st.caption("Runs dbt build (no SAO)")
                core_job_name = st.text_input("Job Name", value="SAO Comparison - Core Baseline", key="core_job")
            
            with col2:
                st.markdown("**dbt Fusion Job (with SAO)**")
                st.caption("Uses State Aware Orchestration")
                fusion_job_name = st.text_input("Job Name", value="SAO Comparison - Fusion SAO", key="fusion_job")
            
            if st.button("🔧 Create Jobs in dbt Cloud"):
                api = st.session_state['dbt_api']
                
                with st.spinner("Creating jobs..."):
                    # Core job - uses dbt Core (latest stable)
                    # Valid versions: 'fallback', 'extended', 'compatible', 'latest', 'latest-fusion'
                    st.info("Creating Core baseline job...")
                    core_result = api.create_job(
                        project_id=project_id,
                        environment_id=environment_id,
                        name=core_job_name,
                        execute_steps=["dbt build"],
                        dbt_version="latest"  # dbt Core latest version
                    )
                    
                    # Handle both old (single value) and new (tuple) return formats
                    if isinstance(core_result, tuple):
                        core_job, core_error = core_result
                    else:
                        core_job, core_error = core_result, None
                    
                    # Fusion job - uses Fusion engine with SAO
                    st.info("Creating Fusion SAO job...")
                    fusion_result = api.create_job(
                        project_id=project_id,
                        environment_id=environment_id,
                        name=fusion_job_name,
                        execute_steps=["dbt build"],
                        dbt_version="latest-fusion"  # Fusion engine (supports SAO)
                    )
                    
                    # Handle both old (single value) and new (tuple) return formats
                    if isinstance(fusion_result, tuple):
                        fusion_job, fusion_error = fusion_result
                    else:
                        fusion_job, fusion_error = fusion_result, None
                    
                    if core_job and fusion_job:
                        st.session_state['core_job_id'] = core_job.get('id')
                        st.session_state['fusion_job_id'] = fusion_job.get('id')
                        st.success("✅ Both jobs created successfully!")
                        st.json({"Core Job ID": core_job.get('id'), "Fusion Job ID": fusion_job.get('id')})
                        
                        # Show instructions for enabling SAO (can't be set via API)
                        st.warning("""
⚠️ **Action Required for Fusion Job:**

SAO and Efficient Testing must be enabled manually in dbt Cloud:

1. Go to your **Fusion SAO job** in dbt Cloud
2. Click **Edit** on the job
3. Under "Execution Settings", check **"Enable Fusion cost optimization features"**
4. Expand "More options" and enable:
   - ✅ **State-aware orchestration**
   - ✅ **Efficient testing**
5. Save the job

This ensures the Fusion job uses SAO for the comparison.
""")
                    else:
                        st.error("❌ Failed to create one or both jobs.")
                        
                        if not core_job:
                            st.error(f"**Core Job Error:** {core_error or 'Unknown error'}")
                        else:
                            st.success(f"✅ Core job created (ID: {core_job.get('id')})")
                            st.session_state['core_job_id'] = core_job.get('id')
                        
                        if not fusion_job:
                            st.error(f"**Fusion Job Error:** {fusion_error or 'Unknown error'}")
                        else:
                            st.success(f"✅ Fusion job created (ID: {fusion_job.get('id')})")
                            st.session_state['fusion_job_id'] = fusion_job.get('id')
                        
                        st.info("""
💡 **Troubleshooting Tips:**
1. Make sure you've selected a **deployment** environment (not development)
2. Ensure your dbt Cloud API token has permissions to create jobs
3. Check that the project has a valid connection configured
4. Try creating jobs manually in dbt Cloud to see if there are permission issues
""")
            
            # Step 6: Run Jobs
            if st.session_state.get('core_job_id') and st.session_state.get('fusion_job_id'):
                st.markdown("---")
                st.markdown("#### Step 6: Run Comparison Jobs")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("▶️ Run Core Baseline Job"):
                        api = st.session_state['dbt_api']
                        run = api.trigger_run(st.session_state['core_job_id'], "SAO Estimator - Core baseline")
                        if run:
                            st.success(f"✅ Run started! Run ID: {run.get('id')}")
                            st.info("View progress in dbt Cloud")
                        else:
                            st.error("❌ Failed to trigger run")
                
                with col2:
                    if st.button("▶️ Run Fusion SAO Job"):
                        api = st.session_state['dbt_api']
                        run = api.trigger_run(st.session_state['fusion_job_id'], "SAO Estimator - Fusion with SAO")
                        if run:
                            st.success(f"✅ Run started! Run ID: {run.get('id')}")
                            st.info("View progress in dbt Cloud")
                        else:
                            st.error("❌ Failed to trigger run")
        else:
            st.warning("Please select a Project and Environment in the sidebar.")


# =============================================================================
# SAO LIVE COMPARISON TAB
# =============================================================================

def render_sao_comparison_tab():
    """Render the SAO Live Comparison tab for real Core vs Fusion job testing."""
    st.markdown("### 🔬 SAO Comparison & ROI Calculator")
    st.markdown("""
    Compare dbt Core vs dbt Fusion with State-Aware Orchestration (SAO) to measure compute savings.
    Choose between **Simulation Mode** (instant projections) or **Live Comparison** (real job runs).
    """)
    
    # ==========================================================================
    # MODE SELECTION
    # ==========================================================================
    comparison_mode = st.radio(
        "Select Comparison Mode",
        options=["🎯 Simulation Mode (Instant)", "🔥 Live Comparison (Real Jobs)"],
        horizontal=True,
        help="Simulation Mode projects savings based on your DAG structure. Live Comparison actually runs dbt jobs."
    )
    
    st.markdown("---")
    
    # ==========================================================================
    # SIMULATION MODE
    # ==========================================================================
    if comparison_mode == "🎯 Simulation Mode (Instant)":
        render_simulation_mode()
    else:
        render_live_comparison_mode()


def render_simulation_mode():
    """Render the simulation mode for instant ROI projections."""
    st.markdown("### 🎯 Simulation Mode")
    st.markdown("""
    Project SAO savings based on your DAG structure and configuration without running actual jobs.
    This gives you instant ROI estimates based on model dependency analysis.
    """)
    
    # Check for pipeline config
    pipeline_config = st.session_state.get('pipeline_config', {})
    schema = st.session_state.get('generated_schema', {})
    
    has_config = bool(pipeline_config)
    has_schema = bool(schema and schema.get('sources'))
    
    # Use default config if not set
    if not has_config:
        pipeline_config = {
            'num_models': 100,
            'num_sources': 5,
            'materialization': {'incremental': 30, 'table': 40, 'view': 30},
            'dag_depth': 4,
        }
    
    if not has_schema:
        # Create minimal schema for simulation
        num_sources = pipeline_config.get('num_sources', 5)
        schema = {'sources': [{'name': f'source_{i}'} for i in range(num_sources)]}
    
    # ==========================================================================
    # SIMULATION CONFIGURATION
    # ==========================================================================
    st.markdown("#### ⚙️ Simulation Parameters")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**DAG Configuration**")
        num_models = st.number_input(
            "Total Models",
            min_value=10, max_value=10000, value=pipeline_config.get('num_models', 100),
            help="Total number of dbt models in your project"
        )
        num_sources = st.number_input(
            "Number of Sources",
            min_value=1, max_value=100, value=len(schema.get('sources', [])) or 5,
            help="Number of source tables"
        )
        dag_depth = st.slider(
            "DAG Depth",
            min_value=2, max_value=10, value=pipeline_config.get('dag_depth', 4),
            help="How many layers deep your DAG goes (staging → intermediate → marts → reports)"
        )
    
    with col2:
        st.markdown("**Run Configuration**")
        runs_per_day = st.number_input(
            "Runs per Day",
            min_value=1, max_value=100, value=8,
            key="sim_runs_per_day",
            help="How many times your dbt jobs run per day"
        )
        source_change_rate = st.slider(
            "Source Change Rate (%)",
            min_value=0, max_value=100, value=30,
            help="Average percentage of sources with new data per run. Lower = more SAO savings."
        )
    
    # Warehouse and cost settings
    st.markdown("#### 💰 Cost Configuration")
    col3, col4, col5 = st.columns(3)
    
    with col3:
        warehouse_size = st.selectbox(
            "Warehouse Size",
            options=["X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"],
            index=2,
            key="sim_warehouse"
        )
    with col4:
        credit_cost = st.number_input(
            "Cost per Credit ($)",
            min_value=1.0, max_value=10.0, value=3.0, step=0.25,
            key="sim_credit_cost",
            help="Your Snowflake credit cost (typically $2-4)"
        )
    with col5:
        # Show credits per hour for selected warehouse
        warehouse_credits = {"X-Small": 1, "Small": 2, "Medium": 4, "Large": 8, "X-Large": 16, "2X-Large": 32, "3X-Large": 64, "4X-Large": 128}
        credits_per_hour = warehouse_credits.get(warehouse_size, 4)
        st.metric("Credits/Hour", f"{credits_per_hour}", help="Credits consumed per hour for this warehouse size")
    
    # Materialization mix
    st.markdown("#### 📊 Materialization Mix")
    col6, col7, col8 = st.columns(3)
    
    default_mats = pipeline_config.get('materialization', {'incremental': 30, 'table': 40, 'view': 30})
    with col6:
        pct_incremental = st.slider("% Incremental", 0, 100, default_mats.get('incremental', 30), key="sim_inc")
    with col7:
        pct_table = st.slider("% Table", 0, 100, default_mats.get('table', 40), key="sim_table")
    with col8:
        pct_view = 100 - pct_incremental - pct_table
        st.metric("% View", f"{pct_view}%")
    
    st.markdown("---")
    
    # ==========================================================================
    # RUN SIMULATION
    # ==========================================================================
    if st.button("🚀 Run Simulation", type="primary", use_container_width=True):
        with st.spinner("Simulating SAO savings..."):
            # Build simulation config
            sim_config = {
                'num_models': num_models,
                'num_sources': num_sources,
                'dag_depth': dag_depth,
                'materialization': {'incremental': pct_incremental, 'table': pct_table, 'view': pct_view}
            }
            sim_schema = {'sources': [{'name': f'source_{i}'} for i in range(num_sources)]}
            
            # Create simulation engine
            sim_engine = SAOSimulationEngine(schema=sim_schema, pipeline_config=sim_config)
            
            # Run ROI projection
            roi_projection = sim_engine.project_roi(
                runs_per_day=runs_per_day,
                source_change_rate=source_change_rate,
                warehouse_size=warehouse_size,
                credit_cost=credit_cost
            )
            
            st.session_state['simulation_roi'] = roi_projection
            st.session_state['simulation_config'] = sim_config
        
        st.rerun()
    
    # ==========================================================================
    # DISPLAY SIMULATION RESULTS
    # ==========================================================================
    if st.session_state.get('simulation_roi'):
        roi = st.session_state['simulation_roi']
        dag = roi.get('dag_analysis', {})
        
        st.markdown("---")
        st.markdown("### 📊 Simulation Results")
        
        # DAG Analysis
        st.markdown("#### 🌳 DAG Analysis")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Models", dag.get('total_models', 0))
        with col2:
            st.metric("Total Sources", dag.get('total_sources', 0))
        with col3:
            st.metric("DAG Depth", dag.get('dag_depth', 0))
        with col4:
            st.metric("Dependency Chains", dag.get('dependency_chain_length', 0))
        
        # Per-run comparison
        st.markdown("#### ⚡ Per-Run Comparison")
        per_run = roi.get('per_run_average', {})
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric(
                "Core Models Run",
                per_run.get('core_models_run', 0),
                help="dbt Core runs ALL models every time"
            )
        with col2:
            st.metric(
                "Fusion Models Run",
                per_run.get('fusion_models_run', 0),
                help="SAO only runs affected models"
            )
        with col3:
            models_skipped = per_run.get('models_skipped', 0)
            skip_pct = round(models_skipped / per_run.get('core_models_run', 1) * 100, 1)
            st.metric(
                "Models Skipped",
                f"{models_skipped}",
                delta=f"{skip_pct}% saved",
                delta_color="normal"
            )
        with col4:
            st.metric(
                "Time Saved per Run",
                f"{per_run.get('time_saved_pct', 0)}%",
                delta=f"+{per_run.get('time_saved_pct', 0)}%"
            )
        
        # ROI Highlights
        st.markdown("#### 💰 ROI Projection")
        
        annual_savings = roi.get('annual', {}).get('cost_saved', 0)
        monthly_savings = roi.get('monthly', {}).get('cost_saved', 0)
        daily_savings = roi.get('daily', {}).get('cost_saved', 0)
        
        # Big highlight card
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #065f46 0%, #047857 100%); 
                    border-radius: 16px; padding: 32px; margin: 16px 0;
                    box-shadow: 0 4px 20px rgba(16, 185, 129, 0.3);">
            <p style="color: #d1fae5; font-size: 0.9rem; text-transform: uppercase; margin-bottom: 8px; letter-spacing: 1px;">
                Projected Annual Savings with SAO
            </p>
            <p style="font-size: 3.5rem; font-weight: bold; color: #ffffff; margin: 0; text-shadow: 0 2px 4px rgba(0,0,0,0.2);">
                ${annual_savings:,.0f}
            </p>
            <p style="color: #a7f3d0; margin-top: 12px; font-size: 1.1rem;">
                ${monthly_savings:,.0f}/month · ${daily_savings:,.0f}/day
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Detailed breakdown
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Daily Savings**")
            daily = roi.get('daily', {})
            st.write(f"⏱️ Time saved: {daily.get('time_saved_hours', 0):.1f} hours")
            st.write(f"💎 Credits saved: {daily.get('credits_saved', 0):.1f}")
            st.write(f"💵 Cost saved: **${daily.get('cost_saved', 0):.2f}**")
        
        with col2:
            st.markdown("**Monthly Savings**")
            monthly = roi.get('monthly', {})
            st.write(f"⏱️ Time saved: {monthly.get('time_saved_hours', 0):.0f} hours")
            st.write(f"💎 Credits saved: {monthly.get('credits_saved', 0):.0f}")
            st.write(f"💵 Cost saved: **${monthly.get('cost_saved', 0):,.0f}**")
        
        with col3:
            st.markdown("**Annual Savings**")
            annual = roi.get('annual', {})
            st.write(f"⏱️ Time saved: {annual.get('time_saved_hours', 0):,.0f} hours")
            st.write(f"💎 Credits saved: {annual.get('credits_saved', 0):,.0f}")
            st.write(f"💵 Cost saved: **${annual.get('cost_saved', 0):,.0f}**")
        
        # Visualization
        st.markdown("---")
        st.markdown("#### 📈 Visualizations")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Models comparison bar chart
            core_models = per_run.get('core_models_run', 100)
            fusion_models = per_run.get('fusion_models_run', 50)
            
            fig_models = go.Figure()
            fig_models.add_trace(go.Bar(
                x=['dbt Core', 'dbt Fusion (SAO)'],
                y=[core_models, fusion_models],
                marker_color=['#ef4444', '#10b981'],
                text=[f'{core_models}', f'{fusion_models}'],
                textposition='auto'
            ))
            fig_models.update_layout(
                title="Models Run per Job",
                yaxis_title="Number of Models",
                template='plotly_dark',
                height=350,
                showlegend=False
            )
            st.plotly_chart(fig_models, use_container_width=True)
        
        with col2:
            # Cost savings pie chart
            fusion_cost = roi.get('annual', {}).get('cost_saved', 0)
            # Estimate Core cost based on savings percentage
            time_saved_pct = per_run.get('time_saved_pct', 50)
            core_cost = fusion_cost / (time_saved_pct / 100) if time_saved_pct > 0 else fusion_cost * 2
            actual_fusion_cost = core_cost - fusion_cost
            
            fig_cost = go.Figure(data=[go.Pie(
                labels=['Fusion Runtime Cost', 'SAO Savings'],
                values=[actual_fusion_cost, fusion_cost],
                hole=0.5,
                marker_colors=['#6366f1', '#10b981'],
                textinfo='label+percent'
            )])
            fig_cost.update_layout(
                title="Annual Cost Breakdown",
                template='plotly_dark',
                height=350
            )
            st.plotly_chart(fig_cost, use_container_width=True)
        
        # Assumptions expander
        with st.expander("📋 Simulation Assumptions"):
            params = roi.get('simulation_params', {})
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Runs per day:** {params.get('runs_per_day', 0)}")
                st.write(f"**Source change rate:** {params.get('source_change_rate', 0)}%")
                st.write(f"**Warehouse size:** {params.get('warehouse_size', 'Medium')}")
            with col2:
                st.write(f"**Credits per hour:** {params.get('credits_per_hour', 4)}")
                st.write(f"**Credit cost:** ${params.get('credit_cost', 3.0)}")
                
            st.markdown("---")
            st.markdown("**DAG Layer Distribution:**")
            layers = dag.get('layers', {})
            for layer_name, layer_info in layers.items():
                if isinstance(layer_info, dict):
                    st.write(f"• {layer_name}: {layer_info.get('count', 0)} models")


def render_live_comparison_mode():
    """Render the live comparison mode for actual job runs."""
    st.markdown("### 🔥 Live Comparison Mode")
    st.markdown("""
    Run **real dbt jobs** to compare dbt Core vs dbt Fusion with SAO.
    This injects new data into source tables, runs both jobs, and measures actual compute savings.
    """)
    
    # Check prerequisites
    has_dbt = st.session_state.get('dbt_connected', False)
    has_snowflake = st.session_state.get('sf_connected', False)
    has_jobs = st.session_state.get('core_job_id') and st.session_state.get('fusion_job_id')
    has_schema = st.session_state.get('generated_schema') is not None
    
    # Status indicators
    st.markdown("#### 📋 Prerequisites")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("dbt Cloud", "✅ Connected" if has_dbt else "❌ Required")
    with col2:
        st.metric("Snowflake", "✅ Connected" if has_snowflake else "❌ Required")
    with col3:
        st.metric("Jobs Created", "✅ Ready" if has_jobs else "⚠️ Create in Deploy tab")
    with col4:
        st.metric("Schema Generated", "✅ Ready" if has_schema else "⚠️ Generate in Deploy tab")
    
    if not has_dbt or not has_snowflake:
        st.warning("⚠️ Connect to dbt Cloud and Snowflake in the sidebar to use this feature.")
        return
    
    if not has_jobs:
        st.info("💡 Create comparison jobs in the **Deploy to dbt Cloud** tab first (Step 5).")
        return
    
    st.markdown("---")
    
    # ==========================================================================
    # COMPARISON CONFIGURATION
    # ==========================================================================
    st.markdown("#### ⚙️ Comparison Settings")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        num_runs = st.number_input(
            "Number of Comparison Runs",
            min_value=1, max_value=20, value=3,
            help="How many times to run the Core vs Fusion comparison. More runs = better statistical confidence."
        )
    with col2:
        rows_per_source = st.number_input(
            "Rows to Inject per Source",
            min_value=10, max_value=100000, value=100,
            help="Number of new rows to insert into each modified source table per run."
        )
    with col3:
        # Get available sources count
        schema = st.session_state.get('generated_schema', {})
        max_sources = len(schema.get('sources', [])) if schema else 5
        sources_to_change = st.slider(
            "Sources to Modify per Run",
            min_value=0, max_value=max(max_sources, 1), value=min(2, max_sources),
            help="Number of source tables to inject new data into. 0 = test with no new data (SAO should skip most models)."
        )
    
    col4, col5 = st.columns(2)
    with col4:
        warehouse_size = st.selectbox(
            "Warehouse Size",
            options=["X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large"],
            index=2,
            key="comparison_warehouse"
        )
    with col5:
        runs_per_day = st.number_input(
            "Projected Runs per Day",
            min_value=1, max_value=100, value=8,
            key="live_runs_per_day",
            help="For ROI projection: how many times your jobs typically run per day"
        )
    
    st.markdown("---")
    
    # ==========================================================================
    # JOB INFORMATION
    # ==========================================================================
    st.markdown("#### 📊 Comparison Jobs")
    col1, col2 = st.columns(2)
    with col1:
        st.info(f"**Core Job ID:** {st.session_state.get('core_job_id')}")
    with col2:
        st.info(f"**Fusion Job ID:** {st.session_state.get('fusion_job_id')}")
    
    st.markdown("---")
    
    # ==========================================================================
    # RUN COMPARISON
    # ==========================================================================
    st.markdown("#### 🚀 Run Live Comparison")
    
    # Estimate time
    est_time_per_run = 5  # minutes estimate
    total_est_time = num_runs * est_time_per_run * 2  # Core + Fusion
    st.caption(f"⏱️ Estimated time: ~{total_est_time} minutes for {num_runs} comparison runs")
    
    if st.button("▶️ Start Live Comparison", type="primary", use_container_width=True):
        # Initialize comparison engine
        sf_loader = st.session_state.get('sf_loader')
        dbt_api = st.session_state.get('dbt_api')
        
        target_db = st.session_state.get('target_db', 'RAW_DATA')
        target_schema = st.session_state.get('target_schema', 'PUBLIC')
        
        comparison_engine = SAOComparisonEngine(
            dbt_api=dbt_api,
            snowflake_loader=sf_loader,
            database=target_db,
            schema_name=target_schema
        )
        
        # Progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()
        run_results_placeholder = st.empty()
        
        all_results = []
        
        for run_num in range(num_runs):
            status_text.markdown(f"**Run {run_num + 1}/{num_runs}:** Injecting data and running jobs...")
            progress_bar.progress((run_num) / num_runs)
            
            result = comparison_engine.run_single_comparison(
                core_job_id=st.session_state['core_job_id'],
                fusion_job_id=st.session_state['fusion_job_id'],
                rows_to_inject=rows_per_source,
                sources_to_change=sources_to_change,
                generated_schema=st.session_state.get('generated_schema'),
                timeout=1800
            )
            result["run_number"] = run_num + 1
            all_results.append(result)
            
            # Show intermediate results
            with run_results_placeholder.container():
                st.markdown(f"**Run {run_num + 1} Complete:**")
                if result.get("error"):
                    st.error(f"Error: {result['error']}")
                else:
                    # Handle None metrics
                    metrics = result.get("metrics") or {}
                    col1, col2, col3 = st.columns(3)
                    core_dur = metrics.get('core_duration_seconds', 0) or 0
                    fusion_dur = metrics.get('fusion_duration_seconds', 0) or 0
                    time_pct = metrics.get('time_saved_percentage', 0) or 0
                    col1.metric("Core Duration", f"{core_dur:.0f}s")
                    col2.metric("Fusion Duration", f"{fusion_dur:.0f}s")
                    col3.metric("Time Saved", f"{time_pct:.1f}%")
        
        progress_bar.progress(1.0)
        status_text.markdown("**✅ Comparison Complete!**")
        
        # Store results in session state
        st.session_state['comparison_results'] = {
            "runs": all_results,
            "aggregate": _calculate_comparison_aggregate(all_results)
        }
        
        # Calculate ROI projection
        if st.session_state.get('comparison_results'):
            roi_projection = comparison_engine.simulate_roi_projection(
                comparison_results=st.session_state['comparison_results'],
                runs_per_day=runs_per_day,
                warehouse_size=warehouse_size
            )
            st.session_state['roi_projection'] = roi_projection
        
        st.rerun()
    
    # ==========================================================================
    # DISPLAY RESULTS
    # ==========================================================================
    if st.session_state.get('comparison_results'):
        st.markdown("---")
        st.markdown("### 📈 Comparison Results")
        
        results = st.session_state['comparison_results']
        agg = results.get('aggregate', {})
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric(
                "Avg Core Duration",
                f"{agg.get('avg_core_duration', 0):.0f}s",
                help="Average time for dbt Core full build"
            )
        with col2:
            st.metric(
                "Avg Fusion Duration",
                f"{agg.get('avg_fusion_duration', 0):.0f}s",
                help="Average time for dbt Fusion with SAO"
            )
        with col3:
            time_saved = agg.get('avg_time_saved_pct', 0)
            st.metric(
                "Avg Time Saved",
                f"{time_saved:.1f}%",
                delta=f"{time_saved:.1f}%",
                delta_color="normal"
            )
        with col4:
            st.metric(
                "Successful Runs",
                f"{agg.get('successful_runs', 0)}/{agg.get('total_runs', 0)}"
            )
        
        # ROI Projection
        if st.session_state.get('roi_projection'):
            st.markdown("---")
            st.markdown("### 📊 ROI Projection")
            
            roi = st.session_state['roi_projection']
            per_run = roi.get('per_run', {})
            metrics_source = roi.get('metrics_source', 'dbt_cloud_estimate')
            
            # Metrics source indicator + refresh button
            source_col, refresh_col = st.columns([3, 1])
            with source_col:
                if metrics_source == "snowflake":
                    sf_runs = roi.get('sf_runs_with_data', 0)
                    st.success(f"Timing based on **Snowflake execution data** ({sf_runs} runs with data)")
                else:
                    st.warning(
                        "Timing based on **dbt Cloud run duration** (Snowflake ACCOUNT_USAGE data not yet available). "
                        "Click **Refresh** to check for Snowflake metrics — data typically appears within 45 minutes."
                    )
            with refresh_col:
                if st.button("🔄 Refresh Snowflake Metrics", use_container_width=True):
                    sf_loader = st.session_state.get('sf_loader')
                    dbt_api = st.session_state.get('dbt_api')
                    target_db = st.session_state.get('target_db', 'RAW_DATA')
                    target_schema = st.session_state.get('target_schema', 'PUBLIC')
                    
                    if sf_loader and dbt_api:
                        engine = SAOComparisonEngine(
                            dbt_api=dbt_api, snowflake_loader=sf_loader,
                            database=target_db, schema_name=target_schema
                        )
                        with st.spinner("Querying Snowflake ACCOUNT_USAGE..."):
                            updated = engine.refresh_snowflake_metrics(
                                st.session_state['comparison_results']
                            )
                            st.session_state['comparison_results'] = updated
                            
                            wh = st.session_state.get('comparison_warehouse', 'Medium')
                            rpd = st.session_state.get('live_runs_per_day', 8)
                            roi_refreshed = engine.simulate_roi_projection(
                                comparison_results=updated,
                                runs_per_day=rpd,
                                warehouse_size=wh
                            )
                            st.session_state['roi_projection'] = roi_refreshed
                            
                            new_source = updated.get("aggregate", {}).get("metrics_source", "dbt_cloud_estimate")
                            sf_updated = updated.get("sf_refresh_updated", 0)
                            if new_source == "snowflake":
                                st.success(f"Updated {sf_updated} runs with Snowflake metrics. ROI recalculated.")
                            else:
                                st.info(f"Snowflake data still not available for all runs. {sf_updated} runs updated. Try again in a few minutes.")
                        st.rerun()
                    else:
                        st.error("Snowflake connection not available.")
            
            # Headline: Percent Time Saved
            time_saved_pct = per_run.get('time_saved_pct', 0)
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); 
                        border-radius: 12px; padding: 24px; margin: 16px 0;
                        border: 1px solid rgba(99, 102, 241, 0.3);">
                <p style="color: #a0a0c0; font-size: 0.9rem; text-transform: uppercase; margin-bottom: 8px;">
                    Fusion ROI vs Core
                </p>
                <p style="font-size: 2.5rem; font-weight: bold; color: #10b981; margin: 0;">
                    {time_saved_pct:.1f}% ROI
                </p>
                <p style="color: #a0a0c0; margin-top: 8px;">
                    {per_run.get('time_saved_seconds', 0):.0f}s saved per run 
                    ({per_run.get('core_duration_seconds', 0):.0f}s Core vs {per_run.get('fusion_duration_seconds', 0):.0f}s Fusion)
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            # Time-based breakdown
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("**Per Run**")
                st.write(f"Core: {per_run.get('core_duration_seconds', 0):.0f}s")
                st.write(f"Fusion: {per_run.get('fusion_duration_seconds', 0):.0f}s")
                st.write(f"Saved: {per_run.get('time_saved_seconds', 0):.0f}s ({time_saved_pct:.1f}%)")
            
            with col2:
                daily = roi.get('daily', {})
                st.markdown("**Daily ({} runs)**".format(daily.get('runs', 0)))
                st.write(f"Time saved: {daily.get('time_saved_seconds', 0):.0f}s")
                st.write(f"That's {daily.get('time_saved_minutes', 0):.1f} minutes/day")
            
            with col3:
                monthly = roi.get('monthly', {})
                st.markdown("**Monthly**")
                st.write(f"Time saved: {monthly.get('time_saved_hours', 0):.1f} hours")
            
            if results.get("sf_refresh_time"):
                st.caption(f"Snowflake metrics last refreshed: {results['sf_refresh_time']}")
        
        # Detailed run breakdown
        with st.expander("📊 Detailed Run Results", expanded=False):
            runs = results.get('runs', [])
            if runs:
                run_data = []
                for r in runs:
                    metrics = r.get('metrics') or {}
                    injection = r.get('injection') or {}
                    run_data.append({
                        "Run": r.get('run_number', 0),
                        "Sources Changed": metrics.get('sources_changed', 0),
                        "Rows Injected": metrics.get('rows_injected', 0),
                        "Core (s)": metrics.get('core_duration_seconds', 0),
                        "Fusion (s)": metrics.get('fusion_duration_seconds', 0),
                        "Time Saved (s)": metrics.get('time_saved_seconds', 0),
                        "Time Saved %": f"{metrics.get('time_saved_percentage', 0):.1f}%",
                        "Status": "✅" if not r.get('error') else "❌"
                    })
                st.dataframe(pd.DataFrame(run_data), use_container_width=True, hide_index=True)
        
        with st.expander("🔍 Debug: Raw Run Data", expanded=False):
            for r in results.get('runs', []):
                st.markdown(f"**Run {r.get('run_number', '?')}**")
                col_a, col_b = st.columns(2)
                with col_a:
                    st.markdown("**Core (dbt Cloud)**")
                    core_run = r.get('core_run') or {}
                    st.json({
                        "run_id": core_run.get('run_id'),
                        "duration_seconds": core_run.get('duration_seconds'),
                        "status": core_run.get('status_humanized'),
                        "created_at": core_run.get('created_at'),
                        "finished_at": core_run.get('finished_at'),
                    })
                with col_b:
                    st.markdown("**Fusion (dbt Cloud)**")
                    fusion_run = r.get('fusion_run') or {}
                    st.json({
                        "run_id": fusion_run.get('run_id'),
                        "duration_seconds": fusion_run.get('duration_seconds'),
                        "status": fusion_run.get('status_humanized'),
                        "created_at": fusion_run.get('created_at'),
                        "finished_at": fusion_run.get('finished_at'),
                    })
                if r.get('error'):
                    st.error(f"Error: {r['error']}")
                
                # Only show Snowflake metrics if there's actual data
                sf = r.get('snowflake_metrics') or {}
                sf_core = sf.get('core', {})
                sf_fusion = sf.get('fusion', {})
                core_has_sf = sf_core.get('query_count', 0) > 0
                fusion_has_sf = sf_fusion.get('query_count', 0) > 0
                
                if core_has_sf or fusion_has_sf:
                    col_c, col_d = st.columns(2)
                    with col_c:
                        st.markdown("**Core (Snowflake)**")
                        st.json({
                            "query_count": sf_core.get('query_count', 0),
                            "execution_seconds": sf_core.get('execution_seconds', 0),
                            "total_seconds": sf_core.get('total_seconds', 0),
                            "credits_used": sf_core.get('credits_used', 0),
                        })
                    with col_d:
                        st.markdown("**Fusion (Snowflake)**")
                        st.json({
                            "query_count": sf_fusion.get('query_count', 0),
                            "execution_seconds": sf_fusion.get('execution_seconds', 0),
                            "total_seconds": sf_fusion.get('total_seconds', 0),
                            "credits_used": sf_fusion.get('credits_used', 0),
                        })
                else:
                    st.caption("Snowflake metrics: pending (ACCOUNT_USAGE data not yet available — use Refresh button)")
                st.markdown("---")
        
        # Visualization
        st.markdown("---")
        st.markdown("### 📊 Comparison Visualization")
        
        runs = results.get('runs', [])
        if runs:
            # Prepare data for chart - handle None metrics
            chart_data = []
            for r in runs:
                metrics = r.get('metrics') or {}
                chart_data.append({
                    "Run": f"Run {r.get('run_number', 0)}",
                    "dbt Core": metrics.get('core_duration_seconds', 0) or 0,
                    "dbt Fusion (SAO)": metrics.get('fusion_duration_seconds', 0) or 0
                })
            
            df_chart = pd.DataFrame(chart_data)
            
            # Bar chart comparison
            fig = go.Figure()
            fig.add_trace(go.Bar(
                name='dbt Core',
                x=df_chart['Run'],
                y=df_chart['dbt Core'],
                marker_color='#ef4444'
            ))
            fig.add_trace(go.Bar(
                name='dbt Fusion (SAO)',
                x=df_chart['Run'],
                y=df_chart['dbt Fusion (SAO)'],
                marker_color='#10b981'
            ))
            fig.update_layout(
                title="Job Duration Comparison",
                xaxis_title="Run",
                yaxis_title="Duration (seconds)",
                barmode='group',
                template='plotly_dark',
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Time saved pie chart
            col1, col2 = st.columns(2)
            with col1:
                avg_core = agg.get('avg_core_duration', 0) or 0
                avg_fusion = agg.get('avg_fusion_duration', 0) or 0
                time_saved = avg_core - avg_fusion
                
                # Ensure positive values for pie chart
                if time_saved < 0:
                    time_saved = 0
                
                fig_pie = go.Figure(data=[go.Pie(
                    labels=['Fusion Runtime', 'Time Saved'],
                    values=[max(avg_fusion, 0), max(time_saved, 0)],
                    hole=0.5,
                    marker_colors=['#10b981', '#6366f1']
                )])
                fig_pie.update_layout(
                    title="Average Runtime Breakdown",
                    template='plotly_dark',
                    height=300
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with col2:
                # Sources changed vs time saved scatter - handle None metrics
                scatter_data = []
                for r in runs:
                    metrics = r.get('metrics') or {}
                    scatter_data.append({
                        "Sources Changed": metrics.get('sources_changed', 0) or 0,
                        "Time Saved %": metrics.get('time_saved_percentage', 0) or 0
                    })
                
                df_scatter = pd.DataFrame(scatter_data)
                fig_scatter = px.scatter(
                    df_scatter,
                    x="Sources Changed",
                    y="Time Saved %",
                    title="Sources Changed vs Time Saved",
                    template='plotly_dark',
                    height=300
                )
                fig_scatter.update_traces(marker=dict(size=12, color='#6366f1'))
                st.plotly_chart(fig_scatter, use_container_width=True)


def _calculate_comparison_aggregate(runs: list) -> dict:
    """Calculate aggregate statistics from comparison runs."""
    successful_runs = [r for r in runs if not r.get("error") and r.get("metrics")]
    
    if not successful_runs:
        return {"error": "No successful runs", "total_runs": len(runs)}
    
    core_times = [r["metrics"]["core_duration_seconds"] for r in successful_runs]
    fusion_times = [r["metrics"]["fusion_duration_seconds"] for r in successful_runs]
    time_saved_pcts = [r["metrics"]["time_saved_percentage"] for r in successful_runs]
    
    # Snowflake execution times (actual compute, excluding dbt compilation/artifacts)
    sf_runs = [r for r in successful_runs if r["metrics"].get("sf_metrics_available")]
    sf_core_exec = [r["metrics"]["sf_core_execution_seconds"] for r in sf_runs]
    sf_fusion_exec = [r["metrics"]["sf_fusion_execution_seconds"] for r in sf_runs]
    
    has_snowflake = len(sf_runs) > 0
    
    return {
        "total_runs": len(runs),
        "successful_runs": len(successful_runs),
        "avg_core_duration": round(np.mean(core_times), 2) if core_times else 0,
        "avg_fusion_duration": round(np.mean(fusion_times), 2) if fusion_times else 0,
        "avg_time_saved_pct": round(np.mean(time_saved_pcts), 1) if time_saved_pcts else 0,
        "min_time_saved_pct": round(min(time_saved_pcts), 1) if time_saved_pcts else 0,
        "max_time_saved_pct": round(max(time_saved_pcts), 1) if time_saved_pcts else 0,
        "total_rows_injected": sum(r["injection"].get("total_rows_injected", 0) for r in successful_runs if r.get("injection")),
        "sf_avg_core_execution": round(np.mean(sf_core_exec), 2) if sf_core_exec else 0,
        "sf_avg_fusion_execution": round(np.mean(sf_fusion_exec), 2) if sf_fusion_exec else 0,
        "sf_runs_with_data": len(sf_runs),
        "metrics_source": "snowflake" if has_snowflake else "dbt_cloud_estimate",
    }


# =============================================================================
# MAIN APP
# =============================================================================

def main():
    init_session_state()
    render_sidebar()
    render_header()
    
    tab1, tab2, tab3, tab4 = st.tabs(["📝 Configure & Simulate", "🚀 Deploy to dbt Cloud", "🔬 SAO Live Comparison", "📈 Results"])
    
    with tab1:
        # Render pipeline configuration inputs and get the config
        pipeline_config = render_pipeline_config()
        st.session_state['pipeline_config'] = pipeline_config
        
        st.markdown("---")
        
        if st.button("🎯 Run SAO Simulation", type="primary", use_container_width=True):
            simulation_results, savings_analysis = run_simulation(pipeline_config)
            st.session_state['simulation_results'] = simulation_results
            st.session_state['savings_analysis'] = savings_analysis
            st.session_state['simulation_complete'] = True
            st.success("✅ Simulation complete! Go to the **Results** tab to see your savings.")
    
    with tab2:
        render_deploy_tab()
    
    with tab3:
        render_sao_comparison_tab()
    
    with tab4:
        if st.session_state.get('simulation_complete'):
            render_results(
                st.session_state['simulation_results'],
                st.session_state['savings_analysis']
            )
        else:
            st.info("Run a simulation in the first tab to see results here.")
            
            if st.button("🎮 Load Demo Results"):
                demo_config = {
                    'num_models': 200, 'num_sources': 15, 'runs_per_day': 24,
                    'materialization': {'incremental': 30, 'table': 40, 'view': 30},
                    'avg_row_count': 1000000, 'avg_column_count': 30,
                    'dag_depth': 5, 'source_change_rate': 30,
                    'warehouse_size': 'Medium', 'current_monthly_cost': 5000,
                }
                simulation_results, savings_analysis = run_simulation(demo_config)
                st.session_state['simulation_results'] = simulation_results
                st.session_state['savings_analysis'] = savings_analysis
                st.session_state['simulation_complete'] = True
                st.rerun()


if __name__ == "__main__":
    main()



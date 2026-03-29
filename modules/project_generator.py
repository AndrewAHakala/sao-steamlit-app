"""
dbt Project Generator Module
============================
Generates dbt project structure (models, sources, configs) based on
user-specified pipeline characteristics.
"""

import os
import random
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
import yaml
from jinja2 import Template


@dataclass
class ModelConfig:
    """Configuration for a single dbt model."""
    name: str
    materialization: str  # 'view', 'table', 'incremental'
    dependencies: List[str] = field(default_factory=list)
    is_source: bool = False
    layer: str = "staging"  # staging, intermediate, marts
    description: str = ""
    columns: List[Dict[str, Any]] = field(default_factory=list)
    unique_key: Optional[str] = None
    incremental_strategy: Optional[str] = None


@dataclass
class SourceConfig:
    """Configuration for a dbt source."""
    name: str
    schema_name: str
    database: Optional[str] = None
    tables: List[Dict[str, str]] = field(default_factory=list)


class DBTProjectGenerator:
    """
    Generates a complete dbt project structure for SAO simulation.
    
    Creates:
    - dbt_project.yml
    - Source definitions
    - Staging models
    - Intermediate models
    - Marts models
    - Model configurations
    """
    
    # Model name patterns by layer
    MODEL_PATTERNS = {
        "staging": ["stg_{source}_{entity}"],
        "intermediate": ["int_{entity}_{verb}", "int_{entity}_pivot", "int_{entity}_agg"],
        "marts": ["fct_{entity}", "dim_{entity}", "rpt_{entity}"],
    }
    
    # Common transformation verbs
    VERBS = ["joined", "filtered", "enriched", "deduped", "aggregated", "pivoted", "unioned"]
    
    # Common business entities
    ENTITIES = [
        "customers", "orders", "products", "transactions", "events",
        "sessions", "users", "accounts", "payments", "shipments",
        "inventory", "subscriptions", "invoices", "returns"
    ]
    
    def __init__(self, project_name: str = "sao_simulation"):
        """
        Initialize the project generator.
        
        Args:
            project_name: Name for the dbt project
        """
        self.project_name = project_name
        self.models: List[ModelConfig] = []
        self.sources: List[SourceConfig] = []
    
    def generate_project(
        self,
        num_models: int,
        materialization_dist: Dict[str, int],
        num_sources: int,
        dag_depth: int = 5,
        avg_dependencies: int = 3,
        source_schemas: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Generate a complete dbt project configuration.
        
        Args:
            num_models: Total number of models to generate
            materialization_dist: Distribution of materializations (percentages)
            num_sources: Number of source tables
            dag_depth: Average depth of the DAG
            avg_dependencies: Average number of dependencies per model
            source_schemas: List of source schema names
            
        Returns:
            Project configuration dictionary
        """
        # Calculate model counts by materialization
        total_pct = sum(materialization_dist.values())
        model_counts = {
            mat: int(num_models * pct / total_pct)
            for mat, pct in materialization_dist.items()
        }
        
        # Ensure we hit the target
        diff = num_models - sum(model_counts.values())
        if diff > 0:
            model_counts["table"] = model_counts.get("table", 0) + diff
        
        # Generate sources
        self._generate_sources(num_sources, source_schemas)
        
        # Generate models in layers
        self._generate_models_by_layer(
            model_counts,
            dag_depth,
            avg_dependencies
        )
        
        # Build project configuration
        project_config = {
            "project": self._generate_dbt_project_yml(),
            "sources": self._generate_sources_yml(),
            "models": self._generate_model_files(),
            "schema": self._generate_schema_yml(),
            "dag_structure": self._get_dag_structure(),
        }
        
        return project_config
    
    def _generate_sources(
        self,
        num_sources: int,
        source_schemas: Optional[List[str]] = None
    ):
        """Generate source definitions."""
        if source_schemas:
            tables = source_schemas
        else:
            # Generate source table names
            tables = []
            for i in range(num_sources):
                entity = random.choice(self.ENTITIES)
                prefix = random.choice(["raw", "src", "landing"])
                tables.append(f"{prefix}_{entity}")
        
        # Group by schema
        schema_groups = {}
        for table in tables:
            schema = "raw_data"  # Default schema
            if table not in schema_groups.get(schema, []):
                schema_groups.setdefault(schema, []).append(table)
        
        # Create source configs
        for schema, table_list in schema_groups.items():
            self.sources.append(SourceConfig(
                name=f"src_{schema.replace('_', '')}",
                schema_name=schema,
                tables=[{"name": t, "identifier": t} for t in table_list]
            ))
    
    def _generate_models_by_layer(
        self,
        model_counts: Dict[str, int],
        dag_depth: int,
        avg_dependencies: int
    ):
        """Generate models organized by layer."""
        total_models = sum(model_counts.values())
        
        # Distribute models across layers
        # Staging: ~30%, Intermediate: ~40%, Marts: ~30%
        staging_count = max(len(self._get_all_source_tables()), int(total_models * 0.3))
        marts_count = int(total_models * 0.3)
        intermediate_count = total_models - staging_count - marts_count
        
        # Generate staging models (one per source + extras)
        staging_models = self._generate_staging_models(staging_count, model_counts)
        
        # Generate intermediate models
        intermediate_models = self._generate_intermediate_models(
            intermediate_count,
            staging_models,
            model_counts,
            avg_dependencies
        )
        
        # Generate marts models
        marts_models = self._generate_marts_models(
            marts_count,
            staging_models + intermediate_models,
            model_counts,
            avg_dependencies
        )
        
        self.models = staging_models + intermediate_models + marts_models
    
    def _generate_staging_models(
        self,
        count: int,
        model_counts: Dict[str, int]
    ) -> List[ModelConfig]:
        """Generate staging layer models."""
        models = []
        source_tables = self._get_all_source_tables()
        
        # Create a staging model for each source
        for source, table in source_tables:
            entity = table.split("_")[-1] if "_" in table else table
            model = ModelConfig(
                name=f"stg_{source}_{entity}",
                materialization="view",  # Staging is typically views
                dependencies=[f"source('{source}', '{table}')"],
                is_source=False,
                layer="staging",
                description=f"Staging model for {table}",
                columns=self._generate_column_config(entity)
            )
            models.append(model)
        
        # Add extra staging models if needed
        while len(models) < count:
            entity = random.choice(self.ENTITIES)
            source = self.sources[0].name if self.sources else "src_raw"
            model = ModelConfig(
                name=f"stg_{source}_{entity}_{len(models)}",
                materialization=self._pick_materialization(model_counts, "staging"),
                dependencies=[models[random.randint(0, len(models)-1)].name] if models else [],
                layer="staging",
                description=f"Additional staging model for {entity}",
            )
            models.append(model)
        
        return models[:count]
    
    def _generate_intermediate_models(
        self,
        count: int,
        upstream_models: List[ModelConfig],
        model_counts: Dict[str, int],
        avg_dependencies: int
    ) -> List[ModelConfig]:
        """Generate intermediate layer models."""
        models = []
        
        for i in range(count):
            entity = random.choice(self.ENTITIES)
            verb = random.choice(self.VERBS)
            
            # Pick dependencies from upstream
            num_deps = min(avg_dependencies, len(upstream_models))
            deps = random.sample(
                [m.name for m in upstream_models],
                max(1, random.randint(1, num_deps))
            )
            
            materialization = self._pick_materialization(model_counts, "intermediate")
            
            model = ModelConfig(
                name=f"int_{entity}_{verb}_{i}",
                materialization=materialization,
                dependencies=deps,
                layer="intermediate",
                description=f"Intermediate model: {entity} {verb}",
                unique_key="id" if materialization == "incremental" else None,
                incremental_strategy="merge" if materialization == "incremental" else None,
            )
            models.append(model)
            
            # Add to upstream pool for next iterations
            upstream_models = upstream_models + [model]
        
        return models
    
    def _generate_marts_models(
        self,
        count: int,
        upstream_models: List[ModelConfig],
        model_counts: Dict[str, int],
        avg_dependencies: int
    ) -> List[ModelConfig]:
        """Generate marts layer models."""
        models = []
        
        # Generate fact and dimension tables
        for i in range(count):
            entity = random.choice(self.ENTITIES)
            
            if i % 3 == 0:
                prefix = "fct"  # Fact tables
            elif i % 3 == 1:
                prefix = "dim"  # Dimension tables
            else:
                prefix = "rpt"  # Report tables
            
            # Pick dependencies
            num_deps = min(avg_dependencies + 1, len(upstream_models))
            deps = random.sample(
                [m.name for m in upstream_models],
                max(1, random.randint(2, num_deps))
            )
            
            materialization = self._pick_materialization(model_counts, "marts")
            
            model = ModelConfig(
                name=f"{prefix}_{entity}_{i}",
                materialization=materialization,
                dependencies=deps,
                layer="marts",
                description=f"Marts {prefix} model for {entity}",
                unique_key="id" if materialization == "incremental" else None,
                incremental_strategy="delete+insert" if materialization == "incremental" else None,
                columns=self._generate_column_config(entity, is_marts=True)
            )
            models.append(model)
        
        return models
    
    def _pick_materialization(
        self,
        model_counts: Dict[str, int],
        layer: str
    ) -> str:
        """Pick a materialization based on remaining counts and layer."""
        # Layer preferences
        preferences = {
            "staging": ["view", "table", "incremental"],
            "intermediate": ["table", "view", "incremental"],
            "marts": ["table", "incremental", "view"],
        }
        
        # Check what's available
        available = [m for m, c in model_counts.items() if c > 0]
        
        if not available:
            return "view"  # Default fallback
        
        # Prefer based on layer
        for pref in preferences.get(layer, ["view"]):
            if pref in available:
                model_counts[pref] -= 1
                return pref
        
        # Fall back to first available
        choice = available[0]
        model_counts[choice] -= 1
        return choice
    
    def _get_all_source_tables(self) -> List[Tuple[str, str]]:
        """Get list of (source_name, table_name) tuples."""
        tables = []
        for source in self.sources:
            for table in source.tables:
                tables.append((source.name, table["name"]))
        return tables
    
    def _generate_column_config(
        self,
        entity: str,
        is_marts: bool = False
    ) -> List[Dict[str, Any]]:
        """Generate column configurations for a model."""
        base_columns = [
            {"name": "id", "description": "Primary key", "tests": ["unique", "not_null"]},
            {"name": "created_at", "description": "Record creation timestamp"},
            {"name": "updated_at", "description": "Record update timestamp"},
        ]
        
        entity_columns = {
            "customers": [
                {"name": "customer_id", "description": "Customer identifier"},
                {"name": "email", "description": "Customer email"},
                {"name": "status", "description": "Customer status"},
            ],
            "orders": [
                {"name": "order_id", "description": "Order identifier"},
                {"name": "customer_id", "description": "Customer foreign key"},
                {"name": "total_amount", "description": "Order total"},
            ],
            "products": [
                {"name": "product_id", "description": "Product identifier"},
                {"name": "name", "description": "Product name"},
                {"name": "price", "description": "Product price"},
            ],
        }
        
        columns = base_columns.copy()
        columns.extend(entity_columns.get(entity, []))
        
        if is_marts:
            # Add metric columns for marts
            columns.extend([
                {"name": "metric_value", "description": "Calculated metric"},
                {"name": "record_count", "description": "Aggregated count"},
            ])
        
        return columns
    
    def _generate_dbt_project_yml(self) -> Dict[str, Any]:
        """Generate dbt_project.yml content."""
        return {
            "name": self.project_name,
            "version": "1.0.0",
            "config-version": 2,
            "profile": self.project_name,
            "model-paths": ["models"],
            "analysis-paths": ["analyses"],
            "test-paths": ["tests"],
            "seed-paths": ["seeds"],
            "macro-paths": ["macros"],
            "snapshot-paths": ["snapshots"],
            "clean-targets": ["target", "dbt_packages"],
            "models": {
                self.project_name: {
                    "staging": {
                        "+materialized": "view",
                        "+schema": "staging",
                    },
                    "intermediate": {
                        "+materialized": "table",
                        "+schema": "intermediate",
                    },
                    "marts": {
                        "+materialized": "table",
                        "+schema": "marts",
                    },
                }
            }
        }
    
    def _generate_sources_yml(self) -> Dict[str, Any]:
        """Generate sources.yml content."""
        sources_config = {"version": 2, "sources": []}
        
        for source in self.sources:
            source_entry = {
                "name": source.name,
                "schema": source.schema_name,
                "tables": source.tables,
            }
            if source.database:
                source_entry["database"] = source.database
            
            sources_config["sources"].append(source_entry)
        
        return sources_config
    
    def _generate_model_files(self) -> Dict[str, str]:
        """Generate SQL content for all models."""
        model_files = {}
        
        for model in self.models:
            sql = self._generate_model_sql(model)
            path = f"models/{model.layer}/{model.name}.sql"
            model_files[path] = sql
        
        return model_files
    
    def _generate_model_sql(self, model: ModelConfig) -> str:
        """Generate SQL content for a single model."""
        # Config block
        config_parts = [f"materialized='{model.materialization}'"]
        
        if model.unique_key:
            config_parts.append(f"unique_key='{model.unique_key}'")
        
        if model.incremental_strategy:
            config_parts.append(f"incremental_strategy='{model.incremental_strategy}'")
        
        config_block = "{{ config(" + ", ".join(config_parts) + ") }}"
        
        # CTE for each dependency
        ctes = []
        for i, dep in enumerate(model.dependencies):
            if dep.startswith("source("):
                ref = "{{ " + dep + " }}"
            else:
                ref = "{{ ref('" + dep + "') }}"
            
            cte_name = f"source_{i}" if "source(" in dep else dep.replace("-", "_")
            ctes.append(f"{cte_name} as (\n    select * from {ref}\n)")
        
        # Build SQL
        sql_parts = [
            config_block,
            "",
            "-- " + model.description,
            "",
        ]
        
        if ctes:
            sql_parts.append("with")
            sql_parts.append(",\n\n".join(ctes))
            sql_parts.append("")
            sql_parts.append("select")
            sql_parts.append("    *")
            
            # Reference first CTE
            first_cte = ctes[0].split(" as (")[0].strip()
            sql_parts.append(f"from {first_cte}")
            
            # Add join placeholders for multiple dependencies
            for i, cte in enumerate(ctes[1:], 1):
                cte_name = cte.split(" as (")[0].strip()
                sql_parts.append(f"left join {cte_name} using (id)")
        else:
            sql_parts.append("select 1 as id")
        
        # Add incremental filter
        if model.materialization == "incremental":
            sql_parts.append("")
            sql_parts.append("{% if is_incremental() %}")
            sql_parts.append("where updated_at > (select max(updated_at) from {{ this }})")
            sql_parts.append("{% endif %}")
        
        return "\n".join(sql_parts)
    
    def _generate_schema_yml(self) -> Dict[str, Any]:
        """Generate schema.yml with model documentation."""
        schema = {"version": 2, "models": []}
        
        for model in self.models:
            model_entry = {
                "name": model.name,
                "description": model.description,
            }
            
            if model.columns:
                model_entry["columns"] = model.columns
            
            schema["models"].append(model_entry)
        
        return schema
    
    def _get_dag_structure(self) -> Dict[str, Any]:
        """Get the DAG structure for visualization."""
        nodes = []
        edges = []
        
        # Add source nodes
        for source in self.sources:
            for table in source.tables:
                nodes.append({
                    "id": f"source.{source.name}.{table['name']}",
                    "type": "source",
                    "name": table["name"],
                })
        
        # Add model nodes and edges
        for model in self.models:
            nodes.append({
                "id": f"model.{model.name}",
                "type": "model",
                "name": model.name,
                "materialization": model.materialization,
                "layer": model.layer,
            })
            
            for dep in model.dependencies:
                if dep.startswith("source("):
                    # Parse source reference
                    parts = dep.replace("source('", "").replace("')", "").replace("'", "").split(", ")
                    source_id = f"source.{parts[0]}.{parts[1]}"
                else:
                    source_id = f"model.{dep}"
                
                edges.append({
                    "source": source_id,
                    "target": f"model.{model.name}",
                })
        
        return {
            "nodes": nodes,
            "edges": edges,
            "depth": self._calculate_dag_depth(edges),
        }
    
    def _calculate_dag_depth(self, edges: List[Dict]) -> int:
        """Calculate the maximum depth of the DAG."""
        # Build adjacency list
        graph = {}
        for edge in edges:
            graph.setdefault(edge["source"], []).append(edge["target"])
        
        # Find max depth using DFS
        def dfs(node, visited):
            if node in visited:
                return 0
            visited.add(node)
            
            children = graph.get(node, [])
            if not children:
                return 1
            
            return 1 + max(dfs(child, visited.copy()) for child in children)
        
        # Start from source nodes
        source_nodes = [n for n in graph.keys() if n.startswith("source.")]
        if not source_nodes:
            return 0
        
        return max(dfs(source, set()) for source in source_nodes)
    
    def export_project(self, output_dir: str):
        """
        Export the generated project to files.
        
        Args:
            output_dir: Directory to write files to
        """
        # Create directory structure
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(os.path.join(output_dir, "models", "staging"), exist_ok=True)
        os.makedirs(os.path.join(output_dir, "models", "intermediate"), exist_ok=True)
        os.makedirs(os.path.join(output_dir, "models", "marts"), exist_ok=True)
        
        # Write dbt_project.yml
        project_yml = self._generate_dbt_project_yml()
        with open(os.path.join(output_dir, "dbt_project.yml"), "w") as f:
            yaml.dump(project_yml, f, default_flow_style=False)
        
        # Write sources.yml
        sources_yml = self._generate_sources_yml()
        with open(os.path.join(output_dir, "models", "sources.yml"), "w") as f:
            yaml.dump(sources_yml, f, default_flow_style=False)
        
        # Write model files
        model_files = self._generate_model_files()
        for path, content in model_files.items():
            full_path = os.path.join(output_dir, path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "w") as f:
                f.write(content)
        
        # Write schema.yml files by layer
        for layer in ["staging", "intermediate", "marts"]:
            layer_models = [m for m in self.models if m.layer == layer]
            if layer_models:
                schema = {
                    "version": 2,
                    "models": [
                        {
                            "name": m.name,
                            "description": m.description,
                            "columns": m.columns if m.columns else []
                        }
                        for m in layer_models
                    ]
                }
                schema_path = os.path.join(output_dir, "models", layer, "schema.yml")
                with open(schema_path, "w") as f:
                    yaml.dump(schema, f, default_flow_style=False)


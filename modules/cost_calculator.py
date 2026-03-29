"""
SAO Cost Calculator Module
==========================
Calculates cost savings from State Aware Orchestration (SAO) based on
pipeline simulation results.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import numpy as np
import random


@dataclass
class SnowflakeWarehousePricing:
    """Snowflake warehouse pricing by size (credits per hour)."""
    X_SMALL: float = 1.0
    SMALL: float = 2.0
    MEDIUM: float = 4.0
    LARGE: float = 8.0
    X_LARGE: float = 16.0
    TWO_X_LARGE: float = 32.0
    
    # Cost per credit (USD) - varies by region/contract
    CREDIT_COST_USD: float = 3.0


class SAOCostCalculator:
    """
    Calculate cost savings from SAO based on pipeline configuration
    and simulation results.
    
    SAO (State Aware Orchestration) optimizes dbt pipelines by:
    1. Tracking source data freshness
    2. Skipping unchanged models
    3. Only rebuilding models with changed upstream data
    """
    
    def __init__(self, credit_cost: float = 3.0):
        """
        Initialize the calculator.
        
        Args:
            credit_cost: Cost per Snowflake credit in USD
        """
        self.pricing = SnowflakeWarehousePricing()
        self.pricing.CREDIT_COST_USD = credit_cost
    
    def simulate_pipeline_runs(
        self,
        pipeline_config: Dict[str, Any],
        num_simulation_runs: int = 24,
        seed: int = 42
    ) -> Dict[str, Any]:
        """
        Simulate pipeline runs with and without SAO.
        
        Args:
            pipeline_config: Pipeline configuration from user inputs
            num_simulation_runs: Number of runs to simulate
            seed: Random seed for reproducibility
            
        Returns:
            Simulation results with metrics
        """
        np.random.seed(seed)
        random.seed(seed)
        
        num_models = pipeline_config['num_models']
        num_sources = pipeline_config['num_sources']
        source_change_rate = pipeline_config['source_change_rate'] / 100.0
        dag_depth = pipeline_config['dag_depth']
        materialization = pipeline_config['materialization']
        
        # Calculate base execution time per model (seconds)
        base_time_per_model = self._estimate_model_execution_time(
            pipeline_config['avg_row_count'],
            pipeline_config['avg_column_count'],
            pipeline_config['warehouse_size']
        )
        
        run_details = []
        total_models_skipped = 0
        total_time_saved = 0
        
        for run_num in range(num_simulation_runs):
            # Simulate source changes for this run
            sources_changed = self._simulate_source_changes(
                num_sources,
                source_change_rate
            )
            
            # Calculate models affected by changes
            models_affected = self._calculate_affected_models(
                num_models,
                sources_changed,
                num_sources,
                dag_depth,
                materialization
            )
            
            # Core runs all models
            core_models_run = num_models
            core_time = core_models_run * base_time_per_model
            
            # Fusion with SAO only runs affected models
            fusion_models_run = models_affected
            fusion_time = fusion_models_run * base_time_per_model
            
            # Add some variance for realism
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
        
        # Calculate aggregate metrics
        avg_models_skipped = total_models_skipped / num_simulation_runs
        avg_time_saved = total_time_saved / num_simulation_runs
        avg_core_time = sum(r['time_core_sec'] for r in run_details) / num_simulation_runs
        avg_fusion_time = sum(r['time_fusion_sec'] for r in run_details) / num_simulation_runs
        
        time_saved_percentage = (
            (avg_core_time - avg_fusion_time) / avg_core_time * 100
            if avg_core_time > 0 else 0
        )
        
        return {
            'run_details': run_details,
            'total_runs': num_simulation_runs,
            'total_models_skipped': total_models_skipped,
            'total_time_saved_sec': round(total_time_saved, 2),
            'avg_models_skipped': round(avg_models_skipped, 1),
            'avg_time_saved_sec': round(avg_time_saved, 2),
            'avg_core_time_sec': round(avg_core_time, 2),
            'avg_fusion_time_sec': round(avg_fusion_time, 2),
            'time_saved_percentage': round(time_saved_percentage, 1),
            'skip_rate': round(avg_models_skipped / pipeline_config['num_models'] * 100, 1),
        }
    
    def _estimate_model_execution_time(
        self,
        avg_row_count: int,
        avg_column_count: int,
        warehouse_size: str
    ) -> float:
        """
        Estimate average execution time per model in seconds.
        
        Based on data size and warehouse capacity.
        """
        # Base time factors
        row_factor = np.log10(max(avg_row_count, 1000)) / 6  # Normalize to 0-1
        column_factor = avg_column_count / 100  # Normalize
        
        # Warehouse multipliers (larger = faster)
        warehouse_multipliers = {
            "X-Small": 1.0,
            "Small": 0.6,
            "Medium": 0.35,
            "Large": 0.2,
            "X-Large": 0.12,
            "2X-Large": 0.07,
        }
        
        warehouse_mult = warehouse_multipliers.get(warehouse_size, 0.35)
        
        # Base execution time: 5-30 seconds depending on complexity
        base_time = 5 + (row_factor * 20) + (column_factor * 5)
        
        return base_time * warehouse_mult
    
    def _simulate_source_changes(
        self,
        num_sources: int,
        change_rate: float
    ) -> int:
        """Simulate how many sources have changed in a given run."""
        # Use binomial distribution
        return np.random.binomial(num_sources, change_rate)
    
    def _calculate_affected_models(
        self,
        num_models: int,
        sources_changed: int,
        total_sources: int,
        dag_depth: int,
        materialization: Dict[str, int]
    ) -> int:
        """
        Calculate how many models need to be rebuilt based on source changes.
        
        SAO tracks:
        - Which sources have new data
        - Which models depend on changed sources
        - The cascade effect through the DAG
        """
        if sources_changed == 0:
            # No source changes = minimal rebuilds (just views refresh)
            view_pct = materialization.get('view', 0) / 100.0
            return max(1, int(num_models * view_pct * 0.1))
        
        # Calculate base affected percentage from source changes
        source_impact = sources_changed / total_sources
        
        # DAG depth affects propagation
        # Deeper DAGs = more models potentially affected per source
        depth_factor = 1 + (dag_depth - 1) * 0.1  # 10% increase per depth level
        
        # Materialization affects rebuild needs
        # Incremental models benefit most from SAO
        incremental_pct = materialization.get('incremental', 0) / 100.0
        table_pct = materialization.get('table', 0) / 100.0
        view_pct = materialization.get('view', 0) / 100.0
        
        # Incremental models only process new data
        # Tables need full rebuild but can be skipped
        # Views always refresh but are cheap
        
        incremental_affected = incremental_pct * source_impact * 0.3  # SAO benefit
        table_affected = table_pct * source_impact * depth_factor
        view_affected = view_pct * 0.8  # Views usually refresh
        
        total_affected_pct = incremental_affected + table_affected + view_affected
        
        # Add some randomness
        total_affected_pct *= np.random.uniform(0.8, 1.2)
        
        # Clamp between minimum and maximum
        total_affected_pct = min(max(total_affected_pct, 0.05), 1.0)
        
        return max(1, int(num_models * total_affected_pct))
    
    def calculate_savings(
        self,
        simulation_results: Dict[str, Any],
        warehouse_size: str,
        current_monthly_cost: float
    ) -> Dict[str, Any]:
        """
        Calculate cost savings based on simulation results.
        
        Args:
            simulation_results: Results from simulate_pipeline_runs
            warehouse_size: Snowflake warehouse size
            current_monthly_cost: Current monthly compute cost
            
        Returns:
            Savings analysis with various metrics
        """
        # Get warehouse credits per hour
        warehouse_credits = {
            "X-Small": self.pricing.X_SMALL,
            "Small": self.pricing.SMALL,
            "Medium": self.pricing.MEDIUM,
            "Large": self.pricing.LARGE,
            "X-Large": self.pricing.X_LARGE,
            "2X-Large": self.pricing.TWO_X_LARGE,
        }
        
        credits_per_hour = warehouse_credits.get(warehouse_size, self.pricing.MEDIUM)
        credits_per_second = credits_per_hour / 3600
        cost_per_second = credits_per_second * self.pricing.CREDIT_COST_USD
        
        # Calculate savings from time reduction
        time_saved_percentage = simulation_results['time_saved_percentage']
        
        # If user provided current cost, use that as baseline
        if current_monthly_cost > 0:
            monthly_savings = current_monthly_cost * (time_saved_percentage / 100)
            cost_after = current_monthly_cost - monthly_savings
        else:
            # Estimate from simulation
            total_runs_per_month = simulation_results['total_runs'] * 30
            monthly_core_time = simulation_results['avg_core_time_sec'] * total_runs_per_month
            monthly_fusion_time = simulation_results['avg_fusion_time_sec'] * total_runs_per_month
            
            monthly_core_cost = monthly_core_time * cost_per_second
            monthly_fusion_cost = monthly_fusion_time * cost_per_second
            
            monthly_savings = monthly_core_cost - monthly_fusion_cost
            cost_after = monthly_fusion_cost
            current_monthly_cost = monthly_core_cost
        
        # Calculate annual projections
        annual_savings = monthly_savings * 12
        annual_cost_before = current_monthly_cost * 12
        annual_cost_after = cost_after * 12
        
        # ROI calculation (assuming dbt Cloud Enterprise upgrade cost)
        # This is a simplified estimate
        estimated_upgrade_cost = current_monthly_cost * 0.3  # 30% price increase estimate
        roi = annual_savings / (estimated_upgrade_cost * 12) if estimated_upgrade_cost > 0 else 0
        
        return {
            'monthly_savings': round(monthly_savings, 2),
            'annual_savings': round(annual_savings, 2),
            'cost_before': round(current_monthly_cost, 2),
            'cost_after': round(cost_after, 2),
            'savings_percentage': round(time_saved_percentage, 1),
            'annual_cost_before': round(annual_cost_before, 2),
            'annual_cost_after': round(annual_cost_after, 2),
            'roi_estimate': round(roi, 1),
            'credits_saved_per_run': round(
                simulation_results['avg_time_saved_sec'] * credits_per_second,
                2
            ),
            'credits_saved_monthly': round(
                simulation_results['avg_time_saved_sec'] * credits_per_second * 
                simulation_results['total_runs'] * 30,
                2
            ),
        }
    
    def generate_savings_breakdown(
        self,
        pipeline_config: Dict[str, Any],
        simulation_results: Dict[str, Any],
        savings_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a detailed breakdown of savings by category.
        
        Args:
            pipeline_config: Original pipeline configuration
            simulation_results: Simulation results
            savings_analysis: Calculated savings
            
        Returns:
            Detailed breakdown by model type, layer, etc.
        """
        materialization = pipeline_config['materialization']
        total_savings = savings_analysis['monthly_savings']
        
        # Estimate savings contribution by materialization type
        # Incremental models benefit most from SAO
        incremental_benefit = 0.6  # 60% of savings from incremental
        table_benefit = 0.35  # 35% from tables
        view_benefit = 0.05  # 5% from views
        
        breakdown = {
            'by_materialization': {
                'incremental': {
                    'model_count': int(pipeline_config['num_models'] * materialization['incremental'] / 100),
                    'savings_contribution': round(total_savings * incremental_benefit * (materialization['incremental'] / 100) / 0.3, 2),
                    'benefit_factor': 'High - Only processes new/changed data',
                },
                'table': {
                    'model_count': int(pipeline_config['num_models'] * materialization['table'] / 100),
                    'savings_contribution': round(total_savings * table_benefit * (materialization['table'] / 100) / 0.4, 2),
                    'benefit_factor': 'Medium - Can be skipped entirely when upstream unchanged',
                },
                'view': {
                    'model_count': int(pipeline_config['num_models'] * materialization['view'] / 100),
                    'savings_contribution': round(total_savings * view_benefit * (materialization['view'] / 100) / 0.3, 2),
                    'benefit_factor': 'Low - Always refresh but minimal compute',
                },
            },
            'by_frequency': {
                'runs_per_day': pipeline_config['runs_per_day'],
                'savings_per_run': round(total_savings / (pipeline_config['runs_per_day'] * 30), 2),
                'optimal_frequency': self._calculate_optimal_frequency(
                    pipeline_config['source_change_rate'],
                    pipeline_config['runs_per_day']
                ),
            },
            'optimization_opportunities': self._identify_optimizations(
                pipeline_config,
                simulation_results
            ),
        }
        
        return breakdown
    
    def _calculate_optimal_frequency(
        self,
        source_change_rate: float,
        current_frequency: int
    ) -> str:
        """Suggest optimal run frequency based on source change patterns."""
        # If sources change infrequently, running less often may be better
        if source_change_rate < 20:
            if current_frequency > 12:
                return f"Consider reducing to {current_frequency // 2} runs/day - low source change rate"
        elif source_change_rate > 60:
            if current_frequency < 12:
                return f"Current frequency optimal for high change rate"
        
        return "Current frequency appears optimal for your change patterns"
    
    def _identify_optimizations(
        self,
        pipeline_config: Dict[str, Any],
        simulation_results: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """Identify additional optimization opportunities."""
        optimizations = []
        
        materialization = pipeline_config['materialization']
        
        # Check incremental model percentage
        if materialization['incremental'] < 20:
            optimizations.append({
                'category': 'Materialization',
                'recommendation': 'Consider converting more tables to incremental models',
                'potential_impact': 'High',
                'details': f"Only {materialization['incremental']}% of models are incremental. "
                          "Incremental models see the greatest benefit from SAO."
            })
        
        # Check DAG depth
        if pipeline_config['dag_depth'] < 3:
            optimizations.append({
                'category': 'DAG Structure',
                'recommendation': 'Consider adding intermediate transformation layers',
                'potential_impact': 'Medium',
                'details': "Shallow DAGs have less opportunity for SAO optimization. "
                          "More intermediate models = more skip opportunities."
            })
        
        # Check run frequency vs change rate
        if pipeline_config['runs_per_day'] > 24 and pipeline_config['source_change_rate'] < 30:
            optimizations.append({
                'category': 'Run Frequency',
                'recommendation': 'Consider reducing pipeline run frequency',
                'potential_impact': 'Medium',
                'details': f"Running {pipeline_config['runs_per_day']}x/day with only "
                          f"{pipeline_config['source_change_rate']}% source change rate. "
                          "Many runs may have no new data to process."
            })
        
        # Check if skip rate is low
        if simulation_results['skip_rate'] < 30:
            optimizations.append({
                'category': 'Source Tracking',
                'recommendation': 'Ensure source freshness checks are configured',
                'potential_impact': 'High',
                'details': f"Skip rate of {simulation_results['skip_rate']}% is lower than expected. "
                          "Verify that dbt source freshness is properly configured."
            })
        
        return optimizations


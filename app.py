"""
SAO Cost Savings Estimator - Streamlit App
==========================================
Helps dbt Labs prospects estimate cost savings from State Aware Orchestration (SAO)
by simulating their pipeline in a sandbox environment.
"""

import sys
import os

# Add the app directory to Python path for Snowflake compatibility
app_dir = os.path.dirname(os.path.abspath(__file__))
if app_dir not in sys.path:
    sys.path.insert(0, app_dir)

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import json

from modules.dbt_api import DBTCloudAPI
from modules.data_generator import SyntheticDataGenerator
from modules.project_generator import DBTProjectGenerator
from modules.cost_calculator import SAOCostCalculator
from modules.visualizations import create_cost_comparison_chart, create_sao_impact_chart

# Page Configuration
st.set_page_config(
    page_title="SAO Cost Savings Estimator",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Outfit:wght@300;400;600;700&display=swap');
    
    .stApp {
        background: linear-gradient(135deg, #0f0f23 0%, #1a1a3e 50%, #0f0f23 100%);
    }
    
    .main-header {
        font-family: 'Outfit', sans-serif;
        font-size: 3rem;
        font-weight: 700;
        background: linear-gradient(90deg, #FF694A 0%, #FF8F6B 50%, #FFB347 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    
    .sub-header {
        font-family: 'Outfit', sans-serif;
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
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
    }
    
    .metric-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 2.5rem;
        font-weight: 600;
        color: #FF694A;
    }
    
    .metric-label {
        font-family: 'Outfit', sans-serif;
        color: #8080a0;
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 1px;
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
        font-family: 'JetBrains Mono', monospace;
        font-size: 4rem;
        font-weight: 700;
        color: #4ade80;
    }
    
    .section-divider {
        height: 2px;
        background: linear-gradient(90deg, transparent, #3a3a6a, transparent);
        margin: 2rem 0;
    }
    
    .input-section {
        background: rgba(30, 30, 63, 0.5);
        border-radius: 12px;
        padding: 1.5rem;
        border: 1px solid #3a3a6a;
    }
    
    .stButton > button {
        background: linear-gradient(90deg, #FF694A 0%, #FF8F6B 100%);
        color: white;
        font-family: 'Outfit', sans-serif;
        font-weight: 600;
        border: none;
        border-radius: 8px;
        padding: 0.75rem 2rem;
        font-size: 1.1rem;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(255, 105, 74, 0.4);
    }
    
    .status-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    
    .status-pending {
        background: #3a3a6a;
        color: #a0a0c0;
    }
    
    .status-running {
        background: #3a5a8a;
        color: #60a5fa;
    }
    
    .status-complete {
        background: #2a5a4a;
        color: #4ade80;
    }
</style>
""", unsafe_allow_html=True)


def init_session_state():
    """Initialize session state variables."""
    defaults = {
        'step': 1,
        'pipeline_config': None,
        'synthetic_data_generated': False,
        'dbt_project_created': False,
        'jobs_created': False,
        'jobs_run': False,
        'results': None,
        'dbt_api': None,
        'data_generator': None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def render_header():
    """Render the app header."""
    st.markdown('<h1 class="main-header">🔄 SAO Cost Savings Estimator</h1>', unsafe_allow_html=True)
    st.markdown(
        '<p class="sub-header">Estimate your savings from dbt Fusion\'s State Aware Orchestration</p>',
        unsafe_allow_html=True
    )


def render_sidebar():
    """Render the sidebar with configuration options."""
    with st.sidebar:
        st.markdown("### 🔑 API Configuration")
        
        # dbt Cloud Configuration
        st.markdown("#### dbt Cloud")
        dbt_account_id = st.text_input(
            "Account ID",
            help="Your dbt Cloud account ID"
        )
        dbt_api_token = st.text_input(
            "API Token",
            type="password",
            help="Your dbt Cloud API token"
        )
        sandbox_project_id = st.text_input(
            "Sandbox Project ID",
            help="The dbt Cloud project ID for the sandbox environment"
        )
        
        st.markdown("---")
        
        # OpenAI Configuration for Data Designer
        st.markdown("#### Data Generation")
        openai_api_key = st.text_input(
            "OpenAI API Key",
            type="password",
            help="Required for NVIDIA Data Designer synthetic data generation"
        )
        
        st.markdown("---")
        
        # Snowflake Configuration
        st.markdown("#### Snowflake Connection")
        sf_account = st.text_input("Account", help="Snowflake account identifier")
        sf_warehouse = st.text_input("Warehouse", value="COMPUTE_WH")
        sf_database = st.text_input("Database", value="SAO_SANDBOX")
        sf_schema = st.text_input("Schema", value="PUBLIC")
        
        # Store configuration
        if st.button("💾 Save Configuration"):
            st.session_state['config'] = {
                'dbt': {
                    'account_id': dbt_account_id,
                    'api_token': dbt_api_token,
                    'sandbox_project_id': sandbox_project_id,
                },
                'openai': {
                    'api_key': openai_api_key,
                },
                'snowflake': {
                    'account': sf_account,
                    'warehouse': sf_warehouse,
                    'database': sf_database,
                    'schema': sf_schema,
                }
            }
            st.success("Configuration saved!")
        
        return {
            'dbt': {
                'account_id': dbt_account_id,
                'api_token': dbt_api_token,
                'sandbox_project_id': sandbox_project_id,
            },
            'openai': {
                'api_key': openai_api_key,
            },
            'snowflake': {
                'account': sf_account,
                'warehouse': sf_warehouse,
                'database': sf_database,
                'schema': sf_schema,
            }
        }


def render_pipeline_inputs():
    """Render the pipeline configuration input form."""
    st.markdown("### 📊 Pipeline Configuration")
    st.markdown("Tell us about your current dbt Core deployment:")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="input-section">', unsafe_allow_html=True)
        st.markdown("#### Model Structure")
        
        num_models = st.slider(
            "Total Number of Models",
            min_value=10,
            max_value=2000,
            value=200,
            step=10,
            help="Total number of dbt models in your project"
        )
        
        st.markdown("**Materialization Distribution**")
        incremental_pct = st.slider(
            "Incremental Models (%)",
            min_value=0,
            max_value=100,
            value=30,
            help="Percentage of models that are incremental"
        )
        
        remaining = 100 - incremental_pct
        table_pct = st.slider(
            "Table Models (%)",
            min_value=0,
            max_value=remaining,
            value=min(40, remaining),
            help="Percentage of models that are tables"
        )
        
        view_pct = remaining - table_pct
        st.info(f"View Models: {view_pct}%")
        
        num_sources = st.slider(
            "Number of Sources",
            min_value=1,
            max_value=100,
            value=15,
            help="Number of source tables/views feeding your dbt project"
        )
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="input-section">', unsafe_allow_html=True)
        st.markdown("#### Execution Patterns")
        
        source_refresh_frequency = st.selectbox(
            "Source Refresh Frequency",
            options=[
                "Real-time (streaming)",
                "Every 15 minutes",
                "Hourly",
                "Every 4 hours",
                "Daily",
                "Weekly"
            ],
            index=2,
            help="How often do your source tables get updated?"
        )
        
        pipeline_frequency = st.selectbox(
            "Pipeline Run Frequency",
            options=[
                "Every 15 minutes",
                "Hourly",
                "Every 4 hours",
                "Daily (once)",
                "Daily (multiple times)",
                "On-demand"
            ],
            index=1,
            help="How often do you run your dbt pipeline?"
        )
        
        runs_per_day = st.number_input(
            "Average Runs Per Day",
            min_value=1,
            max_value=96,
            value=24,
            help="Average number of pipeline runs per day"
        )
        
        st.markdown("#### Data Characteristics")
        
        avg_row_count = st.select_slider(
            "Average Table Row Count",
            options=["1K", "10K", "100K", "1M", "10M", "100M", "1B"],
            value="1M",
            help="Average number of rows in your tables"
        )
        
        avg_column_count = st.slider(
            "Average Column Count",
            min_value=5,
            max_value=200,
            value=30,
            help="Average number of columns per table"
        )
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Additional advanced options
    with st.expander("🔧 Advanced Configuration"):
        col3, col4 = st.columns(2)
        
        with col3:
            dag_depth = st.slider(
                "Average DAG Depth",
                min_value=2,
                max_value=15,
                value=5,
                help="Average depth of model dependencies"
            )
            
            avg_dependencies = st.slider(
                "Avg Dependencies per Model",
                min_value=1,
                max_value=10,
                value=3,
                help="Average number of upstream dependencies per model"
            )
        
        with col4:
            source_change_rate = st.slider(
                "Source Change Rate (%)",
                min_value=5,
                max_value=100,
                value=30,
                help="Percentage of sources that typically change between runs"
            )
            
            warehouse_size = st.selectbox(
                "Snowflake Warehouse Size",
                options=["X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large"],
                index=2,
                help="Your typical Snowflake warehouse size"
            )
            
            current_monthly_cost = st.number_input(
                "Current Monthly Compute Cost ($)",
                min_value=0,
                max_value=1000000,
                value=5000,
                help="Your current estimated monthly Snowflake compute cost for dbt"
            )
    
    # Convert row count to number
    row_count_map = {
        "1K": 1000,
        "10K": 10000,
        "100K": 100000,
        "1M": 1000000,
        "10M": 10000000,
        "100M": 100000000,
        "1B": 1000000000
    }
    
    return {
        'num_models': num_models,
        'materialization': {
            'incremental': incremental_pct,
            'table': table_pct,
            'view': view_pct,
        },
        'num_sources': num_sources,
        'source_refresh_frequency': source_refresh_frequency,
        'pipeline_frequency': pipeline_frequency,
        'runs_per_day': runs_per_day,
        'avg_row_count': row_count_map[avg_row_count],
        'avg_column_count': avg_column_count,
        'dag_depth': dag_depth,
        'avg_dependencies': avg_dependencies,
        'source_change_rate': source_change_rate,
        'warehouse_size': warehouse_size,
        'current_monthly_cost': current_monthly_cost,
    }


def render_simulation_step(config, pipeline_config):
    """Render the simulation execution step."""
    st.markdown("### 🚀 Pipeline Simulation")
    
    # Show configuration summary
    with st.expander("📋 Configuration Summary", expanded=True):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Models", pipeline_config['num_models'])
            st.metric("Sources", pipeline_config['num_sources'])
        
        with col2:
            st.metric("Runs/Day", pipeline_config['runs_per_day'])
            st.metric("DAG Depth", pipeline_config['dag_depth'])
        
        with col3:
            st.metric("Source Change Rate", f"{pipeline_config['source_change_rate']}%")
            st.metric("Current Monthly Cost", f"${pipeline_config['current_monthly_cost']:,}")
    
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    # Simulation steps
    steps = [
        ("1️⃣", "Generate Synthetic Data", "Creating realistic test data based on your specifications"),
        ("2️⃣", "Create dbt Project", "Building model structure matching your pipeline"),
        ("3️⃣", "Setup Sandbox Jobs", "Creating Core and Fusion comparison jobs"),
        ("4️⃣", "Execute Simulation", "Running both pipelines to measure differences"),
        ("5️⃣", "Calculate Savings", "Analyzing results and computing cost savings"),
    ]
    
    for icon, title, description in steps:
        col1, col2 = st.columns([1, 11])
        with col1:
            st.markdown(f"### {icon}")
        with col2:
            st.markdown(f"**{title}**")
            st.caption(description)
    
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    # Run simulation button
    if st.button("🎯 Run Full Simulation", type="primary", use_container_width=True):
        run_simulation(config, pipeline_config)


def run_simulation(config, pipeline_config):
    """Execute the full simulation pipeline."""
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        # Step 1: Generate Synthetic Data
        status_text.markdown("**Step 1/5:** Generating synthetic data...")
        progress_bar.progress(10)
        
        data_generator = SyntheticDataGenerator(
            openai_api_key=config['openai']['api_key']
        )
        
        synthetic_data = data_generator.generate_pipeline_data(
            num_sources=pipeline_config['num_sources'],
            avg_row_count=min(pipeline_config['avg_row_count'], 10000),  # Limit for demo
            avg_column_count=pipeline_config['avg_column_count']
        )
        
        st.session_state['synthetic_data'] = synthetic_data
        progress_bar.progress(20)
        
        # Step 2: Create dbt Project Structure
        status_text.markdown("**Step 2/5:** Creating dbt project structure...")
        
        project_generator = DBTProjectGenerator()
        project_config = project_generator.generate_project(
            num_models=pipeline_config['num_models'],
            materialization_dist=pipeline_config['materialization'],
            num_sources=pipeline_config['num_sources'],
            dag_depth=pipeline_config['dag_depth'],
            avg_dependencies=pipeline_config['avg_dependencies'],
            source_schemas=list(synthetic_data.keys())
        )
        
        st.session_state['project_config'] = project_config
        progress_bar.progress(40)
        
        # Step 3: Setup dbt Cloud Jobs (if API configured)
        status_text.markdown("**Step 3/5:** Setting up sandbox jobs...")
        
        if config['dbt']['api_token'] and config['dbt']['account_id']:
            dbt_api = DBTCloudAPI(
                account_id=config['dbt']['account_id'],
                api_token=config['dbt']['api_token']
            )
            
            # Create jobs would happen here
            st.session_state['jobs_created'] = True
        else:
            st.warning("dbt Cloud API not configured - using simulation mode")
        
        progress_bar.progress(60)
        
        # Step 4: Simulate Execution (or run actual jobs)
        status_text.markdown("**Step 4/5:** Simulating pipeline execution...")
        
        calculator = SAOCostCalculator()
        simulation_results = calculator.simulate_pipeline_runs(
            pipeline_config=pipeline_config,
            num_simulation_runs=pipeline_config['runs_per_day']
        )
        
        st.session_state['simulation_results'] = simulation_results
        progress_bar.progress(80)
        
        # Step 5: Calculate Savings
        status_text.markdown("**Step 5/5:** Calculating cost savings...")
        
        savings_analysis = calculator.calculate_savings(
            simulation_results=simulation_results,
            warehouse_size=pipeline_config['warehouse_size'],
            current_monthly_cost=pipeline_config['current_monthly_cost']
        )
        
        st.session_state['savings_analysis'] = savings_analysis
        st.session_state['simulation_complete'] = True
        progress_bar.progress(100)
        
        status_text.markdown("✅ **Simulation complete!**")
        st.rerun()
        
    except Exception as e:
        st.error(f"Simulation error: {str(e)}")
        progress_bar.empty()


def render_results():
    """Render the simulation results and savings analysis."""
    st.markdown("### 📈 SAO Impact Analysis")
    
    savings = st.session_state.get('savings_analysis', {})
    simulation = st.session_state.get('simulation_results', {})
    
    # Main savings highlight
    st.markdown(f"""
    <div class="savings-highlight">
        <p class="metric-label">Estimated Monthly Savings with SAO</p>
        <p class="savings-amount">${savings.get('monthly_savings', 0):,.0f}</p>
        <p style="color: #a0a0c0;">
            {savings.get('savings_percentage', 0):.1f}% reduction in compute costs
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Detailed metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
            <p class="metric-label">Models Skipped (Avg)</p>
            <p class="metric-value">{}</p>
        </div>
        """.format(simulation.get('avg_models_skipped', 0)), unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
            <p class="metric-label">Execution Time Saved</p>
            <p class="metric-value">{}%</p>
        </div>
        """.format(simulation.get('time_saved_percentage', 0)), unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="metric-card">
            <p class="metric-label">Annual Savings</p>
            <p class="metric-value">${:,.0f}</p>
        </div>
        """.format(savings.get('annual_savings', 0)), unsafe_allow_html=True)
    
    with col4:
        st.markdown("""
        <div class="metric-card">
            <p class="metric-label">ROI Estimate</p>
            <p class="metric-value">{}x</p>
        </div>
        """.format(savings.get('roi_estimate', 0)), unsafe_allow_html=True)
    
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    # Visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Cost Comparison")
        fig1 = create_cost_comparison_chart(savings)
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        st.markdown("#### Model Execution Analysis")
        fig2 = create_sao_impact_chart(simulation)
        st.plotly_chart(fig2, use_container_width=True)
    
    # Detailed breakdown
    with st.expander("📊 Detailed Analysis"):
        st.markdown("#### Run-by-Run Comparison")
        
        if 'run_details' in simulation:
            df = pd.DataFrame(simulation['run_details'])
            st.dataframe(df, use_container_width=True)
        
        st.markdown("#### Assumptions & Methodology")
        st.markdown("""
        **SAO Cost Savings Calculation:**
        
        1. **Model Skip Rate**: Based on source change rate and DAG structure
        2. **Compute Savings**: Proportional to models skipped × model complexity
        3. **Warehouse Costs**: Snowflake credit rates by warehouse size
        4. **Incremental Benefit**: Incremental models benefit most from SAO
        
        **Key Factors Affecting Savings:**
        - Higher source change rates = Lower savings (more models need to run)
        - Deeper DAGs = Higher savings (more downstream models can be skipped)
        - More incremental models = Higher savings (incremental models track state)
        """)
    
    # Export options
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📥 Export Report (PDF)"):
            st.info("PDF export coming soon!")
    
    with col2:
        if st.button("📊 Export Data (CSV)"):
            if 'run_details' in simulation:
                csv = pd.DataFrame(simulation['run_details']).to_csv(index=False)
                st.download_button(
                    "Download CSV",
                    csv,
                    "sao_analysis.csv",
                    "text/csv"
                )
    
    with col3:
        if st.button("🔄 Run New Simulation"):
            st.session_state['simulation_complete'] = False
            st.rerun()


def main():
    """Main application entry point."""
    init_session_state()
    render_header()
    config = render_sidebar()
    
    # Main content
    tab1, tab2, tab3 = st.tabs(["📝 Configure Pipeline", "🚀 Run Simulation", "📈 Results"])
    
    with tab1:
        pipeline_config = render_pipeline_inputs()
        st.session_state['pipeline_config'] = pipeline_config
        
        if st.button("Continue to Simulation →", type="primary"):
            st.session_state['step'] = 2
    
    with tab2:
        if st.session_state.get('pipeline_config'):
            render_simulation_step(config, st.session_state['pipeline_config'])
        else:
            st.warning("Please configure your pipeline first in the 'Configure Pipeline' tab.")
    
    with tab3:
        if st.session_state.get('simulation_complete'):
            render_results()
        else:
            st.info("Run a simulation to see results here.")
            
            # Show demo results option
            if st.button("🎮 Load Demo Results"):
                # Load sample demo results
                st.session_state['simulation_results'] = {
                    'avg_models_skipped': 142,
                    'time_saved_percentage': 68,
                    'run_details': [
                        {'run': i, 'models_run_core': 200, 'models_run_fusion': 200 - int(142 * (0.8 + 0.4 * (i % 3) / 3)), 
                         'time_core_sec': 300, 'time_fusion_sec': 300 * (1 - 0.68 * (0.8 + 0.2 * (i % 5) / 5))}
                        for i in range(1, 25)
                    ]
                }
                st.session_state['savings_analysis'] = {
                    'monthly_savings': 3200,
                    'annual_savings': 38400,
                    'savings_percentage': 64,
                    'roi_estimate': 8.5,
                    'cost_before': 5000,
                    'cost_after': 1800,
                }
                st.session_state['simulation_complete'] = True
                st.rerun()


if __name__ == "__main__":
    main()


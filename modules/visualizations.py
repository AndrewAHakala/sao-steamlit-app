"""
Visualization Module
====================
Charts and graphs for SAO cost savings analysis.
"""

import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any


# Color scheme
COLORS = {
    'primary': '#FF694A',       # dbt orange
    'secondary': '#FF8F6B',
    'accent': '#FFB347',
    'success': '#4ade80',
    'info': '#60a5fa',
    'background': '#1e1e3f',
    'surface': '#2a2a5a',
    'text': '#ffffff',
    'text_muted': '#a0a0c0',
}


def create_cost_comparison_chart(savings_analysis: Dict[str, Any]) -> go.Figure:
    """
    Create a cost comparison bar chart.
    
    Args:
        savings_analysis: Savings data from calculator
        
    Returns:
        Plotly figure
    """
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
    
    # Before bars
    fig.add_trace(go.Bar(
        name='Without SAO (dbt Core)',
        x=categories,
        y=before,
        marker_color=COLORS['text_muted'],
        text=[f'${v:,.0f}' for v in before],
        textposition='outside',
    ))
    
    # After bars
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
        title=dict(
            text='Cost Comparison: Core vs Fusion',
            font=dict(color=COLORS['text'], size=16),
        ),
        xaxis=dict(
            title='',
            tickfont=dict(color=COLORS['text']),
        ),
        yaxis=dict(
            title='Cost (USD)',
            tickfont=dict(color=COLORS['text']),
            titlefont=dict(color=COLORS['text']),
            gridcolor='rgba(255,255,255,0.1)',
        ),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1,
            font=dict(color=COLORS['text']),
        ),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=80, b=40),
    )
    
    return fig


def create_sao_impact_chart(simulation_results: Dict[str, Any]) -> go.Figure:
    """
    Create a chart showing SAO impact on model execution.
    
    Args:
        simulation_results: Simulation data
        
    Returns:
        Plotly figure
    """
    run_details = simulation_results.get('run_details', [])
    
    if not run_details:
        # Return empty figure
        return go.Figure()
    
    runs = [r['run'] for r in run_details]
    core_models = [r['models_run_core'] for r in run_details]
    fusion_models = [r['models_run_fusion'] for r in run_details]
    
    fig = go.Figure()
    
    # Core execution (full)
    fig.add_trace(go.Scatter(
        x=runs,
        y=core_models,
        mode='lines+markers',
        name='dbt Core (All Models)',
        line=dict(color=COLORS['text_muted'], width=2),
        marker=dict(size=6),
    ))
    
    # Fusion execution (optimized)
    fig.add_trace(go.Scatter(
        x=runs,
        y=fusion_models,
        mode='lines+markers',
        name='dbt Fusion (SAO Optimized)',
        line=dict(color=COLORS['primary'], width=2),
        marker=dict(size=6),
        fill='tonexty',
        fillcolor='rgba(255,105,74,0.2)',
    ))
    
    fig.update_layout(
        title=dict(
            text='Models Executed per Run',
            font=dict(color=COLORS['text'], size=16),
        ),
        xaxis=dict(
            title='Run Number',
            tickfont=dict(color=COLORS['text']),
            titlefont=dict(color=COLORS['text']),
            gridcolor='rgba(255,255,255,0.1)',
        ),
        yaxis=dict(
            title='Models Executed',
            tickfont=dict(color=COLORS['text']),
            titlefont=dict(color=COLORS['text']),
            gridcolor='rgba(255,255,255,0.1)',
        ),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1,
            font=dict(color=COLORS['text']),
        ),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=80, b=40),
        hovermode='x unified',
    )
    
    return fig


def create_savings_breakdown_chart(breakdown: Dict[str, Any]) -> go.Figure:
    """
    Create a pie chart showing savings breakdown by materialization.
    
    Args:
        breakdown: Savings breakdown data
        
    Returns:
        Plotly figure
    """
    by_mat = breakdown.get('by_materialization', {})
    
    labels = []
    values = []
    colors = [COLORS['primary'], COLORS['secondary'], COLORS['accent']]
    
    for mat_type, data in by_mat.items():
        labels.append(mat_type.capitalize())
        values.append(data.get('savings_contribution', 0))
    
    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=values,
        hole=0.4,
        marker_colors=colors,
        textinfo='label+percent',
        textfont=dict(color=COLORS['text']),
    )])
    
    fig.update_layout(
        title=dict(
            text='Savings by Materialization Type',
            font=dict(color=COLORS['text'], size=16),
        ),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        showlegend=True,
        legend=dict(
            font=dict(color=COLORS['text']),
        ),
        margin=dict(t=60, b=40),
    )
    
    return fig


def create_time_comparison_chart(simulation_results: Dict[str, Any]) -> go.Figure:
    """
    Create a chart comparing execution times.
    
    Args:
        simulation_results: Simulation data
        
    Returns:
        Plotly figure
    """
    run_details = simulation_results.get('run_details', [])
    
    if not run_details:
        return go.Figure()
    
    runs = [r['run'] for r in run_details]
    core_time = [r['time_core_sec'] for r in run_details]
    fusion_time = [r['time_fusion_sec'] for r in run_details]
    
    fig = go.Figure()
    
    # Core time
    fig.add_trace(go.Bar(
        name='dbt Core',
        x=runs,
        y=core_time,
        marker_color=COLORS['text_muted'],
    ))
    
    # Fusion time
    fig.add_trace(go.Bar(
        name='dbt Fusion',
        x=runs,
        y=fusion_time,
        marker_color=COLORS['success'],
    ))
    
    fig.update_layout(
        barmode='overlay',
        title=dict(
            text='Execution Time per Run (seconds)',
            font=dict(color=COLORS['text'], size=16),
        ),
        xaxis=dict(
            title='Run Number',
            tickfont=dict(color=COLORS['text']),
            titlefont=dict(color=COLORS['text']),
        ),
        yaxis=dict(
            title='Time (seconds)',
            tickfont=dict(color=COLORS['text']),
            titlefont=dict(color=COLORS['text']),
            gridcolor='rgba(255,255,255,0.1)',
        ),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1,
            font=dict(color=COLORS['text']),
        ),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=80, b=40),
        bargap=0.1,
    )
    
    return fig


def create_dag_visualization(dag_structure: Dict[str, Any]) -> go.Figure:
    """
    Create a simple DAG visualization.
    
    Args:
        dag_structure: DAG nodes and edges
        
    Returns:
        Plotly figure
    """
    nodes = dag_structure.get('nodes', [])
    edges = dag_structure.get('edges', [])
    
    # Simple layout: layers by type
    node_positions = {}
    layer_counts = {'source': 0, 'staging': 0, 'intermediate': 0, 'marts': 0}
    
    for node in nodes:
        layer = node.get('layer', node.get('type', 'source'))
        if layer == 'source':
            x = 0
        elif layer == 'staging':
            x = 1
        elif layer == 'intermediate':
            x = 2
        else:
            x = 3
        
        y = layer_counts.get(layer, 0)
        layer_counts[layer] = y + 1
        node_positions[node['id']] = (x, y)
    
    # Create edge traces
    edge_x = []
    edge_y = []
    for edge in edges:
        if edge['source'] in node_positions and edge['target'] in node_positions:
            x0, y0 = node_positions[edge['source']]
            x1, y1 = node_positions[edge['target']]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
    
    fig = go.Figure()
    
    # Edges
    fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y,
        mode='lines',
        line=dict(width=1, color='rgba(255,255,255,0.3)'),
        hoverinfo='none',
    ))
    
    # Nodes by type
    for layer, color in [
        ('source', COLORS['info']),
        ('staging', COLORS['secondary']),
        ('intermediate', COLORS['accent']),
        ('marts', COLORS['success']),
    ]:
        layer_nodes = [n for n in nodes if n.get('layer', n.get('type')) == layer]
        if layer_nodes:
            x_vals = [node_positions[n['id']][0] for n in layer_nodes if n['id'] in node_positions]
            y_vals = [node_positions[n['id']][1] for n in layer_nodes if n['id'] in node_positions]
            names = [n['name'] for n in layer_nodes if n['id'] in node_positions]
            
            fig.add_trace(go.Scatter(
                x=x_vals, y=y_vals,
                mode='markers+text',
                marker=dict(size=20, color=color),
                text=names,
                textposition='top center',
                textfont=dict(size=8, color=COLORS['text']),
                name=layer.capitalize(),
            ))
    
    fig.update_layout(
        title=dict(
            text='Pipeline DAG Structure',
            font=dict(color=COLORS['text'], size=16),
        ),
        showlegend=True,
        legend=dict(font=dict(color=COLORS['text'])),
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            showticklabels=False,
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            showticklabels=False,
        ),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=60, b=20, l=20, r=20),
    )
    
    return fig


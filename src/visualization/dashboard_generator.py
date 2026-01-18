"""
Dashboard Generator for Vehicle Telemetry Analytics Platform.
Creates interactive dashboards for data visualization and monitoring.
"""
import logging
import json
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime, timedelta
import dash
from dash import dcc, html, Input, Output, State, callback_context
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import ipywidgets as widgets
from IPython.display import display, HTML
import folium
from folium.plugins import HeatMap, MarkerCluster
import altair as alt
import bokeh.plotting as bk
from bokeh.models import HoverTool, ColumnDataSource
from bokeh.layouts import gridplot
from bokeh.io import output_file, show
import streamlit as st
import warnings

warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)


class DashboardTheme:
    """Dashboard theme configuration."""
    
    def __init__(self, theme_name: str = 'dark'):
        self.theme_name = theme_name
        self.colors = self._get_theme_colors(theme_name)
        self.font_family = 'Arial, sans-serif'
        self.font_size = 12
        self.title_size = 20
        self.margin = dict(l=50, r=50, t=50, b=50)
    
    def _get_theme_colors(self, theme_name: str) -> Dict[str, Any]:
        """Get color palette for theme."""
        themes = {
            'dark': {
                'background': '#1e1e1e',
                'text': '#ffffff',
                'grid': '#404040',
                'primary': '#1f77b4',
                'secondary': '#ff7f0e',
                'success': '#2ca02c',
                'warning': '#d62728',
                'error': '#e74c3c',
                'sequential': px.colors.sequential.Viridis,
                'diverging': px.colors.diverging.RdBu,
                'categorical': px.colors.qualitative.Set3
            },
            'light': {
                'background': '#ffffff',
                'text': '#000000',
                'grid': '#d3d3d3',
                'primary': '#1f77b4',
                'secondary': '#ff7f0e',
                'success': '#2ca02c',
                'warning': '#d62728',
                'error': '#e74c3c',
                'sequential': px.colors.sequential.Blues,
                'diverging': px.colors.diverging.RdBu,
                'categorical': px.colors.qualitative.Set3
            },
            'corporate': {
                'background': '#f8f9fa',
                'text': '#212529',
                'grid': '#dee2e6',
                'primary': '#007bff',
                'secondary': '#6c757d',
                'success': '#28a745',
                'warning': '#ffc107',
                'error': '#dc3545',
                'sequential': ['#007bff', '#0056b3', '#003d80'],
                'diverging': ['#dc3545', '#ffc107', '#28a745'],
                'categorical': ['#007bff', '#28a745', '#ffc107', '#dc3545', '#6c757d']
            }
        }
        
        return themes.get(theme_name, themes['dark'])
    
    def apply_theme(self, fig: go.Figure) -> go.Figure:
        """Apply theme to Plotly figure."""
        fig.update_layout(
            plot_bgcolor=self.colors['background'],
            paper_bgcolor=self.colors['background'],
            font_color=self.colors['text'],
            font_family=self.font_family,
            font_size=self.font_size,
            title_font_size=self.title_size,
            margin=self.margin,
            xaxis=dict(
                gridcolor=self.colors['grid'],
                zerolinecolor=self.colors['grid']
            ),
            yaxis=dict(
                gridcolor=self.colors['grid'],
                zerolinecolor=self.colors['grid']
            )
        )
        return fig


class BaseDashboard:
    """Base class for all dashboards."""
    
    def __init__(self, title: str, data: pd.DataFrame, theme: DashboardTheme = None):
        self.title = title
        self.data = data
        self.theme = theme or DashboardTheme('dark')
        self.figures = {}
        self.widgets = {}
        self.metrics = {}
        
    def calculate_metrics(self) -> Dict[str, Any]:
        """Calculate dashboard metrics."""
        raise NotImplementedError
    
    def create_figures(self) -> Dict[str, go.Figure]:
        """Create dashboard figures."""
        raise NotImplementedError
    
    def generate_html(self) -> str:
        """Generate HTML representation of dashboard."""
        raise NotImplementedError
    
    def save(self, filepath: str, format: str = 'html'):
        """Save dashboard to file."""
        raise NotImplementedError


class TelemetryDashboard(BaseDashboard):
    """Telemetry data dashboard."""
    
    def __init__(self, data: pd.DataFrame, vehicle_id: str = None, theme: DashboardTheme = None):
        title = f"Telemetry Dashboard - {vehicle_id}" if vehicle_id else "Telemetry Dashboard"
        super().__init__(title, data, theme)
        self.vehicle_id = vehicle_id
        self.calculate_metrics()
        self.create_figures()
    
    def calculate_metrics(self) -> Dict[str, Any]:
        """Calculate telemetry metrics."""
        if self.data.empty:
            return {}
        
        data = self.data.copy()
        
        # Convert timestamp if needed
        if 'timestamp' in data.columns and not pd.api.types.is_datetime64_any_dtype(data['timestamp']):
            data['timestamp'] = pd.to_datetime(data['timestamp'])
        
        # Calculate basic metrics
        metrics = {
            'vehicle_id': self.vehicle_id or 'Multiple',
            'data_points': len(data),
            'time_range': {
                'start': data['timestamp'].min() if 'timestamp' in data.columns else None,
                'end': data['timestamp'].max() if 'timestamp' in data.columns else None,
                'duration': (data['timestamp'].max() - data['timestamp'].min()).total_seconds() / 3600 
                           if 'timestamp' in data.columns else None
            }
        }
        
        # Numeric metrics
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        
        for col in numeric_columns:
            if col in data.columns:
                metrics[f'{col}_avg'] = float(data[col].mean())
                metrics[f'{col}_min'] = float(data[col].min())
                metrics[f'{col}_max'] = float(data[col].max())
                metrics[f'{col}_std'] = float(data[col].std())
        
        # Special calculations for telemetry columns
        if 'speed_kmh' in data.columns:
            metrics['avg_speed'] = float(data['speed_kmh'].mean())
            metrics['max_speed'] = float(data['speed_kmh'].max())
            metrics['speeding_count'] = int((data['speed_kmh'] > 120).sum())
        
        if 'engine_temperature_c' in data.columns:
            metrics['avg_engine_temp'] = float(data['engine_temperature_c'].mean())
            metrics['overheating_count'] = int((data['engine_temperature_c'] > 120).sum())
        
        if 'fuel_level_percent' in data.columns:
            metrics['avg_fuel_level'] = float(data['fuel_level_percent'].mean())
            metrics['low_fuel_count'] = int((data['fuel_level_percent'] < 15).sum())
        
        # Calculate distance if coordinates available
        if all(col in data.columns for col in ['latitude', 'longitude']):
            # Simple distance calculation (Haversine)
            from math import radians, sin, cos, sqrt, atan2
            
            coords = data[['latitude', 'longitude']].dropna()
            if len(coords) > 1:
                distances = []
                for i in range(1, len(coords)):
                    lat1, lon1 = radians(coords.iloc[i-1]['latitude']), radians(coords.iloc[i-1]['longitude'])
                    lat2, lon2 = radians(coords.iloc[i]['latitude']), radians(coords.iloc[i]['longitude'])
                    
                    dlon = lon2 - lon1
                    dlat = lat2 - lat1
                    
                    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
                    c = 2 * atan2(sqrt(a), sqrt(1-a))
                    distances.append(6371 * c)  # Earth radius in km
                
                metrics['total_distance_km'] = float(sum(distances))
                metrics['avg_speed_calculated'] = metrics['total_distance_km'] / (metrics['time_range']['duration'] or 1)
        
        self.metrics = metrics
        return metrics
    
    def create_figures(self) -> Dict[str, go.Figure]:
        """Create telemetry dashboard figures."""
        figures = {}
        
        # Time series of key metrics
        figures['speed_time_series'] = self._create_speed_time_series()
        figures['engine_temp_time_series'] = self._create_engine_temp_time_series()
        figures['fuel_level_time_series'] = self._create_fuel_level_time_series()
        figures['rpm_time_series'] = self._create_rpm_time_series()
        
        # Distribution plots
        figures['speed_distribution'] = self._create_speed_distribution()
        figures['engine_temp_distribution'] = self._create_engine_temp_distribution()
        
        # Correlation matrix
        figures['correlation_heatmap'] = self._create_correlation_heatmap()
        
        # Geographical map
        if all(col in self.data.columns for col in ['latitude', 'longitude']):
            figures['location_map'] = self._create_location_map()
        
        # Gauge charts for current values
        figures['current_metrics_gauges'] = self._create_current_metrics_gauges()
        
        self.figures = figures
        return figures
    
    def _create_speed_time_series(self) -> go.Figure:
        """Create speed time series plot."""
        if 'speed_kmh' not in self.data.columns or 'timestamp' not in self.data.columns:
            return go.Figure()
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=self.data['timestamp'],
            y=self.data['speed_kmh'],
            mode='lines',
            name='Speed',
            line=dict(color=self.theme.colors['primary'], width=2),
            hovertemplate='Time: %{x}<br>Speed: %{y} km/h<extra></extra>'
        ))
        
        # Add threshold line
        fig.add_hline(
            y=120,
            line_dash="dash",
            line_color=self.theme.colors['warning'],
            annotation_text="Speed Limit",
            annotation_position="bottom right"
        )
        
        fig.update_layout(
            title='Speed Over Time',
            xaxis_title='Time',
            yaxis_title='Speed (km/h)',
            hovermode='x unified'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_engine_temp_time_series(self) -> go.Figure:
        """Create engine temperature time series plot."""
        if 'engine_temperature_c' not in self.data.columns or 'timestamp' not in self.data.columns:
            return go.Figure()
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=self.data['timestamp'],
            y=self.data['engine_temperature_c'],
            mode='lines',
            name='Engine Temperature',
            line=dict(color=self.theme.colors['error'], width=2),
            hovertemplate='Time: %{x}<br>Temperature: %{y}°C<extra></extra>'
        ))
        
        # Add threshold lines
        fig.add_hline(
            y=100,
            line_dash="dash",
            line_color=self.theme.colors['warning'],
            annotation_text="Warning",
            annotation_position="bottom right"
        )
        
        fig.add_hline(
            y=120,
            line_dash="dot",
            line_color=self.theme.colors['error'],
            annotation_text="Critical",
            annotation_position="bottom right"
        )
        
        fig.update_layout(
            title='Engine Temperature Over Time',
            xaxis_title='Time',
            yaxis_title='Temperature (°C)',
            hovermode='x unified'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_fuel_level_time_series(self) -> go.Figure:
        """Create fuel level time series plot."""
        if 'fuel_level_percent' not in self.data.columns or 'timestamp' not in self.data.columns:
            return go.Figure()
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=self.data['timestamp'],
            y=self.data['fuel_level_percent'],
            mode='lines+markers',
            name='Fuel Level',
            line=dict(color=self.theme.colors['success'], width=2),
            marker=dict(size=4),
            hovertemplate='Time: %{x}<br>Fuel: %{y}%<extra></extra>'
        ))
        
        # Add threshold line
        fig.add_hline(
            y=15,
            line_dash="dash",
            line_color=self.theme.colors['warning'],
            annotation_text="Low Fuel",
            annotation_position="bottom right"
        )
        
        fig.update_layout(
            title='Fuel Level Over Time',
            xaxis_title='Time',
            yaxis_title='Fuel Level (%)',
            hovermode='x unified'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_rpm_time_series(self) -> go.Figure:
        """Create RPM time series plot."""
        if 'rpm' not in self.data.columns or 'timestamp' not in self.data.columns:
            return go.Figure()
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=self.data['timestamp'],
            y=self.data['rpm'],
            mode='lines',
            name='RPM',
            line=dict(color=self.theme.colors['secondary'], width=2),
            hovertemplate='Time: %{x}<br>RPM: %{y}<extra></extra>'
        ))
        
        fig.update_layout(
            title='Engine RPM Over Time',
            xaxis_title='Time',
            yaxis_title='RPM',
            hovermode='x unified'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_speed_distribution(self) -> go.Figure:
        """Create speed distribution histogram."""
        if 'speed_kmh' not in self.data.columns:
            return go.Figure()
        
        fig = px.histogram(
            self.data,
            x='speed_kmh',
            nbins=50,
            title='Speed Distribution',
            labels={'speed_kmh': 'Speed (km/h)'},
            color_discrete_sequence=[self.theme.colors['primary']]
        )
        
        fig.update_layout(
            xaxis_title='Speed (km/h)',
            yaxis_title='Count',
            bargap=0.1
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_engine_temp_distribution(self) -> go.Figure:
        """Create engine temperature distribution histogram."""
        if 'engine_temperature_c' not in self.data.columns:
            return go.Figure()
        
        fig = px.histogram(
            self.data,
            x='engine_temperature_c',
            nbins=30,
            title='Engine Temperature Distribution',
            labels={'engine_temperature_c': 'Temperature (°C)'},
            color_discrete_sequence=[self.theme.colors['error']]
        )
        
        fig.update_layout(
            xaxis_title='Temperature (°C)',
            yaxis_title='Count',
            bargap=0.1
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_correlation_heatmap(self) -> go.Figure:
        """Create correlation heatmap for telemetry data."""
        # Select numeric columns
        numeric_data = self.data.select_dtypes(include=[np.number])
        
        if len(numeric_data.columns) < 2:
            return go.Figure()
        
        # Calculate correlation matrix
        correlation_matrix = numeric_data.corr()
        
        fig = go.Figure(data=go.Heatmap(
            z=correlation_matrix.values,
            x=correlation_matrix.columns,
            y=correlation_matrix.columns,
            colorscale=self.theme.colors['sequential'],
            zmin=-1,
            zmax=1,
            colorbar=dict(title='Correlation'),
            text=correlation_matrix.values.round(2),
            texttemplate='%{text}',
            textfont={"size": 10}
        ))
        
        fig.update_layout(
            title='Telemetry Data Correlation Matrix',
            xaxis_title='Features',
            yaxis_title='Features',
            width=600,
            height=600
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_location_map(self) -> go.Figure:
        """Create geographical location map."""
        if not all(col in self.data.columns for col in ['latitude', 'longitude']):
            return go.Figure()
        
        # Create scatter map
        fig = px.scatter_mapbox(
            self.data,
            lat='latitude',
            lon='longitude',
            color='speed_kmh' if 'speed_kmh' in self.data.columns else None,
            size='speed_kmh' if 'speed_kmh' in self.data.columns else None,
            hover_data=['timestamp', 'speed_kmh'] if 'timestamp' in self.data.columns else None,
            title='Vehicle Location Track',
            color_continuous_scale=self.theme.colors['sequential'],
            zoom=10
        )
        
        fig.update_layout(
            mapbox_style="carto-darkmatter" if self.theme.theme_name == 'dark' else "carto-positron",
            height=500,
            margin={"r":0,"t":0,"l":0,"b":0}
        )
        
        return fig
    
    def _create_current_metrics_gauges(self) -> go.Figure:
        """Create gauge charts for current metrics."""
        # Get latest values
        latest_data = self.data.iloc[-1] if not self.data.empty else {}
        
        # Create subplots for gauges
        fig = make_subplots(
            rows=2, cols=2,
            specs=[[{'type': 'indicator'}, {'type': 'indicator'}],
                   [{'type': 'indicator'}, {'type': 'indicator'}]],
            subplot_titles=('Speed', 'Engine Temp', 'Fuel Level', 'RPM')
        )
        
        # Speed gauge
        if 'speed_kmh' in latest_data:
            fig.add_trace(
                go.Indicator(
                    mode="gauge+number",
                    value=float(latest_data['speed_kmh']),
                    title={'text': "Speed (km/h)"},
                    gauge={
                        'axis': {'range': [0, 200]},
                        'bar': {'color': self.theme.colors['primary']},
                        'steps': [
                            {'range': [0, 80], 'color': self.theme.colors['success']},
                            {'range': [80, 120], 'color': self.theme.colors['warning']},
                            {'range': [120, 200], 'color': self.theme.colors['error']}
                        ]
                    }
                ),
                row=1, col=1
            )
        
        # Engine temperature gauge
        if 'engine_temperature_c' in latest_data:
            fig.add_trace(
                go.Indicator(
                    mode="gauge+number",
                    value=float(latest_data['engine_temperature_c']),
                    title={'text': "Engine Temp (°C)"},
                    gauge={
                        'axis': {'range': [0, 150]},
                        'bar': {'color': self.theme.colors['error']},
                        'steps': [
                            {'range': [0, 90], 'color': self.theme.colors['success']},
                            {'range': [90, 120], 'color': self.theme.colors['warning']},
                            {'range': [120, 150], 'color': self.theme.colors['error']}
                        ]
                    }
                ),
                row=1, col=2
            )
        
        # Fuel level gauge
        if 'fuel_level_percent' in latest_data:
            fig.add_trace(
                go.Indicator(
                    mode="gauge+number",
                    value=float(latest_data['fuel_level_percent']),
                    title={'text': "Fuel Level (%)"},
                    gauge={
                        'axis': {'range': [0, 100]},
                        'bar': {'color': self.theme.colors['success']},
                        'steps': [
                            {'range': [0, 15], 'color': self.theme.colors['error']},
                            {'range': [15, 30], 'color': self.theme.colors['warning']},
                            {'range': [30, 100], 'color': self.theme.colors['success']}
                        ]
                    }
                ),
                row=2, col=1
            )
        
        # RPM gauge
        if 'rpm' in latest_data:
            fig.add_trace(
                go.Indicator(
                    mode="gauge+number",
                    value=float(latest_data['rpm']),
                    title={'text': "RPM"},
                    gauge={
                        'axis': {'range': [0, 8000]},
                        'bar': {'color': self.theme.colors['secondary']}
                    }
                ),
                row=2, col=2
            )
        
        fig.update_layout(
            height=600,
            title_text="Current Vehicle Metrics"
        )
        
        return self.theme.apply_theme(fig)
    
    def generate_html(self) -> str:
        """Generate HTML dashboard."""
        import jinja2
        
        # Create template
        template_str = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>{{ title }}</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body {
                    font-family: {{ theme.font_family }};
                    background-color: {{ theme.colors.background }};
                    color: {{ theme.colors.text }};
                    margin: 0;
                    padding: 20px;
                }
                .dashboard-header {
                    text-align: center;
                    margin-bottom: 30px;
                    padding-bottom: 20px;
                    border-bottom: 2px solid {{ theme.colors.primary }};
                }
                .metrics-container {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 20px;
                    margin-bottom: 30px;
                }
                .metric-card {
                    background-color: rgba(255, 255, 255, 0.1);
                    border-radius: 8px;
                    padding: 15px;
                    text-align: center;
                }
                .metric-value {
                    font-size: 24px;
                    font-weight: bold;
                    color: {{ theme.colors.primary }};
                }
                .metric-label {
                    font-size: 14px;
                    color: {{ theme.colors.text }};
                    opacity: 0.8;
                }
                .charts-container {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(600px, 1fr));
                    gap: 30px;
                }
                .chart {
                    background-color: rgba(255, 255, 255, 0.05);
                    border-radius: 8px;
                    padding: 20px;
                }
            </style>
        </head>
        <body>
            <div class="dashboard-header">
                <h1>{{ title }}</h1>
                <p>Generated on {{ generated_date }}</p>
            </div>
            
            {% if metrics %}
            <div class="metrics-container">
                {% for key, value in metrics.items() if key not in ['time_range', 'vehicle_id'] %}
                <div class="metric-card">
                    <div class="metric-value">
                        {% if value is number %}
                            {{ value|round(2) }}
                        {% else %}
                            {{ value }}
                        {% endif %}
                    </div>
                    <div class="metric-label">{{ key|replace('_', ' ')|title }}</div>
                </div>
                {% endfor %}
            </div>
            {% endif %}
            
            <div class="charts-container">
                {% for chart_name, chart_html in charts.items() %}
                <div class="chart">
                    <h3>{{ chart_name|replace('_', ' ')|title }}</h3>
                    {{ chart_html }}
                </div>
                {% endfor %}
            </div>
        </body>
        </html>
        """
        
        # Convert figures to HTML
        charts_html = {}
        for name, fig in self.figures.items():
            if fig and hasattr(fig, 'to_html'):
                charts_html[name] = fig.to_html(full_html=False, include_plotlyjs=False)
        
        # Render template
        template = jinja2.Template(template_str)
        html_content = template.render(
            title=self.title,
            theme=self.theme,
            metrics=self.metrics,
            charts=charts_html,
            generated_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        
        return html_content
    
    def save(self, filepath: str, format: str = 'html'):
        """Save dashboard to file."""
        if format == 'html':
            html_content = self.generate_html()
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logger.info(f"Dashboard saved to {filepath}")
        
        elif format == 'png':
            # Save each figure as PNG
            import os
            base_name = os.path.splitext(filepath)[0]
            
            for name, fig in self.figures.items():
                if fig:
                    fig.write_image(f"{base_name}_{name}.png")
            
            logger.info(f"Dashboard figures saved as PNG to {base_name}_*.png")
        
        else:
            raise ValueError(f"Unsupported format: {format}")


class PerformanceDashboard(TelemetryDashboard):
    """Performance analysis dashboard."""
    
    def __init__(self, data: pd.DataFrame, vehicle_id: str = None, theme: DashboardTheme = None):
        super().__init__(data, vehicle_id, theme)
        self.title = f"Performance Dashboard - {vehicle_id}" if vehicle_id else "Performance Dashboard"
    
    def create_figures(self) -> Dict[str, go.Figure]:
        """Create performance dashboard figures."""
        figures = super().create_figures()
        
        # Add performance-specific figures
        figures['acceleration_analysis'] = self._create_acceleration_analysis()
        figures['braking_analysis'] = self._create_braking_analysis()
        figures['cornering_analysis'] = self._create_cornering_analysis()
        figures['performance_summary'] = self._create_performance_summary()
        
        return figures
    
    def _create_acceleration_analysis(self) -> go.Figure:
        """Create acceleration analysis plot."""
        if 'acceleration_x' not in self.data.columns or 'timestamp' not in self.data.columns:
            return go.Figure()
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=self.data['timestamp'],
            y=self.data['acceleration_x'],
            mode='lines',
            name='Longitudinal Acceleration',
            line=dict(color=self.theme.colors['primary'], width=2),
            hovertemplate='Time: %{x}<br>Acceleration: %{y} m/s²<extra></extra>'
        ))
        
        fig.update_layout(
            title='Acceleration Analysis',
            xaxis_title='Time',
            yaxis_title='Acceleration (m/s²)',
            hovermode='x unified'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_braking_analysis(self) -> go.Figure:
        """Create braking analysis plot."""
        if 'brake_pressure_bar' not in self.data.columns or 'timestamp' not in self.data.columns:
            return go.Figure()
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=self.data['timestamp'],
            y=self.data['brake_pressure_bar'],
            mode='lines',
            name='Brake Pressure',
            line=dict(color=self.theme.colors['error'], width=2),
            hovertemplate='Time: %{x}<br>Brake Pressure: %{y} bar<extra></extra>'
        ))
        
        fig.update_layout(
            title='Braking Analysis',
            xaxis_title='Time',
            yaxis_title='Brake Pressure (bar)',
            hovermode='x unified'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_cornering_analysis(self) -> go.Figure:
        """Create cornering analysis plot."""
        if 'acceleration_y' not in self.data.columns or 'timestamp' not in self.data.columns:
            return go.Figure()
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=self.data['timestamp'],
            y=self.data['acceleration_y'].abs(),
            mode='lines',
            name='Lateral Acceleration',
            line=dict(color=self.theme.colors['secondary'], width=2),
            hovertemplate='Time: %{x}<br>Lateral Acc: %{y} m/s²<extra></extra>'
        ))
        
        fig.update_layout(
            title='Cornering Analysis',
            xaxis_title='Time',
            yaxis_title='Lateral Acceleration (m/s²)',
            hovermode='x unified'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_performance_summary(self) -> go.Figure:
        """Create performance summary radar chart."""
        # Calculate performance metrics
        metrics = {}
        
        if 'acceleration_x' in self.data.columns:
            metrics['Max Acceleration'] = float(self.data['acceleration_x'].max())
            metrics['Avg Acceleration'] = float(self.data['acceleration_x'].mean())
        
        if 'brake_pressure_bar' in self.data.columns:
            metrics['Max Braking'] = float(self.data['brake_pressure_bar'].max())
            metrics['Avg Braking'] = float(self.data['brake_pressure_bar'].mean())
        
        if 'acceleration_y' in self.data.columns:
            metrics['Max Cornering'] = float(self.data['acceleration_y'].abs().max())
            metrics['Avg Cornering'] = float(self.data['acceleration_y'].abs().mean())
        
        if 'speed_kmh' in self.data.columns:
            metrics['Top Speed'] = float(self.data['speed_kmh'].max())
            metrics['Avg Speed'] = float(self.data['speed_kmh'].mean())
        
        if not metrics:
            return go.Figure()
        
        # Normalize metrics for radar chart
        categories = list(metrics.keys())
        values = list(metrics.values())
        
        # Create radar chart
        fig = go.Figure(data=go.Scatterpolar(
            r=values,
            theta=categories,
            fill='toself',
            fillcolor='rgba(31, 119, 180, 0.3)',
            line=dict(color=self.theme.colors['primary'], width=2),
            name='Performance'
        ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, max(values) * 1.1]
                )
            ),
            showlegend=False,
            title='Performance Summary'
        )
        
        return self.theme.apply_theme(fig)


class MaintenanceDashboard(BaseDashboard):
    """Maintenance and fault analysis dashboard."""
    
    def __init__(self, telemetry_data: pd.DataFrame, maintenance_data: pd.DataFrame, 
                 fault_data: pd.DataFrame = None, theme: DashboardTheme = None):
        super().__init__("Maintenance Dashboard", telemetry_data, theme)
        self.maintenance_data = maintenance_data
        self.fault_data = fault_data
        self.calculate_metrics()
        self.create_figures()
    
    def calculate_metrics(self) -> Dict[str, Any]:
        """Calculate maintenance metrics."""
        metrics = {}
        
        # Maintenance metrics
        if not self.maintenance_data.empty:
            metrics['total_maintenance'] = len(self.maintenance_data)
            metrics['avg_maintenance_cost'] = float(self.maintenance_data['cost'].mean()) if 'cost' in self.maintenance_data.columns else 0
            metrics['total_maintenance_cost'] = float(self.maintenance_data['cost'].sum()) if 'cost' in self.maintenance_data.columns else 0
            
            if 'maintenance_type' in self.maintenance_data.columns:
                maintenance_types = self.maintenance_data['maintenance_type'].value_counts().to_dict()
                metrics['maintenance_by_type'] = maintenance_types
        
        # Fault metrics
        if self.fault_data is not None and not self.fault_data.empty:
            metrics['total_faults'] = len(self.fault_data)
            metrics['active_faults'] = int(self.fault_data['resolved_at'].isna().sum()) if 'resolved_at' in self.fault_data.columns else 0
            
            if 'severity' in self.fault_data.columns:
                fault_severities = self.fault_data['severity'].value_counts().to_dict()
                metrics['faults_by_severity'] = fault_severities
            
            if 'component' in self.fault_data.columns:
                fault_components = self.fault_data['component'].value_counts().to_dict()
                metrics['faults_by_component'] = fault_components
        
        # Telemetry-based maintenance indicators
        if not self.data.empty:
            if 'engine_temperature_c' in self.data.columns:
                metrics['overheating_events'] = int((self.data['engine_temperature_c'] > 120).sum())
            
            if 'oil_pressure_kpa' in self.data.columns:
                metrics['low_oil_pressure_events'] = int((self.data['oil_pressure_kpa'] < 150).sum())
            
            if 'battery_voltage' in self.data.columns:
                metrics['low_battery_events'] = int((self.data['battery_voltage'] < 12.0).sum())
        
        self.metrics = metrics
        return metrics
    
    def create_figures(self) -> Dict[str, go.Figure]:
        """Create maintenance dashboard figures."""
        figures = {}
        
        figures['maintenance_timeline'] = self._create_maintenance_timeline()
        figures['maintenance_cost_analysis'] = self._create_maintenance_cost_analysis()
        
        if self.fault_data is not None:
            figures['fault_timeline'] = self._create_fault_timeline()
            figures['fault_severity'] = self._create_fault_severity_chart()
            figures['fault_components'] = self._create_fault_components_chart()
        
        figures['predictive_maintenance'] = self._create_predictive_maintenance_chart()
        figures['maintenance_recommendations'] = self._create_maintenance_recommendations()
        
        return figures
    
    def _create_maintenance_timeline(self) -> go.Figure:
        """Create maintenance timeline."""
        if self.maintenance_data.empty or 'maintenance_date' not in self.maintenance_data.columns:
            return go.Figure()
        
        # Convert date column
        data = self.maintenance_data.copy()
        data['maintenance_date'] = pd.to_datetime(data['maintenance_date'])
        
        fig = go.Figure()
        
        # Group by maintenance type
        if 'maintenance_type' in data.columns:
            maintenance_types = data['maintenance_type'].unique()
            
            for maint_type in maintenance_types:
                type_data = data[data['maintenance_type'] == maint_type]
                
                fig.add_trace(go.Scatter(
                    x=type_data['maintenance_date'],
                    y=[maint_type] * len(type_data),
                    mode='markers',
                    name=maint_type,
                    marker=dict(size=10),
                    text=type_data['description'] if 'description' in type_data.columns else '',
                    hovertemplate='Date: %{x}<br>Type: %{y}<br>Description: %{text}<extra></extra>'
                ))
        else:
            fig.add_trace(go.Scatter(
                x=data['maintenance_date'],
                y=['Maintenance'] * len(data),
                mode='markers',
                name='Maintenance',
                marker=dict(size=10, color=self.theme.colors['primary']),
                hovertemplate='Date: %{x}<extra></extra>'
            ))
        
        fig.update_layout(
            title='Maintenance Timeline',
            xaxis_title='Date',
            yaxis_title='Maintenance Type',
            hovermode='closest'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_maintenance_cost_analysis(self) -> go.Figure:
        """Create maintenance cost analysis."""
        if self.maintenance_data.empty or 'cost' not in self.maintenance_data.columns:
            return go.Figure()
        
        data = self.maintenance_data.copy()
        
        # Group by maintenance type
        if 'maintenance_type' in data.columns:
            cost_by_type = data.groupby('maintenance_type')['cost'].agg(['sum', 'mean', 'count']).reset_index()
            
            fig = make_subplots(
                rows=1, cols=2,
                subplot_titles=('Total Cost by Type', 'Average Cost by Type')
            )
            
            fig.add_trace(
                go.Bar(
                    x=cost_by_type['maintenance_type'],
                    y=cost_by_type['sum'],
                    name='Total Cost',
                    marker_color=self.theme.colors['primary']
                ),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Bar(
                    x=cost_by_type['maintenance_type'],
                    y=cost_by_type['mean'],
                    name='Average Cost',
                    marker_color=self.theme.colors['secondary']
                ),
                row=1, col=2
            )
            
            fig.update_layout(
                title='Maintenance Cost Analysis',
                showlegend=False
            )
        else:
            # Simple cost distribution
            fig = px.histogram(
                data,
                x='cost',
                nbins=20,
                title='Maintenance Cost Distribution',
                labels={'cost': 'Cost'},
                color_discrete_sequence=[self.theme.colors['primary']]
            )
        
        return self.theme.apply_theme(fig)
    
    def _create_fault_timeline(self) -> go.Figure:
        """Create fault timeline."""
        if self.fault_data is None or self.fault_data.empty or 'timestamp' not in self.fault_data.columns:
            return go.Figure()
        
        data = self.fault_data.copy()
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        
        fig = go.Figure()
        
        # Color by severity if available
        if 'severity' in data.columns:
            severities = data['severity'].unique()
            colors = {
                'low': self.theme.colors['success'],
                'medium': self.theme.colors['warning'],
                'high': self.theme.colors['error'],
                'critical': '#ff0000'
            }
            
            for severity in severities:
                severity_data = data[data['severity'] == severity]
                color = colors.get(severity.lower(), self.theme.colors['primary'])
                
                fig.add_trace(go.Scatter(
                    x=severity_data['timestamp'],
                    y=[severity] * len(severity_data),
                    mode='markers',
                    name=severity.capitalize(),
                    marker=dict(size=12, color=color, symbol='diamond'),
                    text=severity_data['fault_description'] if 'fault_description' in severity_data.columns else '',
                    hovertemplate='Date: %{x}<br>Severity: %{y}<br>Description: %{text}<extra></extra>'
                ))
        else:
            fig.add_trace(go.Scatter(
                x=data['timestamp'],
                y=['Fault'] * len(data),
                mode='markers',
                name='Faults',
                marker=dict(size=10, color=self.theme.colors['error'], symbol='x'),
                hovertemplate='Date: %{x}<extra></extra>'
            ))
        
        fig.update_layout(
            title='Fault Timeline',
            xaxis_title='Date',
            yaxis_title='Fault Severity',
            hovermode='closest'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_fault_severity_chart(self) -> go.Figure:
        """Create fault severity distribution."""
        if self.fault_data is None or self.fault_data.empty or 'severity' not in self.fault_data.columns:
            return go.Figure()
        
        severity_counts = self.fault_data['severity'].value_counts()
        
        fig = go.Figure(data=[go.Pie(
            labels=severity_counts.index,
            values=severity_counts.values,
            hole=.3,
            marker_colors=[self.theme.colors['success'], self.theme.colors['warning'], 
                          self.theme.colors['error'], '#ff0000']
        )])
        
        fig.update_layout(
            title='Fault Severity Distribution'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_fault_components_chart(self) -> go.Figure:
        """Create fault components chart."""
        if self.fault_data is None or self.fault_data.empty or 'component' not in self.fault_data.columns:
            return go.Figure()
        
        component_counts = self.fault_data['component'].value_counts().head(10)
        
        fig = go.Figure(data=[go.Bar(
            x=component_counts.values,
            y=component_counts.index,
            orientation='h',
            marker_color=self.theme.colors['primary']
        )])
        
        fig.update_layout(
            title='Top 10 Faulty Components',
            xaxis_title='Number of Faults',
            yaxis_title='Component'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_predictive_maintenance_chart(self) -> go.Figure:
        """Create predictive maintenance chart."""
        # This is a simplified version - in reality you would use ML models
        if self.data.empty:
            return go.Figure()
        
        # Calculate health indicators
        health_data = {}
        
        if 'engine_temperature_c' in self.data.columns:
            avg_temp = self.data['engine_temperature_c'].mean()
            health_data['Engine Temperature'] = max(0, 100 - (avg_temp - 90) * 2)  # Simplified health score
        
        if 'oil_pressure_kpa' in self.data.columns:
            avg_pressure = self.data['oil_pressure_kpa'].mean()
            health_data['Oil Pressure'] = max(0, 100 - (200 - avg_pressure) * 0.5)
        
        if 'battery_voltage' in self.data.columns:
            avg_voltage = self.data['battery_voltage'].mean()
            health_data['Battery'] = max(0, 100 - (13.5 - avg_voltage) * 20)
        
        if 'tire_pressure_front_left' in self.data.columns:
            avg_tire_pressure = self.data[['tire_pressure_front_left', 'tire_pressure_front_right',
                                         'tire_pressure_rear_left', 'tire_pressure_rear_right']].mean().mean()
            health_data['Tires'] = max(0, 100 - abs(35 - avg_tire_pressure) * 5)  # Assuming 35 psi optimal
        
        if not health_data:
            return go.Figure()
        
        # Create radar chart
        categories = list(health_data.keys())
        values = list(health_data.values())
        
        fig = go.Figure(data=go.Scatterpolar(
            r=values,
            theta=categories,
            fill='toself',
            fillcolor='rgba(46, 204, 113, 0.3)',
            line=dict(color=self.theme.colors['success'], width=2),
            name='Component Health'
        ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 100]
                )
            ),
            showlegend=False,
            title='Predictive Maintenance - Component Health'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_maintenance_recommendations(self) -> go.Figure:
        """Create maintenance recommendations table as a figure."""
        recommendations = []
        
        # Generate recommendations based on metrics
        if 'overheating_events' in self.metrics and self.metrics['overheating_events'] > 10:
            recommendations.append({
                'component': 'Cooling System',
                'issue': 'Frequent overheating events detected',
                'recommendation': 'Check coolant level, radiator, and thermostat',
                'priority': 'High'
            })
        
        if 'low_oil_pressure_events' in self.metrics and self.metrics['low_oil_pressure_events'] > 5:
            recommendations.append({
                'component': 'Engine Oil System',
                'issue': 'Low oil pressure events detected',
                'recommendation': 'Check oil level and oil pump',
                'priority': 'High'
            })
        
        if 'low_battery_events' in self.metrics and self.metrics['low_battery_events'] > 3:
            recommendations.append({
                'component': 'Electrical System',
                'issue': 'Low battery voltage events',
                'recommendation': 'Test battery and alternator',
                'priority': 'Medium'
            })
        
        if not recommendations:
            # Add generic recommendation if none specific
            recommendations.append({
                'component': 'General',
                'issue': 'No critical issues detected',
                'recommendation': 'Continue regular maintenance schedule',
                'priority': 'Low'
            })
        
        # Create table
        fig = go.Figure(data=[go.Table(
            header=dict(
                values=['Component', 'Issue', 'Recommendation', 'Priority'],
                fill_color=self.theme.colors['background'],
                align='left',
                font=dict(color=self.theme.colors['text'], size=12)
            ),
            cells=dict(
                values=[[r['component'] for r in recommendations],
                       [r['issue'] for r in recommendations],
                       [r['recommendation'] for r in recommendations],
                       [r['priority'] for r in recommendations]],
                fill_color='rgba(255, 255, 255, 0.05)',
                align='left',
                font=dict(color=self.theme.colors['text'], size=11)
            )
        )])
        
        fig.update_layout(
            title='Maintenance Recommendations',
            height=200 + len(recommendations) * 30
        )
        
        return self.theme.apply_theme(fig)


class FleetOverviewDashboard(BaseDashboard):
    """Fleet overview dashboard for multiple vehicles."""
    
    def __init__(self, fleet_data: pd.DataFrame, theme: DashboardTheme = None):
        super().__init__("Fleet Overview Dashboard", fleet_data, theme)
        self.calculate_metrics()
        self.create_figures()
    
    def calculate_metrics(self) -> Dict[str, Any]:
        """Calculate fleet metrics."""
        if self.data.empty:
            return {}
        
        metrics = {
            'total_vehicles': self.data['vehicle_id'].nunique() if 'vehicle_id' in self.data.columns else 0,
            'active_vehicles': self.data['status'].eq('active').sum() if 'status' in self.data.columns else 0,
            'total_distance': float(self.data['total_distance_km'].sum()) if 'total_distance_km' in self.data.columns else 0,
            'avg_distance_per_vehicle': float(self.data['total_distance_km'].mean()) if 'total_distance_km' in self.data.columns else 0
        }
        
        # Vehicle age statistics
        if 'year' in self.data.columns:
            current_year = datetime.now().year
            self.data['vehicle_age'] = current_year - self.data['year']
            metrics['avg_vehicle_age'] = float(self.data['vehicle_age'].mean())
            metrics['oldest_vehicle'] = int(self.data['vehicle_age'].max())
            metrics['newest_vehicle'] = int(self.data['vehicle_age'].min())
        
        # Fleet composition
        if 'make' in self.data.columns:
            fleet_by_make = self.data['make'].value_counts().to_dict()
            metrics['fleet_by_make'] = fleet_by_make
        
        if 'fuel_type' in self.data.columns:
            fleet_by_fuel = self.data['fuel_type'].value_counts().to_dict()
            metrics['fleet_by_fuel_type'] = fleet_by_fuel
        
        self.metrics = metrics
        return metrics
    
    def create_figures(self) -> Dict[str, go.Figure]:
        """Create fleet overview figures."""
        figures = {}
        
        figures['fleet_composition'] = self._create_fleet_composition()
        figures['distance_distribution'] = self._create_distance_distribution()
        figures['vehicle_age_distribution'] = self._create_vehicle_age_distribution()
        figures['fleet_status'] = self._create_fleet_status()
        figures['fleet_efficiency'] = self._create_fleet_efficiency()
        
        return figures
    
    def _create_fleet_composition(self) -> go.Figure:
        """Create fleet composition chart."""
        if 'make' not in self.data.columns:
            return go.Figure()
        
        make_counts = self.data['make'].value_counts()
        
        fig = go.Figure(data=[go.Pie(
            labels=make_counts.index,
            values=make_counts.values,
            hole=.3,
            marker_colors=self.theme.colors['categorical']
        )])
        
        fig.update_layout(
            title='Fleet Composition by Make'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_distance_distribution(self) -> go.Figure:
        """Create distance distribution chart."""
        if 'total_distance_km' not in self.data.columns:
            return go.Figure()
        
        fig = px.histogram(
            self.data,
            x='total_distance_km',
            nbins=20,
            title='Vehicle Distance Distribution',
            labels={'total_distance_km': 'Total Distance (km)'},
            color_discrete_sequence=[self.theme.colors['primary']]
        )
        
        fig.update_layout(
            xaxis_title='Total Distance (km)',
            yaxis_title='Number of Vehicles'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_vehicle_age_distribution(self) -> go.Figure:
        """Create vehicle age distribution chart."""
        if 'year' not in self.data.columns:
            return go.Figure()
        
        current_year = datetime.now().year
        self.data['vehicle_age'] = current_year - self.data['year']
        
        fig = px.histogram(
            self.data,
            x='vehicle_age',
            nbins=15,
            title='Vehicle Age Distribution',
            labels={'vehicle_age': 'Vehicle Age (years)'},
            color_discrete_sequence=[self.theme.colors['secondary']]
        )
        
        fig.update_layout(
            xaxis_title='Vehicle Age (years)',
            yaxis_title='Number of Vehicles'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_fleet_status(self) -> go.Figure:
        """Create fleet status chart."""
        if 'status' not in self.data.columns:
            return go.Figure()
        
        status_counts = self.data['status'].value_counts()
        
        fig = go.Figure(data=[go.Bar(
            x=status_counts.index,
            y=status_counts.values,
            marker_color=[self.theme.colors['success'] if status == 'active' else 
                         self.theme.colors['warning'] if status == 'maintenance' else 
                         self.theme.colors['error'] for status in status_counts.index]
        )])
        
        fig.update_layout(
            title='Fleet Status',
            xaxis_title='Status',
            yaxis_title='Number of Vehicles'
        )
        
        return self.theme.apply_theme(fig)
    
    def _create_fleet_efficiency(self) -> go.Figure:
        """Create fleet efficiency comparison."""
        # This would require fuel consumption data
        # For now, create a placeholder
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=['Vehicle 1', 'Vehicle 2', 'Vehicle 3', 'Vehicle 4', 'Vehicle 5'],
            y=[12.5, 11.8, 13.2, 10.5, 14.1],
            name='Fuel Efficiency',
            marker_color=self.theme.colors['primary']
        ))
        
        fig.update_layout(
            title='Fleet Fuel Efficiency Comparison',
            xaxis_title='Vehicle',
            yaxis_title='Fuel Efficiency (km/L)'
        )
        
        return self.theme.apply_theme(fig)


class DashboardGenerator:
    """Main dashboard generator class."""
    
    def __init__(self, theme: str = 'dark'):
        self.theme = DashboardTheme(theme)
        self.dashboards = {}
    
    def create_telemetry_dashboard(self, data: pd.DataFrame, vehicle_id: str = None) -> TelemetryDashboard:
        """Create telemetry dashboard."""
        dashboard = TelemetryDashboard(data, vehicle_id, self.theme)
        self.dashboards[f'telemetry_{vehicle_id or "all"}'] = dashboard
        return dashboard
    
    def create_performance_dashboard(self, data: pd.DataFrame, vehicle_id: str = None) -> PerformanceDashboard:
        """Create performance dashboard."""
        dashboard = PerformanceDashboard(data, vehicle_id, self.theme)
        self.dashboards[f'performance_{vehicle_id or "all"}'] = dashboard
        return dashboard
    
    def create_maintenance_dashboard(self, telemetry_data: pd.DataFrame, 
                                   maintenance_data: pd.DataFrame, 
                                   fault_data: pd.DataFrame = None) -> MaintenanceDashboard:
        """Create maintenance dashboard."""
        dashboard = MaintenanceDashboard(telemetry_data, maintenance_data, fault_data, self.theme)
        self.dashboards['maintenance'] = dashboard
        return dashboard
    
    def create_fleet_dashboard(self, fleet_data: pd.DataFrame) -> FleetOverviewDashboard:
        """Create fleet overview dashboard."""
        dashboard = FleetOverviewDashboard(fleet_data, self.theme)
        self.dashboards['fleet'] = dashboard
        return dashboard
    
    def generate_all_dashboards(self, output_dir: str = './dashboards'):
        """Generate all dashboards."""
        import os
        
        os.makedirs(output_dir, exist_ok=True)
        
        for name, dashboard in self.dashboards.items():
            filename = f"{output_dir}/{name}_dashboard.html"
            dashboard.save(filename)
            logger.info(f"Dashboard saved: {filename}")
    
    def create_interactive_dashboard(self) -> dash.Dash:
        """Create interactive Dash web application."""
        app = dash.Dash(
            __name__,
            external_stylesheets=[dbc.themes.BOOTSTRAP],
            suppress_callback_exceptions=True
        )
        
        # Dashboard layout
        app.layout = dbc.Container([
            dbc.Row([
                dbc.Col(html.H1("Vehicle Telemetry Analytics Dashboard", 
                               className="text-center mb-4"), width=12)
            ]),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Vehicle Selection"),
                        dbc.CardBody([
                            dcc.Dropdown(
                                id='vehicle-selector',
                                options=[{'label': 'All Vehicles', 'value': 'all'}] +
                                       [{'label': f'Vehicle {i}', 'value': f'VH{i:03d}'} 
                                        for i in range(1, 11)],
                                value='all',
                                clearable=False
                            ),
                            dcc.DatePickerRange(
                                id='date-range',
                                start_date=datetime.now() - timedelta(days=7),
                                end_date=datetime.now(),
                                display_format='YYYY-MM-DD'
                            )
                        ])
                    ], className="mb-4")
                ], width=3),
                
                dbc.Col([
                    dbc.Tabs([
                        dbc.Tab(label="Telemetry", tab_id="tab-telemetry"),
                        dbc.Tab(label="Performance", tab_id="tab-performance"),
                        dbc.Tab(label="Maintenance", tab_id="tab-maintenance"),
                        dbc.Tab(label="Fleet Overview", tab_id="tab-fleet"),
                    ], id="tabs", active_tab="tab-telemetry"),
                    
                    html.Div(id="tab-content", className="p-4")
                ], width=9)
            ])
        ], fluid=True)
        
        # Callbacks
        @app.callback(
            Output("tab-content", "children"),
            Input("tabs", "active_tab"),
            Input("vehicle-selector", "value"),
            Input("date-range", "start_date"),
            Input("date-range", "end_date")
        )
        def render_tab_content(active_tab, vehicle_id, start_date, end_date):
            if active_tab == "tab-telemetry":
                return self._create_telemetry_tab(vehicle_id, start_date, end_date)
            elif active_tab == "tab-performance":
                return self._create_performance_tab(vehicle_id, start_date, end_date)
            elif active_tab == "tab-maintenance":
                return self._create_maintenance_tab(start_date, end_date)
            elif active_tab == "tab-fleet":
                return self._create_fleet_tab()
            return html.Div("Select a tab")
        
        return app
    
    def _create_telemetry_tab(self, vehicle_id: str, start_date: str, end_date: str) -> dbc.Container:
        """Create telemetry tab content."""
        return dbc.Container([
            dbc.Row([
                dbc.Col(dcc.Graph(id='speed-graph'), width=6),
                dbc.Col(dcc.Graph(id='temp-graph'), width=6)
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id='fuel-graph'), width=6),
                dbc.Col(dcc.Graph(id='rpm-graph'), width=6)
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id='location-map'), width=12)
            ])
        ])
    
    def _create_performance_tab(self, vehicle_id: str, start_date: str, end_date: str) -> dbc.Container:
        """Create performance tab content."""
        return dbc.Container([
            dbc.Row([
                dbc.Col(dcc.Graph(id='acceleration-graph'), width=6),
                dbc.Col(dcc.Graph(id='braking-graph'), width=6)
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id='cornering-graph'), width=12)
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id='performance-summary'), width=12)
            ])
        ])
    
    def _create_maintenance_tab(self, start_date: str, end_date: str) -> dbc.Container:
        """Create maintenance tab content."""
        return dbc.Container([
            dbc.Row([
                dbc.Col(dcc.Graph(id='maintenance-timeline'), width=6),
                dbc.Col(dcc.Graph(id='fault-timeline'), width=6)
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id='maintenance-cost'), width=6),
                dbc.Col(dcc.Graph(id='fault-severity'), width=6)
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id='predictive-maintenance'), width=12)
            ])
        ])
    
    def _create_fleet_tab(self) -> dbc.Container:
        """Create fleet tab content."""
        return dbc.Container([
            dbc.Row([
                dbc.Col(dcc.Graph(id='fleet-composition'), width=6),
                dbc.Col(dcc.Graph(id='fleet-status'), width=6)
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id='distance-distribution'), width=6),
                dbc.Col(dcc.Graph(id='vehicle-age'), width=6)
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id='fleet-efficiency'), width=12)
            ])
        ])
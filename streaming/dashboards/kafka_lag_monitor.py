"""
Kafka Lag Monitoring Dashboard for Vehicle Telemetry Analytics Platform.
Monitors consumer lag, throughput, and Kafka cluster health in real-time.
"""
import logging
import json
import time
import asyncio
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from enum import Enum
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, Input, Output, State, callback_context
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import KafkaError
import threading
import queue
from collections import deque
import psutil
import humanize

logger = logging.getLogger(__name__)


class KafkaMetricType(Enum):
    """Kafka metric types."""
    CONSUMER_LAG = "consumer_lag"
    THROUGHPUT = "throughput"
    LATENCY = "latency"
    ERROR_RATE = "error_rate"
    BROKER_HEALTH = "broker_health"


@dataclass
class KafkaMetric:
    """Kafka metric data container."""
    metric_type: KafkaMetricType
    value: float
    timestamp: datetime
    tags: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'metric_type': self.metric_type.value,
            'value': self.value,
            'timestamp': self.timestamp.isoformat(),
            'tags': self.tags
        }


@dataclass
class ConsumerGroupLag:
    """Consumer group lag information."""
    group_id: str
    topic: str
    partition: int
    current_offset: int
    log_end_offset: int
    lag: int
    consumer_id: str = ""
    client_id: str = ""
    host: str = ""
    
    @property
    def lag_percentage(self) -> float:
        """Calculate lag as percentage of total messages."""
        if self.log_end_offset == 0:
            return 0.0
        return (self.lag / self.log_end_offset) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'group_id': self.group_id,
            'topic': self.topic,
            'partition': self.partition,
            'current_offset': self.current_offset,
            'log_end_offset': self.log_end_offset,
            'lag': self.lag,
            'lag_percentage': self.lag_percentage,
            'consumer_id': self.consumer_id,
            'client_id': self.client_id,
            'host': self.host
        }


class KafkaLagMonitor:
    """
    Monitors Kafka consumer lag and cluster health.
    """
    
    def __init__(self, bootstrap_servers: List[str], refresh_interval: int = 10):
        """
        Initialize Kafka lag monitor.
        
        Args:
            bootstrap_servers: List of Kafka bootstrap servers
            refresh_interval: Refresh interval in seconds
        """
        self.bootstrap_servers = bootstrap_servers
        self.refresh_interval = refresh_interval
        self.metrics_history = deque(maxlen=1000)
        self.consumer_lag_history = deque(maxlen=500)
        self.is_running = False
        self.monitor_thread = None
        self.consumer_groups = {}
        
        # Initialize Kafka clients
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id='kafka-lag-monitor'
            )
            logger.info("Kafka admin client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka admin client: {e}")
            self.admin_client = None
        
        # Metrics storage
        self.metrics = {
            'consumer_lag': {},
            'throughput': {},
            'latency': {},
            'error_rate': {},
            'broker_health': {}
        }
    
    def start_monitoring(self):
        """Start monitoring thread."""
        if self.is_running:
            logger.warning("Monitor is already running")
            return
        
        self.is_running = True
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            daemon=True
        )
        self.monitor_thread.start()
        logger.info("Kafka lag monitor started")
    
    def stop_monitoring(self):
        """Stop monitoring thread."""
        self.is_running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("Kafka lag monitor stopped")
    
    def _monitoring_loop(self):
        """Main monitoring loop."""
        while self.is_running:
            try:
                self._collect_metrics()
                time.sleep(self.refresh_interval)
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(self.refresh_interval)
    
    def _collect_metrics(self):
        """Collect all Kafka metrics."""
        try:
            # Get consumer groups
            consumer_groups = self._get_consumer_groups()
            
            # Get lag for each consumer group
            for group_id in consumer_groups:
                lag_info = self._get_consumer_group_lag(group_id)
                if lag_info:
                    self.consumer_groups[group_id] = lag_info
            
            # Calculate aggregated metrics
            self._calculate_aggregated_metrics()
            
            # Get broker health
            self._get_broker_health()
            
            # Get topic throughput
            self._get_topic_throughput()
            
            logger.debug(f"Collected metrics for {len(self.consumer_groups)} consumer groups")
            
        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
    
    def _get_consumer_groups(self) -> List[str]:
        """Get list of consumer groups."""
        try:
            if self.admin_client:
                groups = self.admin_client.list_consumer_groups()
                return [group[0] for group in groups]
        except Exception as e:
            logger.error(f"Error getting consumer groups: {e}")
        return []
    
    def _get_consumer_group_lag(self, group_id: str) -> List[ConsumerGroupLag]:
        """Get lag information for a consumer group."""
        lag_info = []
        
        try:
            # Create consumer for the group
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                enable_auto_commit=False,
                auto_offset_reset='earliest'
            )
            
            # Get topic partitions
            topics = consumer.topics()
            
            for topic in topics:
                partitions = consumer.partitions_for_topic(topic)
                if not partitions:
                    continue
                
                for partition in partitions:
                    try:
                        # Get current offset
                        tp = TopicPartition(topic, partition)
                        committed = consumer.committed(tp)
                        
                        # Get end offset
                        consumer.assign([tp])
                        consumer.seek_to_end(tp)
                        end_offset = consumer.position(tp)
                        
                        if committed is not None:
                            lag = end_offset - committed
                            lag_info.append(ConsumerGroupLag(
                                group_id=group_id,
                                topic=topic,
                                partition=partition,
                                current_offset=committed,
                                log_end_offset=end_offset,
                                lag=lag
                            ))
                    except Exception as e:
                        logger.debug(f"Error getting lag for {topic}:{partition}: {e}")
            
            consumer.close()
            
        except Exception as e:
            logger.error(f"Error getting lag for group {group_id}: {e}")
        
        return lag_info
    
    def _calculate_aggregated_metrics(self):
        """Calculate aggregated metrics from lag information."""
        total_lag = 0
        max_lag = 0
        lagging_groups = 0
        
        for group_id, lag_list in self.consumer_groups.items():
            group_lag = sum(lag.lag for lag in lag_list)
            total_lag += group_lag
            
            if group_lag > 0:
                lagging_groups += 1
            
            if group_lag > max_lag:
                max_lag = group_lag
        
        # Store metrics
        timestamp = datetime.now()
        
        self.metrics['consumer_lag'][timestamp] = {
            'total_lag': total_lag,
            'max_lag': max_lag,
            'lagging_groups': lagging_groups,
            'total_groups': len(self.consumer_groups)
        }
        
        # Add to history
        self.metrics_history.append(KafkaMetric(
            metric_type=KafkaMetricType.CONSUMER_LAG,
            value=total_lag,
            timestamp=timestamp,
            tags={'metric': 'total_lag'}
        ))
    
    def _get_broker_health(self):
        """Get broker health metrics."""
        try:
            if self.admin_client:
                # Get broker descriptions
                brokers = self.admin_client.describe_cluster()
                
                healthy_brokers = 0
                total_brokers = len(brokers.nodes())
                
                # Simple health check - try to connect to each broker
                for node in brokers.nodes():
                    try:
                        # Create a test connection
                        test_consumer = KafkaConsumer(
                            bootstrap_servers=f"{node.host}:{node.port}",
                            api_version=(2, 0, 0),
                            request_timeout_ms=5000
                        )
                        test_consumer.topics()
                        test_consumer.close()
                        healthy_brokers += 1
                    except:
                        pass
                
                health_percentage = (healthy_brokers / total_brokers * 100) if total_brokers > 0 else 0
                
                timestamp = datetime.now()
                self.metrics['broker_health'][timestamp] = {
                    'healthy_brokers': healthy_brokers,
                    'total_brokers': total_brokers,
                    'health_percentage': health_percentage
                }
                
                self.metrics_history.append(KafkaMetric(
                    metric_type=KafkaMetricType.BROKER_HEALTH,
                    value=health_percentage,
                    timestamp=timestamp,
                    tags={'metric': 'health_percentage'}
                ))
        
        except Exception as e:
            logger.error(f"Error getting broker health: {e}")
    
    def _get_topic_throughput(self):
        """Estimate topic throughput."""
        # This is a simplified version - in production, you'd use Kafka metrics
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='latest',
                enable_auto_commit=False
            )
            
            topics = consumer.topics()
            total_messages = 0
            
            for topic in topics:
                if topic.startswith('_'):
                    continue  # Skip internal topics
                
                partitions = consumer.partitions_for_topic(topic)
                if partitions:
                    for partition in partitions:
                        tp = TopicPartition(topic, partition)
                        consumer.assign([tp])
                        consumer.seek_to_end(tp)
                        end_offset = consumer.position(tp)
                        total_messages += end_offset
            
            consumer.close()
            
            timestamp = datetime.now()
            self.metrics['throughput'][timestamp] = {
                'total_messages': total_messages,
                'topic_count': len([t for t in topics if not t.startswith('_')])
            }
            
        except Exception as e:
            logger.error(f"Error getting topic throughput: {e}")
    
    def get_metrics_dataframe(self, metric_type: str) -> pd.DataFrame:
        """Get metrics as DataFrame."""
        if metric_type not in self.metrics:
            return pd.DataFrame()
        
        data = []
        for timestamp, values in self.metrics[metric_type].items():
            row = {'timestamp': timestamp}
            row.update(values)
            data.append(row)
        
        return pd.DataFrame(data)
    
    def get_lag_summary(self) -> Dict[str, Any]:
        """Get lag summary statistics."""
        total_lag = 0
        max_lag_group = None
        max_lag_value = 0
        
        for group_id, lag_list in self.consumer_groups.items():
            group_lag = sum(lag.lag for lag in lag_list)
            total_lag += group_lag
            
            if group_lag > max_lag_value:
                max_lag_value = group_lag
                max_lag_group = group_id
        
        return {
            'total_lag': total_lag,
            'max_lag': max_lag_value,
            'max_lag_group': max_lag_group,
            'lagging_groups': sum(1 for group in self.consumer_groups.values() 
                                 if sum(lag.lag for lag in group) > 0),
            'total_groups': len(self.consumer_groups)
        }
    
    def get_top_lagging_consumers(self, top_n: int = 10) -> List[Dict[str, Any]]:
        """Get top N lagging consumers."""
        group_lags = []
        
        for group_id, lag_list in self.consumer_groups.items():
            total_lag = sum(lag.lag for lag in lag_list)
            if total_lag > 0:
                group_lags.append({
                    'group_id': group_id,
                    'total_lag': total_lag,
                    'topic_count': len(set(lag.topic for lag in lag_list)),
                    'partition_count': len(lag_list)
                })
        
        # Sort by lag descending
        group_lags.sort(key=lambda x: x['total_lag'], reverse=True)
        
        return group_lags[:top_n]
    
    def create_lag_dashboard(self) -> go.Figure:
        """Create lag monitoring dashboard."""
        fig = make_subplots(
            rows=3, cols=2,
            subplot_titles=(
                'Total Consumer Lag Over Time',
                'Max Lag by Consumer Group',
                'Lag Distribution by Topic',
                'Broker Health Status',
                'Messages Processed',
                'Lagging Consumer Groups'
            ),
            specs=[
                [{'type': 'scatter'}, {'type': 'bar'}],
                [{'type': 'bar'}, {'type': 'indicator'}],
                [{'type': 'scatter'}, {'type': 'pie'}]
            ],
            vertical_spacing=0.1,
            horizontal_spacing=0.15
        )
        
        # Get metrics data
        lag_df = self.get_metrics_dataframe('consumer_lag')
        health_df = self.get_metrics_dataframe('broker_health')
        throughput_df = self.get_metrics_dataframe('throughput')
        
        # 1. Total Consumer Lag Over Time
        if not lag_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=lag_df['timestamp'],
                    y=lag_df['total_lag'],
                    mode='lines+markers',
                    name='Total Lag',
                    line=dict(color='red', width=2),
                    hovertemplate='Time: %{x}<br>Lag: %{y:,}<extra></extra>'
                ),
                row=1, col=1
            )
        
        # 2. Max Lag by Consumer Group
        if self.consumer_groups:
            group_lags = []
            group_names = []
            
            for group_id, lag_list in self.consumer_groups.items():
                group_lag = sum(lag.lag for lag in lag_list)
                if group_lag > 0:
                    group_lags.append(group_lag)
                    group_names.append(group_id[:20] + '...' if len(group_id) > 20 else group_id)
            
            if group_lags:
                fig.add_trace(
                    go.Bar(
                        x=group_names[:10],  # Show top 10
                        y=group_lags[:10],
                        name='Group Lag',
                        marker_color='coral'
                    ),
                    row=1, col=2
                )
        
        # 3. Lag Distribution by Topic
        topic_lags = {}
        for lag_list in self.consumer_groups.values():
            for lag in lag_list:
                if lag.lag > 0:
                    topic_lags[lag.topic] = topic_lags.get(lag.topic, 0) + lag.lag
        
        if topic_lags:
            topics = list(topic_lags.keys())[:10]  # Top 10 topics
            lags = [topic_lags[t] for t in topics]
            
            fig.add_trace(
                go.Bar(
                    x=topics,
                    y=lags,
                    name='Topic Lag',
                    marker_color='lightblue'
                ),
                row=2, col=1
            )
        
        # 4. Broker Health Status
        if not health_df.empty:
            latest_health = health_df.iloc[-1]['health_percentage']
            
            fig.add_trace(
                go.Indicator(
                    mode="gauge+number",
                    value=latest_health,
                    title={'text': "Broker Health"},
                    gauge={
                        'axis': {'range': [0, 100]},
                        'bar': {'color': "green" if latest_health > 90 else "orange" if latest_health > 70 else "red"},
                        'steps': [
                            {'range': [0, 70], 'color': "red"},
                            {'range': [70, 90], 'color': "orange"},
                            {'range': [90, 100], 'color': "green"}
                        ]
                    }
                ),
                row=2, col=2
            )
        
        # 5. Messages Processed
        if not throughput_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=throughput_df['timestamp'],
                    y=throughput_df['total_messages'],
                    mode='lines',
                    name='Total Messages',
                    line=dict(color='blue', width=2),
                    hovertemplate='Time: %{x}<br>Messages: %{y:,}<extra></extra>'
                ),
                row=3, col=1
            )
        
        # 6. Lagging Consumer Groups
        lag_summary = self.get_lag_summary()
        labels = ['Lagging', 'Not Lagging']
        values = [
            lag_summary['lagging_groups'],
            lag_summary['total_groups'] - lag_summary['lagging_groups']
        ]
        
        fig.add_trace(
            go.Pie(
                labels=labels,
                values=values,
                hole=.3,
                marker_colors=['red', 'green']
            ),
            row=3, col=2
        )
        
        # Update layout
        fig.update_layout(
            height=900,
            showlegend=False,
            title_text="Kafka Lag Monitoring Dashboard",
            title_x=0.5
        )
        
        # Update axes
        fig.update_xaxes(title_text="Time", row=1, col=1)
        fig.update_yaxes(title_text="Lag (messages)", row=1, col=1)
        fig.update_xaxes(title_text="Consumer Group", row=1, col=2)
        fig.update_yaxes(title_text="Lag (messages)", row=1, col=2)
        fig.update_xaxes(title_text="Topic", row=2, col=1)
        fig.update_yaxes(title_text="Lag (messages)", row=2, col=1)
        fig.update_xaxes(title_text="Time", row=3, col=1)
        fig.update_yaxes(title_text="Messages", row=3, col=1)
        
        return fig


class KafkaLagDashboard:
    """Interactive Kafka Lag Dashboard using Dash."""
    
    def __init__(self, bootstrap_servers: List[str]):
        self.bootstrap_servers = bootstrap_servers
        self.monitor = KafkaLagMonitor(bootstrap_servers)
        self.app = self._create_app()
    
    def _create_app(self) -> dash.Dash:
        """Create Dash application."""
        app = dash.Dash(
            __name__,
            external_stylesheets=[dbc.themes.DARKLY],
            suppress_callback_exceptions=True
        )
        
        app.title = "Kafka Lag Monitoring Dashboard"
        
        # Layout
        app.layout = dbc.Container([
            # Header
            dbc.Row([
                dbc.Col([
                    html.H1("Kafka Lag Monitoring Dashboard", 
                           className="text-center my-4"),
                    html.P("Real-time monitoring of Kafka consumer lag and cluster health",
                          className="text-center text-muted")
                ], width=12)
            ]),
            
            # Control Panel
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Control Panel"),
                        dbc.CardBody([
                            dbc.Row([
                                dbc.Col([
                                    dbc.Button(
                                        "Start Monitoring",
                                        id="start-btn",
                                        color="success",
                                        className="me-2"
                                    ),
                                    dbc.Button(
                                        "Stop Monitoring",
                                        id="stop-btn",
                                        color="danger",
                                        className="me-2"
                                    ),
                                    dbc.Button(
                                        "Refresh",
                                        id="refresh-btn",
                                        color="primary"
                                    )
                                ], width=6),
                                dbc.Col([
                                    html.Div([
                                        html.Span("Status: ", className="me-2"),
                                        dbc.Badge(
                                            "Stopped",
                                            id="status-badge",
                                            color="danger",
                                            className="me-2"
                                        ),
                                        html.Span("Last Update: ", className="me-2"),
                                        html.Span("Never", id="last-update")
                                    ])
                                ], width=6)
                            ]),
                            
                            dbc.Row([
                                dbc.Col([
                                    html.Label("Refresh Interval (seconds):"),
                                    dcc.Slider(
                                        id="refresh-slider",
                                        min=5,
                                        max=60,
                                        step=5,
                                        value=10,
                                        marks={i: str(i) for i in range(5, 61, 5)}
                                    )
                                ], width=12)
                            ], className="mt-3")
                        ])
                    ], className="mb-4")
                ], width=12)
            ]),
            
            # Metrics Cards
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Total Lag"),
                        dbc.CardBody([
                            html.H2("0", id="total-lag", className="card-title"),
                            html.P("messages", className="card-text text-muted")
                        ])
                    ])
                ], width=3),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Max Lag"),
                        dbc.CardBody([
                            html.H2("0", id="max-lag", className="card-title"),
                            html.P("messages", className="card-text text-muted")
                        ])
                    ])
                ], width=3),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Lagging Groups"),
                        dbc.CardBody([
                            html.H2("0", id="lagging-groups", className="card-title"),
                            html.P("of 0 total", id="total-groups", className="card-text text-muted")
                        ])
                    ])
                ], width=3),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Broker Health"),
                        dbc.CardBody([
                            html.H2("0%", id="broker-health", className="card-title"),
                            html.P("healthy", className="card-text text-muted")
                        ])
                    ])
                ], width=3)
            ], className="mb-4"),
            
            # Main Charts
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Lag Overview"),
                        dbc.CardBody([
                            dcc.Graph(id="lag-overview-chart")
                        ])
                    ])
                ], width=12)
            ], className="mb-4"),
            
            # Detailed Views
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Top Lagging Consumers"),
                        dbc.CardBody([
                            html.Div(id="lagging-consumers-table")
                        ])
                    ])
                ], width=6),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Cluster Information"),
                        dbc.CardBody([
                            html.Div(id="cluster-info")
                        ])
                    ])
                ], width=6)
            ]),
            
            # Hidden div for storing data
            dcc.Store(id='metrics-store', data={}),
            
            # Interval for auto-refresh
            dcc.Interval(
                id='interval-component',
                interval=10000,  # 10 seconds
                n_intervals=0,
                disabled=True
            )
        ], fluid=True)
        
        # Callbacks
        @app.callback(
            [Output('status-badge', 'children'),
             Output('status-badge', 'color'),
             Output('interval-component', 'disabled')],
            [Input('start-btn', 'n_clicks'),
             Input('stop-btn', 'n_clicks')],
            prevent_initial_call=True
        )
        def control_monitoring(start_clicks, stop_clicks):
            ctx = callback_context
            if not ctx.triggered:
                raise PreventUpdate
            
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]
            
            if button_id == 'start-btn':
                self.monitor.start_monitoring()
                return "Running", "success", False
            elif button_id == 'stop-btn':
                self.monitor.stop_monitoring()
                return "Stopped", "danger", True
            
            raise PreventUpdate
        
        @app.callback(
            [Output('total-lag', 'children'),
             Output('max-lag', 'children'),
             Output('lagging-groups', 'children'),
             Output('total-groups', 'children'),
             Output('broker-health', 'children'),
             Output('last-update', 'children'),
             Output('metrics-store', 'data')],
            [Input('refresh-btn', 'n_clicks'),
             Input('interval-component', 'n_intervals')],
            prevent_initial_call=True
        )
        def update_metrics(refresh_clicks, n_intervals):
            # Get lag summary
            lag_summary = self.monitor.get_lag_summary()
            
            # Get broker health
            health_df = self.monitor.get_metrics_dataframe('broker_health')
            broker_health = "0%"
            if not health_df.empty:
                latest_health = health_df.iloc[-1]['health_percentage']
                broker_health = f"{latest_health:.1f}%"
            
            # Format numbers with commas
            total_lag = f"{lag_summary['total_lag']:,}"
            max_lag = f"{lag_summary['max_lag']:,}"
            lagging_groups = f"{lag_summary['lagging_groups']:,}"
            total_groups = f"of {lag_summary['total_groups']:,} total"
            
            # Current time
            last_update = datetime.now().strftime("%H:%M:%S")
            
            # Store data for other callbacks
            store_data = {
                'lag_summary': lag_summary,
                'last_update': last_update
            }
            
            return total_lag, max_lag, lagging_groups, total_groups, broker_health, last_update, store_data
        
        @app.callback(
            Output('lag-overview-chart', 'figure'),
            [Input('metrics-store', 'data')]
        )
        def update_lag_chart(store_data):
            return self.monitor.create_lag_dashboard()
        
        @app.callback(
            Output('lagging-consumers-table', 'children'),
            [Input('metrics-store', 'data')]
        )
        def update_lagging_consumers_table(store_data):
            top_lagging = self.monitor.get_top_lagging_consumers(10)
            
            if not top_lagging:
                return html.P("No lagging consumers found", className="text-muted")
            
            # Create table
            table_header = [
                html.Thead(html.Tr([
                    html.Th("Consumer Group"),
                    html.Th("Total Lag"),
                    html.Th("Topics"),
                    html.Th("Partitions")
                ]))
            ]
            
            table_rows = []
            for consumer in top_lagging:
                table_rows.append(html.Tr([
                    html.Td(consumer['group_id'][:30] + '...' if len(consumer['group_id']) > 30 else consumer['group_id']),
                    html.Td(f"{consumer['total_lag']:,}"),
                    html.Td(consumer['topic_count']),
                    html.Td(consumer['partition_count'])
                ]))
            
            table_body = [html.Tbody(table_rows)]
            
            return dbc.Table(table_header + table_body, striped=True, hover=True)
        
        @app.callback(
            Output('cluster-info', 'children'),
            [Input('metrics-store', 'data')]
        )
        def update_cluster_info(store_data):
            try:
                # Get cluster info
                if self.monitor.admin_client:
                    cluster_info = self.monitor.admin_client.describe_cluster()
                    
                    info_items = [
                        html.P(f"Cluster ID: {cluster_info.cluster_id}"),
                        html.P(f"Controller ID: {cluster_info.controller.id}"),
                        html.P(f"Brokers: {len(cluster_info.nodes())}"),
                        html.P(f"Bootstrap Servers: {', '.join(self.bootstrap_servers)}")
                    ]
                    
                    return html.Div(info_items)
            
            except Exception as e:
                logger.error(f"Error getting cluster info: {e}")
            
            return html.P("Unable to fetch cluster information", className="text-muted")
        
        return app
    
    def run(self, host: str = "0.0.0.0", port: int = 8050, debug: bool = False):
        """Run the dashboard."""
        logger.info(f"Starting Kafka Lag Dashboard on {host}:{port}")
        self.app.run_server(host=host, port=port, debug=debug)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Lag Monitoring Dashboard")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                       help="Kafka bootstrap servers (comma-separated)")
    parser.add_argument("--host", default="0.0.0.0",
                       help="Dashboard host address")
    parser.add_argument("--port", type=int, default=8050,
                       help="Dashboard port")
    parser.add_argument("--debug", action="store_true",
                       help="Enable debug mode")
    
    args = parser.parse_args()
    
    # Parse bootstrap servers
    bootstrap_servers = args.bootstrap_servers.split(',')
    
    # Create and run dashboard
    dashboard = KafkaLagDashboard(bootstrap_servers)
    dashboard.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
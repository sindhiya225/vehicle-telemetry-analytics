import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
import json
import yaml
from datetime import datetime, timedelta
import time
import threading
from collections import deque
import numpy as np

class RealTimeMonitoringDashboard:
    def __init__(self):
        """Initialize real-time monitoring dashboard"""
        self.load_config()
        self.setup_page()
        self.initialize_data_structures()
        
    def load_config(self):
        """Load Kafka configuration"""
        with open('streaming/kafka/kafka_config.yaml', 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Kafka consumer configuration
        self.consumer_config = {
            'bootstrap.servers': self.config['kafka']['bootstrap_servers'],
            'group.id': 'realtime-dashboard',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'max.poll.interval.ms': 300000
        }
    
    def setup_page(self):
        """Setup Streamlit page configuration"""
        st.set_page_config(
            page_title="Real-Time Vehicle Monitoring",
            page_icon="🚗",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        # Custom CSS
        st.markdown("""
        <style>
            .stAlert {
                padding: 1rem;
                border-radius: 0.5rem;
            }
            .metric-card {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 1.5rem;
                border-radius: 1rem;
                color: white;
                margin-bottom: 1rem;
            }
            .alert-critical {
                background-color: #fef2f2;
                border-left: 4px solid #dc2626;
                padding: 1rem;
                margin: 1rem 0;
            }
            .alert-warning {
                background-color: #fffbeb;
                border-left: 4px solid #f59e0b;
                padding: 1rem;
                margin: 1rem 0;
            }
            .alert-info {
                background-color: #eff6ff;
                border-left: 4px solid #3b82f6;
                padding: 1rem;
                margin: 1rem 0;
            }
            .kpi-card {
                background-color: #f8fafc;
                padding: 1.5rem;
                border-radius: 1rem;
                border: 1px solid #e2e8f0;
                text-align: center;
            }
        </style>
        """, unsafe_allow_html=True)
    
    def initialize_data_structures(self):
        """Initialize data structures for real-time data"""
        # Data buffers for charts
        self.efficiency_data = deque(maxlen=100)
        self.speed_data = deque(maxlen=100)
        self.temperature_data = deque(maxlen=100)
        self.fault_data = deque(maxlen=50)
        self.alert_data = deque(maxlen=20)
        
        # Aggregated metrics
        self.metrics = {
            'total_vehicles': 0,
            'active_vehicles': set(),
            'total_distance': 0,
            'total_fuel': 0,
            'total_faults': 0,
            'total_alerts': 0,
            'avg_efficiency': 0,
            'avg_speed': 0
        }
        
        # Last update time
        self.last_update = datetime.now()
    
    def create_kafka_consumer(self):
        """Create Kafka consumer for real-time data"""
        try:
            consumer = Consumer(self.consumer_config)
            
            # Subscribe to topics
            topics = [
                self.config['kafka']['topics']['processed_telemetry'],
                self.config['kafka']['topics']['anomalies'],
                self.config['kafka']['topics']['maintenance_alerts']
            ]
            
            consumer.subscribe(topics)
            return consumer
        except Exception as e:
            st.error(f"Failed to create Kafka consumer: {e}")
            return None
    
    def process_message(self, msg):
        """Process incoming Kafka message"""
        try:
            data = json.loads(msg.value().decode('utf-8'))
            
            if msg.topic() == self.config['kafka']['topics']['processed_telemetry']:
                self.process_telemetry_data(data)
            elif msg.topic() == self.config['kafka']['topics']['anomalies']:
                self.process_anomaly_data(data)
            elif msg.topic() == self.config['kafka']['topics']['maintenance_alerts']:
                self.process_alert_data(data)
                
        except Exception as e:
            print(f"Error processing message: {e}")
    
    def process_telemetry_data(self, data):
        """Process telemetry data"""
        timestamp = datetime.fromtimestamp(data.get('timestamp', 0) / 1000)
        
        # Update metrics
        self.metrics['active_vehicles'].add(data['vehid'])
        self.metrics['total_distance'] += data.get('trip_distance_km', 0)
        self.metrics['total_fuel'] += data.get('fuel_consumed_l', 0)
        
        if data.get('has_fault', False):
            self.metrics['total_faults'] += 1
        
        # Update data buffers
        self.efficiency_data.append({
            'timestamp': timestamp,
            'vehicle': data['vehid'],
            'efficiency': data.get('fuel_efficiency_kmpl', 0),
            'type': data['vehicle_type']
        })
        
        self.speed_data.append({
            'timestamp': timestamp,
            'vehicle': data['vehid'],
            'speed': data.get('avg_speed_kmph', 0),
            'type': data['vehicle_type']
        })
        
        self.temperature_data.append({
            'timestamp': timestamp,
            'vehicle': data['vehid'],
            'temperature': data.get('avg_engine_temp_c', 0),
            'type': data['vehicle_type']
        })
        
        if data.get('has_fault', False):
            self.fault_data.append({
                'timestamp': timestamp,
                'vehicle': data['vehid'],
                'fault_codes': data.get('fault_codes', []),
                'severity': data.get('fault_severity', 'UNKNOWN')
            })
    
    def process_anomaly_data(self, data):
        """Process anomaly data"""
        timestamp = datetime.fromtimestamp(data.get('timestamp', 0) / 1000)
        
        self.alert_data.append({
            'timestamp': timestamp,
            'vehicle': data['vehid'],
            'type': 'ANOMALY',
            'severity': data.get('severity', 'MEDIUM'),
            'score': data.get('score', 0),
            'description': data.get('description', 'Anomaly detected')
        })
    
    def process_alert_data(self, data):
        """Process maintenance alert data"""
        timestamp = datetime.fromtimestamp(data.get('alert_timestamp', 0) / 1000)
        
        self.alert_data.append({
            'timestamp': timestamp,
            'vehicle': data['vehid'],
            'type': data.get('alert_type', 'ALERT'),
            'severity': data.get('priority', 'MEDIUM'),
            'description': data.get('description', 'Alert triggered'),
            'action': data.get('recommended_action', '')
        })
        
        self.metrics['total_alerts'] += 1
    
    def render_header(self):
        """Render dashboard header"""
        st.markdown("""
        <div style='text-align: center; padding: 2rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 1rem; margin-bottom: 2rem;'>
            <h1 style='color: white; margin: 0;'>🚗 Real-Time Vehicle Telemetry Dashboard</h1>
            <p style='color: rgba(255, 255, 255, 0.9); margin: 0.5rem 0 0 0;'>Live Monitoring & Analytics</p>
        </div>
        """, unsafe_allow_html=True)
    
    def render_metrics(self):
        """Render key metrics"""
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class='kpi-card'>
                <h3 style='margin: 0; color: #3b82f6;'>{len(self.metrics['active_vehicles'])}</h3>
                <p style='margin: 0; color: #64748b;'>Active Vehicles</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            avg_eff = np.mean([d['efficiency'] for d in self.efficiency_data]) if self.efficiency_data else 0
            st.markdown(f"""
            <div class='kpi-card'>
                <h3 style='margin: 0; color: #10b981;'>{avg_eff:.1f}</h3>
                <p style='margin: 0; color: #64748b;'>Avg Efficiency (km/L)</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class='kpi-card'>
                <h3 style='margin: 0; color: #ef4444;'>{self.metrics['total_faults']}</h3>
                <p style='margin: 0; color: #64748b;'>Active Faults</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class='kpi-card'>
                <h3 style='margin: 0; color: #f59e0b;'>{self.metrics['total_alerts']}</h3>
                <p style='margin: 0; color: #64748b;'>Active Alerts</p>
            </div>
            """, unsafe_allow_html=True)
    
    def render_real_time_charts(self):
        """Render real-time charts"""
        st.markdown("### 📈 Real-Time Metrics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Fuel Efficiency Chart
            if self.efficiency_data:
                df = pd.DataFrame(list(self.efficiency_data))
                
                fig = go.Figure()
                
                for vehicle_type in df['type'].unique():
                    type_data = df[df['type'] == vehicle_type]
                    fig.add_trace(go.Scatter(
                        x=type_data['timestamp'],
                        y=type_data['efficiency'],
                        mode='markers',
                        name=vehicle_type,
                        marker=dict(size=8)
                    ))
                
                fig.update_layout(
                    title='Real-Time Fuel Efficiency',
                    xaxis_title='Time',
                    yaxis_title='Efficiency (km/L)',
                    height=300,
                    showlegend=True
                )
                
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Engine Temperature Chart
            if self.temperature_data:
                df = pd.DataFrame(list(self.temperature_data))
                
                fig = go.Figure()
                
                fig.add_trace(go.Scatter(
                    x=df['timestamp'],
                    y=df['temperature'],
                    mode='markers',
                    name='Engine Temp',
                    marker=dict(
                        size=8,
                        color=df['temperature'],
                        colorscale='RdYlGn_r',
                        showscale=True,
                        colorbar=dict(title='°C')
                    )
                ))
                
                # Add warning line
                fig.add_hline(y=105, line_dash="dash", line_color="red", 
                            annotation_text="Warning", annotation_position="bottom right")
                
                fig.update_layout(
                    title='Real-Time Engine Temperature',
                    xaxis_title='Time',
                    yaxis_title='Temperature (°C)',
                    height=300
                )
                
                st.plotly_chart(fig, use_container_width=True)
    
    def render_alerts(self):
        """Render real-time alerts"""
        st.markdown("### ⚠️ Real-Time Alerts")
        
        if not self.alert_data:
            st.info("No active alerts")
            return
        
        # Sort alerts by timestamp (newest first)
        alerts = sorted(self.alert_data, key=lambda x: x['timestamp'], reverse=True)
        
        for alert in alerts[:10]:  # Show last 10 alerts
            alert_class = "alert-critical" if alert['severity'] == 'CRITICAL' else \
                         "alert-warning" if alert['severity'] == 'HIGH' else "alert-info"
            
            st.markdown(f"""
            <div class='{alert_class}'>
                <div style='display: flex; justify-content: space-between;'>
                    <strong>🚨 Vehicle {alert['vehicle']} - {alert['type']}</strong>
                    <span style='color: #6b7280; font-size: 0.9rem;'>{alert['timestamp'].strftime('%H:%M:%S')}</span>
                </div>
                <p style='margin: 0.5rem 0;'>{alert['description']}</p>
                {f"<p style='margin: 0; color: #374151;'><strong>Action:</strong> {alert.get('action', '')}</p>" if alert.get('action') else ''}
                <div style='display: flex; justify-content: space-between; margin-top: 0.5rem;'>
                    <span class='badge' style='background: {'#dc2626' if alert['severity'] == 'CRITICAL' else '#f59e0b' if alert['severity'] == 'HIGH' else '#3b82f6'}; 
                           color: white; padding: 0.25rem 0.75rem; border-radius: 1rem; font-size: 0.8rem;'>
                        {alert['severity']}
                    </span>
                    {f"<span style='color: #6b7280; font-size: 0.9rem;'>Score: {alert.get('score', 0):.2f}</span>" if alert.get('score') is not None else ''}
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    def render_fleet_overview(self):
        """Render fleet overview"""
        st.markdown("### 🚛 Fleet Overview")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Vehicle type distribution
            if self.efficiency_data:
                df = pd.DataFrame(list(self.efficiency_data))
                type_counts = df['type'].value_counts()
                
                fig = go.Figure(data=[go.Pie(
                    labels=type_counts.index,
                    values=type_counts.values,
                    hole=.3,
                    marker_colors=['#3b82f6', '#10b981', '#f59e0b']
                )])
                
                fig.update_layout(
                    title='Vehicle Type Distribution',
                    height=300,
                    showlegend=True
                )
                
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Fault distribution by type
            if self.fault_data:
                df = pd.DataFrame(list(self.fault_data))
                
                if not df.empty and 'severity' in df.columns:
                    severity_counts = df['severity'].value_counts()
                    
                    fig = go.Figure(data=[go.Bar(
                        x=severity_counts.index,
                        y=severity_counts.values,
                        marker_color=['#dc2626', '#f59e0b', '#3b82f6']
                    )])
                    
                    fig.update_layout(
                        title='Fault Severity Distribution',
                        xaxis_title='Severity',
                        yaxis_title='Count',
                        height=300
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
        
        with col3:
            # Performance metrics
            metrics_data = {
                'Metric': ['Avg Speed', 'Total Distance', 'Total Fuel', 'Fault Rate'],
                'Value': [
                    f"{np.mean([d['speed'] for d in self.speed_data]):.1f} km/h" if self.speed_data else "0 km/h",
                    f"{self.metrics['total_distance']:.0f} km",
                    f"{self.metrics['total_fuel']:.1f} L",
                    f"{self.metrics['total_faults']} faults"
                ],
                'Trend': ['↗️', '↗️', '↘️', '⚠️']
            }
            
            df = pd.DataFrame(metrics_data)
            
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Metric": st.column_config.Column(width="medium"),
                    "Value": st.column_config.Column(width="medium"),
                    "Trend": st.column_config.Column(width="small")
                }
            )
    
    def render_vehicle_details(self):
        """Render detailed vehicle information"""
        st.markdown("### 🚗 Vehicle Details")
        
        # Create tabs for different views
        tab1, tab2, tab3 = st.tabs(["Active Vehicles", "Fault History", "Performance"])
        
        with tab1:
            # Active vehicles table
            if self.efficiency_data:
                df = pd.DataFrame(list(self.efficiency_data))
                
                # Get latest reading for each vehicle
                latest_data = []
                for vehicle in df['vehicle'].unique():
                    vehicle_data = df[df['vehicle'] == vehicle]
                    latest = vehicle_data.iloc[vehicle_data['timestamp'].argmax()]
                    latest_data.append(latest)
                
                if latest_data:
                    active_df = pd.DataFrame(latest_data)
                    
                    st.dataframe(
                        active_df[['vehicle', 'type', 'efficiency', 'timestamp']],
                        use_container_width=True,
                        column_config={
                            "vehicle": "Vehicle ID",
                            "type": "Type",
                            "efficiency": "Efficiency",
                            "timestamp": "Last Update"
                        }
                    )
        
        with tab2:
            # Fault history
            if self.fault_data:
                df = pd.DataFrame(list(self.fault_data))
                
                if not df.empty:
                    st.dataframe(
                        df[['vehicle', 'fault_codes', 'severity', 'timestamp']],
                        use_container_width=True,
                        column_config={
                            "vehicle": "Vehicle ID",
                            "fault_codes": "Fault Codes",
                            "severity": "Severity",
                            "timestamp": "Time"
                        }
                    )
        
        with tab3:
            # Performance trends
            st.info("Performance trend analysis coming soon...")
    
    def render_kafka_metrics(self):
        """Render Kafka metrics"""
        st.markdown("### 📊 Kafka Metrics")
        
        try:
            admin_client = AdminClient({'bootstrap.servers': self.config['kafka']['bootstrap_servers']})
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Topic information
                topics = admin_client.list_topics().topics
                
                topic_data = []
                for topic_name, topic_metadata in topics.items():
                    if topic_name.startswith('vehicle.'):
                        partitions = len(topic_metadata.partitions)
                        topic_data.append({
                            'Topic': topic_name,
                            'Partitions': partitions
                        })
                
                if topic_data:
                    st.dataframe(
                        pd.DataFrame(topic_data),
                        use_container_width=True,
                        hide_index=True
                    )
            
            with col2:
                # Consumer lag (simplified)
                consumer = self.create_kafka_consumer()
                if consumer:
                    assignment = consumer.assignment()
                    
                    lag_data = []
                    for tp in assignment:
                        committed = consumer.committed([tp])[0]
                        if committed:
                            lag = consumer.get_watermark_offsets(tp)[1] - committed.offset
                            lag_data.append({
                                'Topic': tp.topic,
                                'Partition': tp.partition,
                                'Lag': lag
                            })
                    
                    consumer.close()
                    
                    if lag_data:
                        st.dataframe(
                            pd.DataFrame(lag_data),
                            use_container_width=True,
                            hide_index=True
                        )
        
        except Exception as e:
            st.error(f"Failed to fetch Kafka metrics: {e}")
    
    def run(self):
        """Run the real-time dashboard"""
        self.render_header()
        self.render_metrics()
        
        # Create placeholder for dynamic updates
        charts_placeholder = st.empty()
        alerts_placeholder = st.empty()
        fleet_placeholder = st.empty()
        details_placeholder = st.empty()
        kafka_placeholder = st.empty()
        
        # Start Kafka consumer in background thread
        consumer = self.create_kafka_consumer()
        
        if consumer:
            st.success("✅ Connected to Kafka cluster")
            
            # Update frequency
            update_interval = 2  # seconds
            
            try:
                while True:
                    # Poll for new messages
                    msg = consumer.poll(1.0)
                    
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            st.error(f"Kafka error: {msg.error()}")
                            break
                    
                    # Process message
                    self.process_message(msg)
                    
                    # Update dashboard at specified interval
                    if (datetime.now() - self.last_update).seconds >= update_interval:
                        self.last_update = datetime.now()
                        
                        # Update charts
                        with charts_placeholder.container():
                            self.render_real_time_charts()
                        
                        # Update alerts
                        with alerts_placeholder.container():
                            self.render_alerts()
                        
                        # Update fleet overview
                        with fleet_placeholder.container():
                            self.render_fleet_overview()
                        
                        # Update vehicle details
                        with details_placeholder.container():
                            self.render_vehicle_details()
                        
                        # Update Kafka metrics
                        with kafka_placeholder.container():
                            self.render_kafka_metrics()
            
            except KeyboardInterrupt:
                print("Stopping dashboard...")
            finally:
                consumer.close()
        else:
            st.error("❌ Failed to connect to Kafka")

if __name__ == "__main__":
    dashboard = RealTimeMonitoringDashboard()
    dashboard.run()
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="Vehicle Telemetry Analytics Dashboard",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #F3F4F6;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .highlight {
        background-color: #DBEAFE;
        padding: 0.5rem;
        border-radius: 5px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

class VehicleTelemetryDashboard:
    def __init__(self):
        self.load_data()
        
    def load_data(self):
        """Load and preprocess data"""
        # Load vehicle data
        self.vehicle_data = pd.read_csv('data/processed/cleaned_vehicle_data.csv')
        
        # Load telemetry data
        self.telemetry_data = pd.read_csv('data/telemetry/simulated_telemetry_data.csv')
        self.telemetry_data['timestamp'] = pd.to_datetime(self.telemetry_data['timestamp'])
        
        # Merge datasets for comprehensive analysis
        self.merged_data = pd.merge(
            self.telemetry_data,
            self.vehicle_data,
            on='vehid',
            how='left',
            suffixes=('', '_vehicle')
        )
        
    def render_sidebar(self):
        """Render sidebar filters"""
        st.sidebar.title("🔧 Dashboard Filters")
        
        # Date range filter
        min_date = self.telemetry_data['timestamp'].min().date()
        max_date = self.telemetry_data['timestamp'].max().date()
        
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        if len(date_range) == 2:
            start_date, end_date = date_range
            self.filtered_data = self.telemetry_data[
                (self.telemetry_data['timestamp'].dt.date >= start_date) &
                (self.telemetry_data['timestamp'].dt.date <= end_date)
            ]
        else:
            self.filtered_data = self.telemetry_data
        
        # Vehicle type filter
        vehicle_types = ['All'] + list(self.vehicle_data['vehicle_type'].unique())
        selected_type = st.sidebar.selectbox(
            "Vehicle Type",
            vehicle_types
        )
        
        if selected_type != 'All':
            vehicle_ids = self.vehicle_data[
                self.vehicle_data['vehicle_type'] == selected_type
            ]['vehid'].unique()
            self.filtered_data = self.filtered_data[
                self.filtered_data['vehid'].isin(vehicle_ids)
            ]
        
        # Vehicle class filter
        vehicle_classes = ['All'] + list(self.vehicle_data['vehicle_class'].dropna().unique())
        selected_class = st.sidebar.selectbox(
            "Vehicle Class",
            vehicle_classes
        )
        
        if selected_class != 'All':
            self.filtered_data = self.filtered_data[
                self.filtered_data['vehicle_class'] == selected_class
            ]
        
        # Key metrics summary
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📊 Quick Stats")
        
        total_trips = len(self.filtered_data)
        total_vehicles = self.filtered_data['vehid'].nunique()
        avg_efficiency = self.filtered_data['fuel_efficiency_kmpl'].mean()
        fault_rate = self.filtered_data['has_fault'].mean() * 100
        
        st.sidebar.metric("Total Trips", f"{total_trips:,}")
        st.sidebar.metric("Unique Vehicles", total_vehicles)
        st.sidebar.metric("Avg Efficiency", f"{avg_efficiency:.1f} km/L")
        st.sidebar.metric("Fault Rate", f"{fault_rate:.1f}%")
    
    def render_header(self):
        """Render dashboard header"""
        st.markdown('<h1 class="main-header">🚗 Vehicle Telemetry Analytics Dashboard</h1>', 
                   unsafe_allow_html=True)
        
        # Summary metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_distance = self.filtered_data['trip_distance_km'].sum()
            st.metric("Total Distance", f"{total_distance:,.0f} km")
        
        with col2:
            total_fuel = self.filtered_data['fuel_consumed_l'].sum()
            st.metric("Fuel Consumed", f"{total_fuel:,.0f} L")
        
        with col3:
            total_idle = self.filtered_data['idle_time_min'].sum() / 60
            st.metric("Idle Time", f"{total_idle:,.1f} hours")
        
        with col4:
            maintenance_count = self.filtered_data['maintenance_flag'].sum()
            st.metric("Maintenance Alerts", maintenance_count)
    
    def render_efficiency_analysis(self):
        """Render fuel efficiency analysis"""
        st.markdown("### ⛽ Fuel Efficiency Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Efficiency by vehicle type
            eff_by_type = self.filtered_data.groupby('vehicle_type')['fuel_efficiency_kmpl'].agg(
                ['mean', 'std', 'count']
            ).reset_index()
            
            fig = px.bar(
                eff_by_type,
                x='vehicle_type',
                y='mean',
                error_y='std',
                title='Average Fuel Efficiency by Vehicle Type',
                labels={'mean': 'Fuel Efficiency (km/L)', 'vehicle_type': 'Vehicle Type'},
                color='vehicle_type'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Efficiency trends over time
            self.filtered_data['date'] = self.filtered_data['timestamp'].dt.date
            daily_eff = self.filtered_data.groupby(['date', 'vehicle_type'])['fuel_efficiency_kmpl'].mean().reset_index()
            
            fig = px.line(
                daily_eff,
                x='date',
                y='fuel_efficiency_kmpl',
                color='vehicle_type',
                title='Fuel Efficiency Trends',
                labels={'fuel_efficiency_kmpl': 'Fuel Efficiency (km/L)', 'date': 'Date'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Detailed efficiency analysis
        st.markdown("#### Detailed Efficiency Comparison")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Efficiency vs Engine Size
            merged_for_analysis = pd.merge(
                self.filtered_data,
                self.vehicle_data[['vehid', 'displacement_l', 'engine_cylinders']],
                on='vehid',
                how='left'
            )
            
            fig = px.scatter(
                merged_for_analysis,
                x='displacement_l',
                y='fuel_efficiency_kmpl',
                color='vehicle_type',
                size='trip_distance_km',
                hover_data=['vehid', 'vehicle_class'],
                title='Efficiency vs Engine Displacement',
                labels={'displacement_l': 'Engine Displacement (L)', 
                       'fuel_efficiency_kmpl': 'Fuel Efficiency (km/L)'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Efficiency distribution
            fig = px.box(
                self.filtered_data,
                x='vehicle_type',
                y='fuel_efficiency_kmpl',
                color='vehicle_type',
                title='Fuel Efficiency Distribution',
                labels={'fuel_efficiency_kmpl': 'Fuel Efficiency (km/L)'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    def render_usage_patterns(self):
        """Render usage patterns analysis"""
        st.markdown("### 📈 Usage Patterns Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Daily usage patterns
            daily_usage = self.filtered_data.groupby(
                self.filtered_data['timestamp'].dt.date
            ).agg({
                'trip_distance_km': 'sum',
                'vehid': 'nunique'
            }).reset_index()
            
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            fig.add_trace(
                go.Scatter(
                    x=daily_usage['timestamp'],
                    y=daily_usage['trip_distance_km'],
                    name='Total Distance',
                    line=dict(color='blue')
                ),
                secondary_y=False
            )
            
            fig.add_trace(
                go.Bar(
                    x=daily_usage['timestamp'],
                    y=daily_usage['vehid'],
                    name='Active Vehicles',
                    marker_color='lightblue',
                    opacity=0.7
                ),
                secondary_y=True
            )
            
            fig.update_layout(
                title='Daily Usage Patterns',
                xaxis_title='Date',
                showlegend=True
            )
            
            fig.update_yaxes(title_text="Distance (km)", secondary_y=False)
            fig.update_yaxes(title_text="Active Vehicles", secondary_y=True)
            
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Hourly usage heatmap
            self.filtered_data['hour'] = self.filtered_data['timestamp'].dt.hour
            hourly_usage = self.filtered_data.groupby(
                ['vehicle_type', 'hour']
            ).size().reset_index(name='trip_count')
            
            fig = px.density_heatmap(
                hourly_usage,
                x='hour',
                y='vehicle_type',
                z='trip_count',
                title='Hourly Usage Patterns',
                labels={'hour': 'Hour of Day', 'vehicle_type': 'Vehicle Type'},
                color_continuous_scale='Viridis'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Trip characteristics
        st.markdown("#### Trip Characteristics")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            avg_trip_distance = self.filtered_data['trip_distance_km'].mean()
            st.metric("Avg Trip Distance", f"{avg_trip_distance:.1f} km")
        
        with col2:
            avg_trip_duration = self.filtered_data['trip_duration_min'].mean()
            st.metric("Avg Trip Duration", f"{avg_trip_duration:.1f} min")
        
        with col3:
            avg_speed = self.filtered_data['avg_speed_kmph'].mean()
            st.metric("Avg Speed", f"{avg_speed:.1f} km/h")
    
    def render_maintenance_analysis(self):
        """Render maintenance and fault analysis"""
        st.markdown("### 🔧 Maintenance & Fault Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Fault analysis by vehicle type
            fault_analysis = self.filtered_data.groupby('vehicle_type').agg({
                'has_fault': 'mean',
                'maintenance_flag': 'mean',
                'vehid': 'nunique'
            }).reset_index()
            
            fault_analysis['fault_rate'] = fault_analysis['has_fault'] * 100
            fault_analysis['maintenance_rate'] = fault_analysis['maintenance_flag'] * 100
            
            fig = go.Figure(data=[
                go.Bar(name='Fault Rate %', x=fault_analysis['vehicle_type'], 
                      y=fault_analysis['fault_rate']),
                go.Bar(name='Maintenance Rate %', x=fault_analysis['vehicle_type'], 
                      y=fault_analysis['maintenance_rate'])
            ])
            
            fig.update_layout(
                title='Fault & Maintenance Rates by Vehicle Type',
                xaxis_title='Vehicle Type',
                yaxis_title='Rate (%)',
                barmode='group'
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Fault trends over time
            self.filtered_data['date'] = self.filtered_data['timestamp'].dt.date
            daily_faults = self.filtered_data.groupby('date').agg({
                'has_fault': 'sum',
                'trip_id': 'count'
            }).reset_index()
            
            daily_faults['fault_rate'] = daily_faults['has_fault'] / daily_faults['trip_id'] * 100
            
            fig = px.line(
                daily_faults,
                x='date',
                y='fault_rate',
                title='Daily Fault Rate Trend',
                labels={'fault_rate': 'Fault Rate (%)', 'date': 'Date'}
            )
            
            fig.update_traces(line=dict(color='red', width=3))
            st.plotly_chart(fig, use_container_width=True)
        
        # Top vehicles needing attention
        st.markdown("#### Vehicles Needing Attention")
        
        vehicles_needing_attention = self.filtered_data.groupby('vehid').agg({
            'has_fault': 'sum',
            'maintenance_flag': 'sum',
            'fuel_efficiency_kmpl': 'mean',
            'vehicle_type': 'first'
        }).reset_index()
        
        vehicles_needing_attention['total_alerts'] = (
            vehicles_needing_attention['has_fault'] + 
            vehicles_needing_attention['maintenance_flag']
        )
        
        top_problem_vehicles = vehicles_needing_attention.sort_values(
            'total_alerts', ascending=False
        ).head(10)
        
        st.dataframe(
            top_problem_vehicles[
                ['vehid', 'vehicle_type', 'total_alerts', 'has_fault', 
                 'maintenance_flag', 'fuel_efficiency_kmpl']
            ].rename(columns={
                'vehid': 'Vehicle ID',
                'vehicle_type': 'Type',
                'total_alerts': 'Total Alerts',
                'has_fault': 'Faults',
                'maintenance_flag': 'Maintenance Flags',
                'fuel_efficiency_kmpl': 'Avg Efficiency'
            }),
            use_container_width=True
        )
    
    def render_idle_time_analysis(self):
        """Render idle time analysis"""
        st.markdown("### ⏱️ Idle Time Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Idle time distribution
            idle_analysis = self.filtered_data.groupby('vehicle_type').agg({
                'idle_time_min': 'mean',
                'trip_duration_min': 'mean'
            }).reset_index()
            
            idle_analysis['idle_percentage'] = (
                idle_analysis['idle_time_min'] / idle_analysis['trip_duration_min'] * 100
            )
            
            fig = px.bar(
                idle_analysis,
                x='vehicle_type',
                y='idle_percentage',
                title='Average Idle Time Percentage by Vehicle Type',
                labels={'idle_percentage': 'Idle Time (%)', 'vehicle_type': 'Vehicle Type'},
                color='vehicle_type'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Idle time vs efficiency correlation
            fig = px.scatter(
                self.filtered_data,
                x='idle_time_min',
                y='fuel_efficiency_kmpl',
                color='vehicle_type',
                size='trip_distance_km',
                hover_data=['vehid', 'trip_duration_min'],
                title='Idle Time vs Fuel Efficiency',
                labels={
                    'idle_time_min': 'Idle Time (minutes)',
                    'fuel_efficiency_kmpl': 'Fuel Efficiency (km/L)'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Cost of idle time
        st.markdown("#### Cost Analysis of Idle Time")
        
        total_idle_hours = self.filtered_data['idle_time_min'].sum() / 60
        fuel_per_hour_idle = 0.5  # liters per hour idle
        fuel_cost_per_liter = 1.2  # USD
        
        idle_fuel_cost = total_idle_hours * fuel_per_hour_idle * fuel_cost_per_liter
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Idle Hours", f"{total_idle_hours:,.1f}")
        
        with col2:
            st.metric("Idle Fuel Consumption", f"{total_idle_hours * fuel_per_hour_idle:,.1f} L")
        
        with col3:
            st.metric("Idle Cost", f"${idle_fuel_cost:,.2f}")
    
    def render_comparative_analysis(self):
        """Render comparative analysis between HEV and ICE"""
        st.markdown("### 🔄 HEV vs ICE Comparative Analysis")
        
        # Prepare comparison data
        comparison_data = self.merged_data.groupby('vehicle_type').agg({
            'fuel_efficiency_kmpl': ['mean', 'std'],
            'trip_distance_km': 'mean',
            'has_fault': 'mean',
            'idle_time_min': 'mean',
            'driver_behavior_score': 'mean',
            'vehid': 'nunique'
        }).round(2)
        
        # Flatten multi-index columns
        comparison_data.columns = ['_'.join(col).strip() for col in comparison_data.columns.values]
        
        # Display comparison table
        st.dataframe(comparison_data, use_container_width=True)
        
        # Visual comparison
        metrics = ['fuel_efficiency_kmpl_mean', 'has_fault_mean', 
                  'idle_time_min_mean', 'driver_behavior_score_mean']
        metric_names = ['Fuel Efficiency', 'Fault Rate', 'Idle Time', 'Driver Score']
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=metric_names,
            specs=[[{'type': 'bar'}, {'type': 'bar'}],
                  [{'type': 'bar'}, {'type': 'bar'}]]
        )
        
        for i, (metric, name) in enumerate(zip(metrics, metric_names)):
            row = i // 2 + 1
            col = i % 2 + 1
            
            fig.add_trace(
                go.Bar(
                    x=comparison_data.index,
                    y=comparison_data[metric],
                    name=name,
                    text=comparison_data[metric].round(2),
                    textposition='auto'
                ),
                row=row, col=col
            )
        
        fig.update_layout(
            height=600,
            showlegend=False,
            title_text="HEV vs ICE Performance Comparison"
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_driver_behavior(self):
        """Render driver behavior analysis"""
        st.markdown("### 👤 Driver Behavior Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Driver score distribution
            fig = px.histogram(
                self.filtered_data,
                x='driver_behavior_score',
                color='vehicle_type',
                nbins=20,
                title='Driver Behavior Score Distribution',
                labels={'driver_behavior_score': 'Driver Score', 'count': 'Number of Trips'},
                opacity=0.7
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Driver score vs efficiency
            fig = px.scatter(
                self.filtered_data,
                x='driver_behavior_score',
                y='fuel_efficiency_kmpl',
                color='vehicle_type',
                trendline='ols',
                title='Driver Score vs Fuel Efficiency',
                labels={
                    'driver_behavior_score': 'Driver Behavior Score',
                    'fuel_efficiency_kmpl': 'Fuel Efficiency (km/L)'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Top and bottom drivers
        driver_stats = self.filtered_data.groupby('vehid').agg({
            'driver_behavior_score': 'mean',
            'fuel_efficiency_kmpl': 'mean',
            'has_fault': 'mean',
            'vehicle_type': 'first'
        }).reset_index()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🏆 Top 5 Drivers")
            top_drivers = driver_stats.sort_values('driver_behavior_score', ascending=False).head(5)
            st.dataframe(
                top_drivers[
                    ['vehid', 'vehicle_type', 'driver_behavior_score', 'fuel_efficiency_kmpl']
                ].rename(columns={
                    'vehid': 'Vehicle ID',
                    'vehicle_type': 'Type',
                    'driver_behavior_score': 'Driver Score',
                    'fuel_efficiency_kmpl': 'Efficiency'
                }),
                use_container_width=True
            )
        
        with col2:
            st.markdown("#### 📉 Bottom 5 Drivers")
            bottom_drivers = driver_stats.sort_values('driver_behavior_score').head(5)
            st.dataframe(
                bottom_drivers[
                    ['vehid', 'vehicle_type', 'driver_behavior_score', 'fuel_efficiency_kmpl']
                ].rename(columns={
                    'vehid': 'Vehicle ID',
                    'vehicle_type': 'Type',
                    'driver_behavior_score': 'Driver Score',
                    'fuel_efficiency_kmpl': 'Efficiency'
                }),
                use_container_width=True
            )
    
    def render_predictive_insights(self):
        """Render predictive insights"""
        st.markdown("### 🔮 Predictive Insights & Recommendations")
        
        # Calculate predictive metrics
        vehicle_performance = self.merged_data.groupby('vehid').agg({
            'fuel_efficiency_kmpl': 'mean',
            'has_fault': 'mean',
            'maintenance_flag': 'sum',
            'trip_distance_km': 'sum',
            'vehicle_type': 'first',
            'vehicle_class': 'first'
        }).reset_index()
        
        # Identify vehicles needing maintenance
        maintenance_threshold = 0.1  # 10% fault rate
        vehicles_needing_maintenance = vehicle_performance[
            (vehicle_performance['has_fault'] > maintenance_threshold) |
            (vehicle_performance['maintenance_flag'] > 0)
        ]
        
        # Calculate potential savings
        avg_improvement = 0.15  # 15% efficiency improvement with maintenance
        fuel_price = 1.2  # USD per liter
        avg_annual_distance = 15000  # km
        
        potential_savings = []
        for _, vehicle in vehicles_needing_maintenance.iterrows():
            current_efficiency = vehicle['fuel_efficiency_kmpl']
            improved_efficiency = current_efficiency * (1 + avg_improvement)
            
            current_fuel = avg_annual_distance / current_efficiency
            improved_fuel = avg_annual_distance / improved_efficiency
            
            savings = (current_fuel - improved_fuel) * fuel_price
            
            potential_savings.append({
                'Vehicle ID': vehicle['vehid'],
                'Type': vehicle['vehicle_type'],
                'Class': vehicle['vehicle_class'],
                'Current Efficiency': round(current_efficiency, 2),
                'Potential Efficiency': round(improved_efficiency, 2),
                'Annual Savings ($)': round(savings, 2)
            })
        
        savings_df = pd.DataFrame(potential_savings)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📊 Maintenance Priority")
            
            if len(vehicles_needing_maintenance) > 0:
                priority_df = vehicles_needing_maintenance.sort_values(
                    ['has_fault', 'maintenance_flag'], 
                    ascending=[False, False]
                ).head(10)
                
                st.dataframe(
                    priority_df[
                        ['vehid', 'vehicle_type', 'vehicle_class', 
                         'has_fault', 'maintenance_flag', 'fuel_efficiency_kmpl']
                    ].rename(columns={
                        'vehid': 'Vehicle ID',
                        'vehicle_type': 'Type',
                        'vehicle_class': 'Class',
                        'has_fault': 'Fault Rate',
                        'maintenance_flag': 'Maintenance Flags',
                        'fuel_efficiency_kmpl': 'Efficiency'
                    }),
                    use_container_width=True
                )
            else:
                st.success("🎉 No vehicles currently need urgent maintenance!")
        
        with col2:
            st.markdown("#### 💰 Potential Cost Savings")
            
            if len(savings_df) > 0:
                total_savings = savings_df['Annual Savings ($)'].sum()
                avg_savings = savings_df['Annual Savings ($)'].mean()
                
                st.metric("Total Potential Savings", f"${total_savings:,.2f}")
                st.metric("Average per Vehicle", f"${avg_savings:,.2f}")
                
                fig = px.bar(
                    savings_df.sort_values('Annual Savings ($)', ascending=False).head(10),
                    x='Vehicle ID',
                    y='Annual Savings ($)',
                    color='Type',
                    title='Top 10 Vehicles by Potential Savings'
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Recommendations
        st.markdown("#### 🎯 Actionable Recommendations")
        
        recommendations = [
            {
                'priority': 'High',
                'action': 'Schedule maintenance for high-fault vehicles',
                'impact': 'Reduce breakdowns by 40%',
                'timeline': 'Immediate'
            },
            {
                'priority': 'Medium',
                'action': 'Implement driver training program',
                'impact': 'Improve fuel efficiency by 8-12%',
                'timeline': '1 month'
            },
            {
                'priority': 'Low',
                'action': 'Optimize route planning for high-idle vehicles',
                'impact': 'Reduce idle time by 25%',
                'timeline': '3 months'
            }
        ]
        
        for rec in recommendations:
            with st.expander(f"[{rec['priority']}] {rec['action']}"):
                st.write(f"**Expected Impact:** {rec['impact']}")
                st.write(f"**Recommended Timeline:** {rec['timeline']}")
    
    def run(self):
        """Run the dashboard"""
        self.render_sidebar()
        self.render_header()
        
        # Create tabs for different analyses
        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
            "📊 Efficiency", 
            "📈 Usage Patterns", 
            "🔧 Maintenance",
            "⏱️ Idle Time",
            "🔄 HEV vs ICE",
            "👤 Driver Behavior",
            "🔮 Predictive Insights"
        ])
        
        with tab1:
            self.render_efficiency_analysis()
        
        with tab2:
            self.render_usage_patterns()
        
        with tab3:
            self.render_maintenance_analysis()
        
        with tab4:
            self.render_idle_time_analysis()
        
        with tab5:
            self.render_comparative_analysis()
        
        with tab6:
            self.render_driver_behavior()
        
        with tab7:
            self.render_predictive_insights()
        
        # Footer
        st.markdown("---")
        st.markdown(
            """
            <div style='text-align: center; color: gray;'>
                <p>Vehicle Telemetry Analytics Dashboard | Generated using Streamlit</p>
                <p>Data Updated: {}</p>
            </div>
            """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            unsafe_allow_html=True
        )

# Run the dashboard
if __name__ == "__main__":
    dashboard = VehicleTelemetryDashboard()
    dashboard.run()
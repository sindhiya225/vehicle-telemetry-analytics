import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

class PerformanceAnalyzer:
    def __init__(self, telemetry_data: pd.DataFrame, vehicle_data: pd.DataFrame):
        self.telemetry = telemetry_data
        self.vehicles = vehicle_data
        self.anomalies = None
        
    def analyze_performance_metrics(self) -> Dict:
        """Comprehensive performance analysis"""
        analysis_results = {
            'summary_stats': self._calculate_summary_statistics(),
            'efficiency_trends': self._analyze_efficiency_trends(),
            'anomaly_detection': self._detect_anomalies(),
            'correlation_analysis': self._perform_correlation_analysis(),
            'predictive_insights': self._generate_predictive_insights()
        }
        return analysis_results
    
    def _calculate_summary_statistics(self) -> Dict:
        """Calculate key performance indicators"""
        # Filter out summary rows
        trip_data = self.telemetry[~self.telemetry['trip_id'].str.contains('SUMMARY')]
        
        summary = {
            'total_trips': len(trip_data),
            'unique_vehicles': trip_data['vehid'].nunique(),
            'total_distance_km': trip_data['trip_distance_km'].sum(),
            'total_fuel_consumed_l': trip_data['fuel_consumed_l'].sum(),
            'avg_fuel_efficiency': trip_data['fuel_efficiency_kmpl'].mean(),
            'avg_trip_distance': trip_data['trip_distance_km'].mean(),
            'avg_trip_duration': trip_data['trip_duration_min'].mean(),
            'total_idle_time_hours': trip_data['idle_time_min'].sum() / 60,
            'fault_rate': trip_data['has_fault'].mean() * 100,
            'maintenance_flag_rate': trip_data['maintenance_flag'].mean() * 100
        }
        
        # By vehicle type
        for vtype in trip_data['vehicle_type'].unique():
            vtype_data = trip_data[trip_data['vehicle_type'] == vtype]
            summary[f'{vtype}_avg_efficiency'] = vtype_data['fuel_efficiency_kmpl'].mean()
            summary[f'{vtype}_fault_rate'] = vtype_data['has_fault'].mean() * 100
        
        return summary
    
    def _analyze_efficiency_trends(self) -> Dict:
        """Analyze fuel efficiency trends"""
        # Merge with vehicle data for better analysis
        merged_data = pd.merge(
            self.telemetry,
            self.vehicles[['vehid', 'engine_type', 'displacement_l', 'is_turbo', 'weight_category']],
            on='vehid',
            how='left'
        )
        
        trends = {}
        
        # Daily efficiency trends
        merged_data['date'] = pd.to_datetime(merged_data['timestamp']).dt.date
        daily_trends = merged_data.groupby(['date', 'vehicle_type']).agg({
            'fuel_efficiency_kmpl': 'mean',
            'trip_distance_km': 'sum',
            'has_fault': 'mean'
        }).reset_index()
        
        trends['daily_efficiency'] = daily_trends
        
        # Efficiency by engine characteristics
        engine_efficiency = merged_data.groupby(['engine_type', 'is_turbo', 'weight_category']).agg({
            'fuel_efficiency_kmpl': ['mean', 'std', 'count'],
            'displacement_l': 'mean'
        }).reset_index()
        
        engine_efficiency.columns = ['_'.join(col).strip('_') for col in engine_efficiency.columns.values]
        trends['engine_efficiency'] = engine_efficiency
        
        # Identify efficiency outliers
        efficiency_zscore = np.abs(
            (merged_data['fuel_efficiency_kmpl'] - merged_data['fuel_efficiency_kmpl'].mean()) / 
            merged_data['fuel_efficiency_kmpl'].std()
        )
        outliers = merged_data[efficiency_zscore > 3]
        trends['efficiency_outliers'] = outliers[['vehid', 'fuel_efficiency_kmpl', 'trip_distance_km']]
        
        return trends
    
    def _detect_anomalies(self) -> Dict:
        """Detect anomalous vehicle behavior using machine learning"""
        # Prepare features for anomaly detection
        features = self.telemetry[~self.telemetry['trip_id'].str.contains('SUMMARY')].copy()
        
        # Select relevant features
        anomaly_features = features[[
            'fuel_efficiency_kmpl',
            'avg_speed_kmph',
            'avg_rpm',
            'avg_engine_temp_c',
            'idle_time_min',
            'driver_behavior_score'
        ]].fillna(method='ffill')
        
        # Standardize features
        scaler = StandardScaler()
        scaled_features = scaler.fit_transform(anomaly_features)
        
        # Use Isolation Forest for anomaly detection
        iso_forest = IsolationForest(
            contamination=0.1,  # Expect 10% anomalies
            random_state=42
        )
        
        predictions = iso_forest.fit_predict(scaled_features)
        
        # Mark anomalies
        features['is_anomaly'] = predictions == -1
        features['anomaly_score'] = iso_forest.decision_function(scaled_features)
        
        # Get anomaly details
        anomalies = features[features['is_anomaly']]
        
        anomaly_analysis = {
            'total_anomalies': len(anomalies),
            'anomaly_rate': len(anomalies) / len(features) * 100,
            'anomalies_by_type': anomalies.groupby('vehicle_type').size().to_dict(),
            'top_anomalous_vehicles': anomalies['vehid'].value_counts().head(10).to_dict(),
            'anomaly_features': self._analyze_anomaly_patterns(anomalies)
        }
        
        self.anomalies = anomalies
        return anomaly_analysis
    
    def _analyze_anomaly_patterns(self, anomalies: pd.DataFrame) -> Dict:
        """Analyze patterns in detected anomalies"""
        patterns = {}
        
        # Common characteristics of anomalies
        patterns['avg_fuel_efficiency'] = anomalies['fuel_efficiency_kmpl'].mean()
        patterns['avg_speed'] = anomalies['avg_speed_kmph'].mean()
        patterns['avg_engine_temp'] = anomalies['avg_engine_temp_c'].mean()
        patterns['fault_rate'] = anomalies['has_fault'].mean() * 100
        
        # Time patterns
        anomalies['hour'] = pd.to_datetime(anomalies['timestamp']).dt.hour
        patterns['hourly_distribution'] = anomalies['hour'].value_counts().sort_index().to_dict()
        
        # Vehicle type patterns
        patterns['vehicle_type_distribution'] = anomalies['vehicle_type'].value_counts().to_dict()
        
        return patterns
    
    def _perform_correlation_analysis(self) -> Dict:
        """Perform correlation analysis between different metrics"""
        # Select numerical features
        numerical_data = self.telemetry.select_dtypes(include=[np.number])
        
        # Calculate correlation matrix
        correlation_matrix = numerical_data.corr()
        
        # Find strong correlations
        strong_correlations = {}
        for col in correlation_matrix.columns:
            strong_corrs = correlation_matrix[col][
                (correlation_matrix[col].abs() > 0.5) & 
                (correlation_matrix[col].abs() < 1.0)
            ]
            if len(strong_corrs) > 0:
                strong_correlations[col] = strong_corrs.to_dict()
        
        # Key insights from correlations
        insights = {
            'fuel_efficiency_correlations': correlation_matrix['fuel_efficiency_kmpl'].sort_values(
                key=abs, ascending=False
            ).head(10).to_dict(),
            'fault_correlations': correlation_matrix['has_fault'].sort_values(
                key=abs, ascending=False
            ).head(10).to_dict(),
            'strong_correlations': strong_correlations,
            'correlation_matrix': correlation_matrix
        }
        
        return insights
    
    def _generate_predictive_insights(self) -> Dict:
        """Generate predictive insights for maintenance and efficiency"""
        # Predict maintenance needs based on patterns
        maintenance_data = self.telemetry[
            ['vehid', 'fuel_efficiency_kmpl', 'avg_engine_temp_c', 
             'avg_rpm', 'idle_time_min', 'has_fault', 'maintenance_flag']
        ].copy()
        
        # Calculate degradation indicators
        maintenance_data['efficiency_degradation'] = (
            maintenance_data.groupby('vehid')['fuel_efficiency_kmpl']
            .transform(lambda x: (x.iloc[0] - x.iloc[-1]) / x.iloc[0] * 100 
                      if len(x) > 1 else 0)
        )
        
        # Identify vehicles needing maintenance
        maintenance_needed = maintenance_data[
            (maintenance_data['maintenance_flag']) |
            (maintenance_data['efficiency_degradation'] > 10) |
            (maintenance_data['has_fault'])
        ]['vehid'].unique()
        
        # Predict fuel efficiency for different scenarios
        vehicle_groups = self.telemetry.groupby(['vehicle_type', 'vehicle_class'])
        
        efficiency_predictions = {}
        for (vtype, vclass), group in vehicle_groups:
            current_eff = group['fuel_efficiency_kmpl'].mean()
            
            # Predict improvement with maintenance
            maintenance_improvement = current_eff * 1.15  # 15% improvement
            
            # Predict with better driving behavior
            behavior_improvement = current_eff * 1.10  # 10% improvement
            
            efficiency_predictions[f"{vtype}_{vclass}"] = {
                'current_efficiency': current_eff,
                'with_maintenance': maintenance_improvement,
                'with_better_driving': behavior_improvement,
                'potential_improvement': maintenance_improvement - current_eff
            }
        
        predictive_insights = {
            'vehicles_needing_maintenance': list(maintenance_needed),
            'maintenance_count': len(maintenance_needed),
            'efficiency_predictions': efficiency_predictions,
            'cost_savings_potential': self._calculate_cost_savings(efficiency_predictions),
            'recommended_actions': self._generate_recommendations()
        }
        
        return predictive_insights
    
    def _calculate_cost_savings(self, predictions: Dict) -> Dict:
        """Calculate potential cost savings from improvements"""
        # Average fuel price per liter
        fuel_price = 1.2  # USD per liter
        avg_distance_per_vehicle = 15000  # km per year
        
        cost_savings = {}
        
        for vehicle, data in predictions.items():
            current_consumption = avg_distance_per_vehicle / data['current_efficiency']
            improved_consumption = avg_distance_per_vehicle / data['with_maintenance']
            
            current_cost = current_consumption * fuel_price
            improved_cost = improved_consumption * fuel_price
            
            savings = current_cost - improved_cost
            
            cost_savings[vehicle] = {
                'current_annual_cost': round(current_cost, 2),
                'improved_annual_cost': round(improved_cost, 2),
                'annual_savings': round(savings, 2),
                'savings_percentage': round((savings / current_cost) * 100, 1)
            }
        
        # Total potential savings
        total_savings = sum([v['annual_savings'] for v in cost_savings.values()])
        
        return {
            'per_vehicle_savings': cost_savings,
            'total_potential_savings': round(total_savings, 2),
            'average_savings_per_vehicle': round(total_savings / len(cost_savings), 2)
        }
    
    def _generate_recommendations(self) -> List[Dict]:
        """Generate actionable recommendations"""
        recommendations = []
        
        # Based on anomalies
        if self.anomalies is not None and len(self.anomalies) > 0:
            rec = {
                'type': 'Anomaly Detection',
                'priority': 'High',
                'action': 'Investigate anomalous vehicles',
                'vehicles': self.anomalies['vehid'].unique()[:5].tolist(),
                'expected_impact': 'Reduce unexpected failures by 30%'
            }
            recommendations.append(rec)
        
        # Based on efficiency
        efficiency_stats = self.telemetry.groupby('vehicle_type')['fuel_efficiency_kmpl'].mean()
        least_efficient = efficiency_stats.idxmin()
        
        rec = {
            'type': 'Fuel Efficiency',
            'priority': 'Medium',
            'action': f'Optimize driving patterns for {least_efficient} vehicles',
            'vehicles': 'All vehicles in category',
            'expected_impact': f'Improve efficiency by 10-15% for {least_efficient}'
        }
        recommendations.append(rec)
        
        # Based on maintenance flags
        maintenance_needed = self.telemetry[self.telemetry['maintenance_flag']]['vehid'].unique()
        if len(maintenance_needed) > 0:
            rec = {
                'type': 'Preventive Maintenance',
                'priority': 'High',
                'action': 'Schedule preventive maintenance',
                'vehicles': maintenance_needed[:10].tolist(),
                'expected_impact': 'Reduce breakdowns by 40%'
            }
            recommendations.append(rec)
        
        return recommendations
    
    def generate_report(self) -> str:
        """Generate a comprehensive analysis report"""
        analysis = self.analyze_performance_metrics()
        
        report = f"""
        VEHICLE TELEMETRY PERFORMANCE ANALYSIS REPORT
        =============================================
        
        Executive Summary:
        - Total Trips Analyzed: {analysis['summary_stats']['total_trips']:,}
        - Unique Vehicles: {analysis['summary_stats']['unique_vehicles']}
        - Total Distance: {analysis['summary_stats']['total_distance_km']:,.0f} km
        - Average Fuel Efficiency: {analysis['summary_stats']['avg_fuel_efficiency']:.2f} km/L
        
        Key Findings:
        1. Fuel Efficiency Trends:
           - HEV vehicles show {analysis['efficiency_trends']['engine_efficiency'].query("engine_type == 'Hybrid'")['fuel_efficiency_kmpl_mean'].mean():.2f} km/L vs 
             ICE vehicles at {analysis['efficiency_trends']['engine_efficiency'].query("engine_type == 'Gasoline'")['fuel_efficiency_kmpl_mean'].mean():.2f} km/L
        
        2. Anomaly Detection:
           - Detected {analysis['anomaly_detection']['total_anomalies']} anomalous trips ({analysis['anomaly_detection']['anomaly_rate']:.1f}%)
           - Top anomalous vehicle types: {list(analysis['anomaly_detection']['anomalies_by_type'].keys())}
        
        3. Predictive Insights:
           - {analysis['predictive_insights']['maintenance_count']} vehicles need maintenance
           - Potential annual savings: ${analysis['predictive_insights']['cost_savings_potential']['total_potential_savings']:,.2f}
        
        Recommendations:
        """
        
        for i, rec in enumerate(analysis['predictive_insights']['recommended_actions'], 1):
            report += f"\n{i}. [{rec['priority']}] {rec['action']}"
            report += f"\n   Impact: {rec['expected_impact']}"
        
        return report
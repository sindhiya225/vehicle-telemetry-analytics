"""
Maintenance Predictor Module
Predictive maintenance algorithms and failure prediction
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import warnings
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
warnings.filterwarnings('ignore')

@dataclass
class MaintenancePrediction:
    """Data class for maintenance predictions"""
    vehid: int
    vehicle_type: str
    predicted_failure: bool
    failure_probability: float
    predicted_failure_type: str
    predicted_failure_date: datetime
    confidence: float
    recommended_actions: List[str]
    urgency: str

class MaintenancePredictor:
    """Predictive maintenance and failure prediction system"""
    
    def __init__(self, telemetry_data: pd.DataFrame, 
                 vehicle_data: pd.DataFrame,
                 maintenance_data: pd.DataFrame = None):
        """
        Initialize maintenance predictor
        
        Args:
            telemetry_data: DataFrame with telemetry data
            vehicle_data: DataFrame with vehicle master data
            maintenance_data: DataFrame with maintenance history (optional)
        """
        self.telemetry = telemetry_data
        self.vehicles = vehicle_data
        self.maintenance = maintenance_data
        self.models = {}
        self.scaler = StandardScaler()
        
        # Preprocess data
        self.preprocessed_data = self.preprocess_data()
        
    def preprocess_data(self) -> pd.DataFrame:
        """Preprocess data for maintenance prediction"""
        
        # Merge telemetry with vehicle data
        merged_data = pd.merge(
            self.telemetry,
            self.vehicles[['vehid', 'vehicle_type', 'vehicle_class', 'engine_type',
                          'displacement_l', 'is_turbo', 'is_hybrid']],
            on='vehid',
            how='left'
        )
        
        # Convert timestamp to datetime if needed
        if 'timestamp' in merged_data.columns:
            if not pd.api.types.is_datetime64_any_dtype(merged_data['timestamp']):
                merged_data['timestamp'] = pd.to_datetime(merged_data['timestamp'])
        
        # Calculate features for maintenance prediction
        merged_data = self.calculate_features(merged_data)
        
        return merged_data
    
    def calculate_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate features for maintenance prediction
        
        Args:
            data: Merged telemetry and vehicle data
            
        Returns:
            DataFrame with calculated features
        """
        # Make a copy to avoid modifying original
        df = data.copy()
        
        # 1. Engine stress indicators
        df['engine_stress'] = (
            df['avg_rpm'] / 1000 * 
            df['avg_engine_temp_c'] / 100 *
            (1.2 if 'is_turbo' in df.columns and df['is_turbo'].any() else 1.0)
        )
        
        # 2. Efficiency degradation (rolling window)
        if 'fuel_efficiency_kmpl' in df.columns:
            df.sort_values(['vehid', 'timestamp'], inplace=True)
            df['efficiency_ma'] = df.groupby('vehid')['fuel_efficiency_kmpl'].transform(
                lambda x: x.rolling(window=10, min_periods=5).mean()
            )
            df['efficiency_degradation'] = (
                df.groupby('vehid')['efficiency_ma'].transform('first') - df['efficiency_ma']
            ) / df.groupby('vehid')['efficiency_ma'].transform('first') * 100
        
        # 3. Temperature anomalies
        if 'avg_engine_temp_c' in df.columns:
            df['temp_anomaly'] = df['avg_engine_temp_c'] > 105
        
        # 4. Vibration indicators (simulated from RPM variations)
        if 'avg_rpm' in df.columns:
            df['rpm_variance'] = df.groupby('vehid')['avg_rpm'].transform(
                lambda x: x.rolling(window=5, min_periods=3).std()
            )
        
        # 5. Brake wear indicator (simulated)
        if 'harsh_braking_count' in df.columns:
            df['brake_wear_indicator'] = df.groupby('vehid')['harsh_braking_count'].transform(
                lambda x: x.rolling(window=20, min_periods=10).sum()
            )
        
        # 6. Time-based features
        if 'timestamp' in df.columns:
            df['hour_of_day'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.dayofweek
            df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        
        # 7. Trip characteristics
        df['trip_intensity'] = df['trip_distance_km'] * df['avg_speed_kmph'] / 100
        
        # 8. Fault history (rolling count)
        if 'has_fault' in df.columns:
            df['fault_history'] = df.groupby('vehid')['has_fault'].transform(
                lambda x: x.rolling(window=50, min_periods=25).sum()
            )
        
        # 9. Maintenance flag history
        if 'maintenance_flag' in df.columns:
            df['maintenance_history'] = df.groupby('vehid')['maintenance_flag'].transform(
                lambda x: x.rolling(window=100, min_periods=50).sum()
            )
        
        # 10. Driver behavior impact
        if 'driver_behavior_score' in df.columns:
            df['driver_impact'] = 1 - df['driver_behavior_score']
        
        return df
    
    def train_predictive_model(self, target_column: str = 'maintenance_flag',
                               test_size: float = 0.2) -> Dict:
        """
        Train predictive maintenance model
        
        Args:
            target_column: Column to predict
            test_size: Proportion of data for testing
            
        Returns:
            Dictionary with model performance metrics
        """
        # Prepare features and target
        feature_columns = [
            'engine_stress', 'efficiency_degradation', 'temp_anomaly',
            'rpm_variance', 'brake_wear_indicator', 'trip_intensity',
            'fault_history', 'maintenance_history', 'driver_impact',
            'hour_of_day', 'is_weekend'
        ]
        
        # Filter columns that exist in data
        available_features = [col for col in feature_columns if col in self.preprocessed_data.columns]
        
        if not available_features:
            raise ValueError("No features available for training")
        
        # Prepare data
        X = self.preprocessed_data[available_features].fillna(0)
        y = self.preprocessed_data[target_column].fillna(0).astype(int)
        
        if len(y.unique()) < 2:
            raise ValueError(f"Target column '{target_column}' has insufficient variance")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42, stratify=y
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train Random Forest classifier
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42,
            class_weight='balanced'
        )
        
        model.fit(X_train_scaled, y_train)
        
        # Make predictions
        y_pred = model.predict(X_test_scaled)
        y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]
        
        # Calculate metrics
        from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
        
        metrics = {
            'accuracy': accuracy_score(y_test, y_pred),
            'precision': precision_score(y_test, y_pred, zero_division=0),
            'recall': recall_score(y_test, y_pred, zero_division=0),
            'f1_score': f1_score(y_test, y_pred, zero_division=0),
            'roc_auc': roc_auc_score(y_test, y_pred_proba) if len(y_test.unique()) > 1 else 0,
            'feature_importance': dict(zip(available_features, model.feature_importances_))
        }
        
        # Store model
        self.models['maintenance_predictor'] = {
            'model': model,
            'scaler': self.scaler,
            'features': available_features,
            'metrics': metrics
        }
        
        return metrics
    
    def predict_maintenance_needs(self, days_ahead: int = 30) -> pd.DataFrame:
        """
        Predict maintenance needs for the next N days
        
        Args:
            days_ahead: Number of days to predict ahead
            
        Returns:
            DataFrame with maintenance predictions
        """
        if 'maintenance_predictor' not in self.models:
            raise ValueError("Model not trained. Call train_predictive_model() first.")
        
        model_info = self.models['maintenance_predictor']
        model = model_info['model']
        scaler = model_info['scaler']
        features = model_info['features']
        
        # Get latest data for each vehicle
        latest_data = self.preprocessed_data.sort_values('timestamp').groupby('vehid').last().reset_index()
        
        # Prepare features for prediction
        X = latest_data[features].fillna(0)
        X_scaled = scaler.transform(X)
        
        # Make predictions
        predictions = model.predict(X_scaled)
        probabilities = model.predict_proba(X_scaled)[:, 1]
        
        # Create predictions DataFrame
        predictions_df = pd.DataFrame({
            'vehid': latest_data['vehid'],
            'vehicle_type': latest_data['vehicle_type'],
            'predicted_maintenance': predictions,
            'maintenance_probability': probabilities,
            'prediction_date': datetime.now(),
            'prediction_horizon_days': days_ahead
        })
        
        # Add vehicle information
        predictions_df = pd.merge(
            predictions_df,
            self.vehicles[['vehid', 'vehicle_class', 'engine_type', 'odometer_km', 'last_service_km']],
            on='vehid',
            how='left'
        )
        
        # Calculate urgency levels
        predictions_df['urgency'] = predictions_df.apply(
            lambda row: self.calculate_urgency(row), axis=1
        )
        
        # Generate recommendations
        predictions_df['recommendations'] = predictions_df.apply(
            lambda row: self.generate_recommendations(row), axis=1
        )
        
        # Filter only vehicles needing maintenance
        maintenance_needed = predictions_df[predictions_df['predicted_maintenance'] == 1].copy()
        
        # Sort by probability and urgency
        maintenance_needed['urgency_score'] = maintenance_needed['urgency'].map({
            'CRITICAL': 3, 'HIGH': 2, 'MEDIUM': 1, 'LOW': 0
        })
        maintenance_needed = maintenance_needed.sort_values(
            ['maintenance_probability', 'urgency_score'], ascending=[False, False]
        )
        
        return maintenance_needed
    
    def calculate_urgency(self, row: pd.Series) -> str:
        """Calculate maintenance urgency level"""
        
        probability = row['maintenance_probability']
        efficiency_degradation = row.get('efficiency_degradation', 0) if isinstance(row, pd.Series) else 0
        
        if probability > 0.8 or efficiency_degradation > 20:
            return 'CRITICAL'
        elif probability > 0.6 or efficiency_degradation > 15:
            return 'HIGH'
        elif probability > 0.4 or efficiency_degradation > 10:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def generate_recommendations(self, row: pd.Series) -> List[str]:
        """Generate maintenance recommendations based on prediction"""
        
        recommendations = []
        vehicle_type = row['vehicle_type']
        probability = row['maintenance_probability']
        
        # General recommendations based on probability
        if probability > 0.7:
            recommendations.append("Schedule immediate diagnostic check")
            recommendations.append("Consider preventive maintenance within 7 days")
        elif probability > 0.5:
            recommendations.append("Schedule maintenance within 14 days")
            recommendations.append("Monitor vehicle performance closely")
        else:
            recommendations.append("Continue regular monitoring")
            recommendations.append("Next scheduled maintenance based on mileage")
        
        # Vehicle-type specific recommendations
        if vehicle_type == 'ICE':
            recommendations.append("Check engine oil and filter condition")
            recommendations.append("Inspect spark plugs and ignition system")
        elif vehicle_type == 'HEV':
            recommendations.append("Check hybrid battery health")
            recommendations.append("Inspect regenerative braking system")
        elif vehicle_type == 'BEV':
            recommendations.append("Check battery management system")
            recommendations.append("Inspect charging system")
        
        # Efficiency-related recommendations
        if 'efficiency_degradation' in row and row['efficiency_degradation'] > 10:
            recommendations.append("Check air filter and fuel system")
            recommendations.append("Verify tire pressure and alignment")
        
        return recommendations
    
    def detect_anomalies(self, method: str = 'isolation_forest') -> pd.DataFrame:
        """
        Detect anomalous vehicle behavior
        
        Args:
            method: Anomaly detection method ('isolation_forest', 'z_score')
            
        Returns:
            DataFrame with detected anomalies
        """
        
        if method == 'isolation_forest':
            return self.detect_anomalies_isolation_forest()
        elif method == 'z_score':
            return self.detect_anomalies_z_score()
        else:
            raise ValueError(f"Unknown anomaly detection method: {method}")
    
    def detect_anomalies_isolation_forest(self) -> pd.DataFrame:
        """Detect anomalies using Isolation Forest"""
        
        # Select features for anomaly detection
        feature_columns = [
            'fuel_efficiency_kmpl', 'avg_engine_temp_c', 'avg_rpm',
            'trip_distance_km', 'idle_time_min'
        ]
        
        available_features = [col for col in feature_columns if col in self.preprocessed_data.columns]
        
        if not available_features:
            return pd.DataFrame()
        
        # Prepare data
        X = self.preprocessed_data[available_features].fillna(0)
        
        # Train Isolation Forest
        iso_forest = IsolationForest(
            contamination=0.1,  # Expect 10% anomalies
            random_state=42,
            n_estimators=100
        )
        
        predictions = iso_forest.fit_predict(X)
        anomaly_scores = iso_forest.decision_function(X)
        
        # Create anomalies DataFrame
        anomalies = self.preprocessed_data.copy()
        anomalies['is_anomaly'] = predictions == -1
        anomalies['anomaly_score'] = anomaly_scores
        
        # Filter anomalies
        detected_anomalies = anomalies[anomalies['is_anomaly']].copy()
        
        if len(detected_anomalies) == 0:
            return pd.DataFrame()
        
        # Add vehicle information
        detected_anomalies = pd.merge(
            detected_anomalies,
            self.vehicles[['vehid', 'vehicle_type', 'vehicle_class']],
            on='vehid',
            how='left'
        )
        
        # Calculate anomaly severity
        detected_anomalies['severity'] = pd.cut(
            detected_anomalies['anomaly_score'],
            bins=[-1, -0.5, -0.2, 0],
            labels=['HIGH', 'MEDIUM', 'LOW'],
            include_lowest=True
        )
        
        # Sort by severity and timestamp
        detected_anomalies = detected_anomalies.sort_values(
            ['severity', 'timestamp'], ascending=[True, False]
        )
        
        return detected_anomalies[['vehid', 'vehicle_type', 'vehicle_class', 'timestamp',
                                   'anomaly_score', 'severity', 'fuel_efficiency_kmpl',
                                   'avg_engine_temp_c', 'avg_rpm']]
    
    def detect_anomalies_z_score(self) -> pd.DataFrame:
        """Detect anomalies using Z-score method"""
        
        anomalies_list = []
        
        # Define metrics to check for anomalies
        metrics_to_check = {
            'fuel_efficiency_kmpl': {'threshold': 3, 'direction': 'both'},
            'avg_engine_temp_c': {'threshold': 3, 'direction': 'above'},
            'avg_rpm': {'threshold': 3, 'direction': 'both'},
            'idle_time_min': {'threshold': 3, 'direction': 'above'}
        }
        
        for metric, config in metrics_to_check.items():
            if metric in self.preprocessed_data.columns:
                threshold = config['threshold']
                direction = config['direction']
                
                # Calculate z-scores by vehicle type for better comparison
                for vehicle_type in self.preprocessed_data['vehicle_type'].unique():
                    type_data = self.preprocessed_data[self.preprocessed_data['vehicle_type'] == vehicle_type]
                    
                    if len(type_data) > 10:  # Need sufficient data
                        mean_val = type_data[metric].mean()
                        std_val = type_data[metric].std()
                        
                        if std_val > 0:  # Avoid division by zero
                            z_scores = (type_data[metric] - mean_val) / std_val
                            
                            if direction == 'above':
                                anomalies = type_data[z_scores > threshold].copy()
                            elif direction == 'below':
                                anomalies = type_data[z_scores < -threshold].copy()
                            else:  # both
                                anomalies = type_data[abs(z_scores) > threshold].copy()
                            
                            if not anomalies.empty:
                                anomalies['anomaly_metric'] = metric
                                anomalies['z_score'] = z_scores[anomalies.index]
                                anomalies['severity'] = pd.cut(
                                    abs(anomalies['z_score']),
                                    bins=[threshold, threshold+1, threshold+2, 10],
                                    labels=['LOW', 'MEDIUM', 'HIGH']
                                )
                                anomalies_list.append(anomalies)
        
        if not anomalies_list:
            return pd.DataFrame()
        
        # Combine all anomalies
        all_anomalies = pd.concat(anomalies_list, ignore_index=True)
        
        # Remove duplicates (same record might be anomalous for multiple metrics)
        all_anomalies = all_anomalies.sort_values('z_score', key=abs, ascending=False)
        all_anomalies = all_anomalies.drop_duplicates(subset=['vehid', 'timestamp'])
        
        # Add vehicle information
        all_anomalies = pd.merge(
            all_anomalies,
            self.vehicles[['vehid', 'vehicle_type', 'vehicle_class']],
            on='vehid',
            how='left',
            suffixes=('', '_vehicle')
        )
        
        return all_anomalies[['vehid', 'vehicle_type', 'vehicle_class', 'timestamp',
                              'anomaly_metric', 'z_score', 'severity']]
    
    def predict_failure_types(self) -> pd.DataFrame:
        """
        Predict specific types of failures
        
        Returns:
            DataFrame with failure type predictions
        """
        # This would typically use a multi-class classification model
        # For now, implement a rule-based approach
        
        predictions = []
        
        for _, vehicle in self.vehicles.iterrows():
            vehid = vehicle['vehid']
            
            # Get recent telemetry for this vehicle
            vehicle_telemetry = self.preprocessed_data[
                self.preprocessed_data['vehid'] == vehid
            ].tail(50)  # Last 50 records
            
            if len(vehicle_telemetry) < 10:
                continue
            
            # Check for different failure patterns
            failure_types = []
            probabilities = []
            
            # 1. Engine failure indicators
            if 'avg_engine_temp_c' in vehicle_telemetry.columns:
                high_temp_count = (vehicle_telemetry['avg_engine_temp_c'] > 110).sum()
                if high_temp_count > 5:
                    failure_types.append('ENGINE_OVERHEATING')
                    probabilities.append(min(0.8, high_temp_count / len(vehicle_telemetry)))
            
            # 2. Fuel system issues
            if 'fuel_efficiency_kmpl' in vehicle_telemetry.columns:
                efficiency_std = vehicle_telemetry['fuel_efficiency_kmpl'].std()
                if efficiency_std > 5:  # High variability
                    failure_types.append('FUEL_SYSTEM_ISSUE')
                    probabilities.append(min(0.6, efficiency_std / 10))
            
            # 3. Brake system issues
            if 'harsh_braking_count' in vehicle_telemetry.columns:
                harsh_braking_total = vehicle_telemetry['harsh_braking_count'].sum()
                if harsh_braking_total > 20:
                    failure_types.append('BRAKE_SYSTEM_ISSUE')
                    probabilities.append(min(0.7, harsh_braking_total / 100))
            
            # 4. Electrical system issues
            if 'battery_voltage' in vehicle_telemetry.columns:
                low_voltage_count = (vehicle_telemetry['battery_voltage'] < 12.0).sum()
                if low_voltage_count > 10:
                    failure_types.append('ELECTRICAL_SYSTEM_ISSUE')
                    probabilities.append(min(0.9, low_voltage_count / len(vehicle_telemetry)))
            
            # 5. Transmission issues (simulated from RPM patterns)
            if 'avg_rpm' in vehicle_telemetry.columns:
                rpm_variance = vehicle_telemetry['avg_rpm'].std()
                if rpm_variance > 500:
                    failure_types.append('TRANSMISSION_ISSUE')
                    probabilities.append(min(0.5, rpm_variance / 1000))
            
            if failure_types:
                # Take the highest probability failure
                max_prob_idx = np.argmax(probabilities)
                
                predictions.append({
                    'vehid': vehid,
                    'vehicle_type': vehicle['vehicle_type'],
                    'vehicle_class': vehicle['vehicle_class'],
                    'predicted_failure_type': failure_types[max_prob_idx],
                    'failure_probability': probabilities[max_prob_idx],
                    'prediction_date': datetime.now(),
                    'confidence': min(0.9, len(vehicle_telemetry) / 100),  # Based on data quantity
                    'additional_risks': ', '.join([f for i, f in enumerate(failure_types) if i != max_prob_idx])
                })
        
        if not predictions:
            return pd.DataFrame()
        
        predictions_df = pd.DataFrame(predictions)
        
        # Sort by probability
        predictions_df = predictions_df.sort_values('failure_probability', ascending=False)
        
        return predictions_df
    
    def generate_maintenance_schedule(self, planning_horizon: int = 90) -> pd.DataFrame:
        """
        Generate optimal maintenance schedule
        
        Args:
            planning_horizon: Planning horizon in days
            
        Returns:
            DataFrame with maintenance schedule
        """
        schedule = []
        current_date = datetime.now()
        
        for _, vehicle in self.vehicles.iterrows():
            vehid = vehicle['vehid']
            
            # Get vehicle usage pattern
            vehicle_telemetry = self.preprocessed_data[self.preprocessed_data['vehid'] == vehid]
            
            if len(vehicle_telemetry) == 0:
                continue
            
            # Calculate average daily distance
            if 'timestamp' in vehicle_telemetry.columns:
                first_date = vehicle_telemetry['timestamp'].min()
                last_date = vehicle_telemetry['timestamp'].max()
                days_observed = (last_date - first_date).days + 1
                
                if days_observed > 0:
                    total_distance = vehicle_telemetry['trip_distance_km'].sum()
                    avg_daily_distance = total_distance / days_observed
                    
                    # Get last service information
                    last_service_km = vehicle.get('last_service_km', 0)
                    service_interval_km = vehicle.get('service_interval_km', 15000)
                    
                    # Calculate when next service is due
                    km_since_last_service = vehicle.get('odometer_km', 0) - last_service_km
                    km_to_next_service = max(0, service_interval_km - km_since_last_service)
                    
                    if avg_daily_distance > 0:
                        days_to_next_service = km_to_next_service / avg_daily_distance
                        
                        # Adjust based on predicted maintenance needs
                        if 'maintenance_predictor' in self.models:
                            # Get prediction for this vehicle
                            vehicle_features = self.preprocessed_data[
                                self.preprocessed_data['vehid'] == vehid
                            ][self.models['maintenance_predictor']['features']].tail(1)
                            
                            if not vehicle_features.empty:
                                scaled_features = self.models['maintenance_predictor']['scaler'].transform(
                                    vehicle_features.fillna(0)
                                )
                                probability = self.models['maintenance_predictor']['model'].predict_proba(
                                    scaled_features
                                )[0, 1]
                                
                                # Adjust schedule based on prediction probability
                                if probability > 0.7:
                                    days_to_next_service = min(days_to_next_service, 7)
                                elif probability > 0.5:
                                    days_to_next_service = min(days_to_next_service, 14)
                        
                        # Only schedule if within planning horizon
                        if days_to_next_service <= planning_horizon:
                            schedule.append({
                                'vehid': vehid,
                                'vehicle_type': vehicle['vehicle_type'],
                                'vehicle_class': vehicle['vehicle_class'],
                                'scheduled_date': current_date + timedelta(days=days_to_next_service),
                                'scheduled_days_from_now': days_to_next_service,
                                'service_type': 'PREVENTIVE_MAINTENANCE',
                                'priority': 'HIGH' if days_to_next_service < 7 else 
                                          'MEDIUM' if days_to_next_service < 30 else 'LOW',
                                'estimated_duration_hours': 4,
                                'estimated_cost': 200,
                                'notes': f'Based on {avg_daily_distance:.1f} km/day average usage'
                            })
        
        if not schedule:
            return pd.DataFrame()
        
        schedule_df = pd.DataFrame(schedule)
        
        # Sort by scheduled date
        schedule_df = schedule_df.sort_values('scheduled_date')
        
        return schedule_df
    
    def calculate_maintenance_cost_benefit(self, investment: float = 10000) -> Dict:
        """
        Calculate cost-benefit of maintenance investment
        
        Args:
            investment: Proposed maintenance investment amount
            
        Returns:
            Dictionary with cost-benefit analysis
        """
        # Calculate current maintenance costs
        if self.maintenance is not None and not self.maintenance.empty:
            current_annual_cost = self.maintenance['cost'].sum()
            breakdown_by_type = self.maintenance.groupby('maintenance_type')['cost'].sum().to_dict()
        else:
            # Estimate based on vehicle count
            vehicle_count = len(self.vehicles)
            current_annual_cost = vehicle_count * 1200  # $1200 per vehicle annually
            breakdown_by_type = {
                'Preventive': vehicle_count * 600,
                'Corrective': vehicle_count * 400,
                'Emergency': vehicle_count * 200
            }
        
        # Calculate current downtime costs (estimated)
        if 'maintenance_flag' in self.preprocessed_data.columns:
            maintenance_days = self.preprocessed_data['maintenance_flag'].sum()
            daily_operating_cost = self.telemetry['trip_distance_km'].sum() / 365 * 0.85  # $0.85 per km
            current_downtime_cost = maintenance_days * daily_operating_cost
        else:
            current_downtime_cost = current_annual_cost * 0.2  # 20% of maintenance cost
        
        # Projected improvements with investment
        # Assumptions:
        # - 30% reduction in corrective maintenance
        # - 50% reduction in emergency maintenance
        # - 20% reduction in downtime
        # - 15% improvement in fuel efficiency
        
        projected_corrective_savings = breakdown_by_type.get('Corrective', 0) * 0.3
        projected_emergency_savings = breakdown_by_type.get('Emergency', 0) * 0.5
        projected_downtime_savings = current_downtime_cost * 0.2
        
        # Fuel savings from better maintenance
        total_fuel = self.telemetry['fuel_consumed_l'].sum()
        fuel_savings = total_fuel * 0.15 * 1.2  # 15% improvement at $1.2 per liter
        
        # Total projected savings
        total_projected_savings = (
            projected_corrective_savings +
            projected_emergency_savings +
            projected_downtime_savings +
            fuel_savings
        )
        
        # ROI calculation
        if investment > 0:
            annual_roi = (total_projected_savings / investment) * 100
            payback_period_months = (investment / total_projected_savings) * 12
        else:
            annual_roi = 0
            payback_period_months = 0
        
        return {
            'current_costs': {
                'annual_maintenance_cost': round(current_annual_cost, 2),
                'annual_downtime_cost': round(current_downtime_cost, 2),
                'total_annual_cost': round(current_annual_cost + current_downtime_cost, 2),
                'cost_breakdown': breakdown_by_type
            },
            'proposed_investment': {
                'amount': investment,
                'allocation': {
                    'predictive_maintenance_software': investment * 0.4,
                    'training_programs': investment * 0.3,
                    'diagnostic_equipment': investment * 0.2,
                    'spare_parts_inventory': investment * 0.1
                }
            },
            'projected_savings': {
                'corrective_maintenance_reduction': round(projected_corrective_savings, 2),
                'emergency_maintenance_reduction': round(projected_emergency_savings, 2),
                'downtime_reduction': round(projected_downtime_savings, 2),
                'fuel_efficiency_improvement': round(fuel_savings, 2),
                'total_annual_savings': round(total_projected_savings, 2)
            },
            'roi_analysis': {
                'annual_roi_percent': round(annual_roi, 1),
                'payback_period_months': round(payback_period_months, 1),
                'net_present_value_5_years': round(total_projected_savings * 5 - investment, 2),
                'internal_rate_of_return': round(min(50, annual_roi * 0.8), 1)  # Simplified
            },
            'risk_assessment': {
                'implementation_risk': 'LOW',
                'technology_risk': 'MEDIUM',
                'adoption_risk': 'MEDIUM',
                'financial_risk': 'LOW'
            },
            'recommendations': [
                'Implement predictive maintenance program',
                'Train maintenance staff on new technologies',
                'Establish baseline metrics for comparison',
                'Start with pilot program for 20% of fleet',
                'Review results after 6 months for full rollout'
            ]
        }
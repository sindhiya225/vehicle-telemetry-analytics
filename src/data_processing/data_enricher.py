"""
Data Enricher Module
Enhances raw vehicle telemetry data with additional features and insights
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import warnings
from geopy.distance import geodesic
warnings.filterwarnings('ignore')

class DataEnricher:
    """Enhances vehicle telemetry data with additional features"""
    
    def __init__(self, telemetry_data: pd.DataFrame, vehicle_data: pd.DataFrame):
        """
        Initialize data enricher
        
        Args:
            telemetry_data: Raw telemetry data
            vehicle_data: Vehicle master data
        """
        self.telemetry = telemetry_data.copy()
        self.vehicles = vehicle_data.copy()
        self.enriched_data = None
        
    def enrich_all(self) -> pd.DataFrame:
        """
        Apply all enrichment methods
        
        Returns:
            Enriched DataFrame
        """
        print("Starting data enrichment process...")
        
        # 1. Basic feature engineering
        print("1. Adding basic features...")
        self.add_basic_features()
        
        # 2. Temporal features
        print("2. Adding temporal features...")
        self.add_temporal_features()
        
        # 3. Performance metrics
        print("3. Adding performance metrics...")
        self.add_performance_metrics()
        
        # 4. Driver behavior features
        print("4. Adding driver behavior features...")
        self.add_driver_behavior_features()
        
        # 5. Maintenance indicators
        print("5. Adding maintenance indicators...")
        self.add_maintenance_indicators()
        
        # 6. Weather and road features (if available)
        print("6. Adding environmental features...")
        self.add_environmental_features()
        
        # 7. Location-based features
        print("7. Adding location-based features...")
        self.add_location_features()
        
        # 8. Anomaly detection features
        print("8. Adding anomaly detection features...")
        self.add_anomaly_features()
        
        # 9. Cost-related features
        print("9. Adding cost-related features...")
        self.add_cost_features()
        
        # 10. Efficiency metrics
        print("10. Adding efficiency metrics...")
        self.add_efficiency_metrics()
        
        print("Data enrichment complete!")
        
        self.enriched_data = self.telemetry.copy()
        return self.enriched_data
    
    def add_basic_features(self):
        """Add basic derived features"""
        
        # Trip intensity (distance * speed)
        if 'trip_distance_km' in self.telemetry.columns and 'avg_speed_kmph' in self.telemetry.columns:
            self.telemetry['trip_intensity'] = (
                self.telemetry['trip_distance_km'] * 
                self.telemetry['avg_speed_kmph']
            )
        
        # Fuel consumption rate (L/hour)
        if 'fuel_consumed_l' in self.telemetry.columns and 'trip_duration_min' in self.telemetry.columns:
            self.telemetry['fuel_consumption_rate_lph'] = (
                self.telemetry['fuel_consumed_l'] / 
                (self.telemetry['trip_duration_min'] / 60)
            ).replace([np.inf, -np.inf], 0)
        
        # Distance per RPM (efficiency indicator)
        if 'trip_distance_km' in self.telemetry.columns and 'avg_rpm' in self.telemetry.columns:
            self.telemetry['distance_per_rpm'] = (
                self.telemetry['trip_distance_km'] / 
                self.telemetry['avg_rpm']
            ).replace([np.inf, -np.inf], 0)
        
        # Engine load indicator
        if 'avg_rpm' in self.telemetry.columns and 'avg_speed_kmph' in self.telemetry.columns:
            self.telemetry['engine_load'] = (
                self.telemetry['avg_rpm'] / 
                self.telemetry['avg_speed_kmph']
            ).replace([np.inf, -np.inf], 0)
    
    def add_temporal_features(self):
        """Add time-based features"""
        
        if 'timestamp' in self.telemetry.columns:
            # Ensure timestamp is datetime
            if not pd.api.types.is_datetime64_any_dtype(self.telemetry['timestamp']):
                self.telemetry['timestamp'] = pd.to_datetime(self.telemetry['timestamp'])
            
            # Extract temporal components
            self.telemetry['year'] = self.telemetry['timestamp'].dt.year
            self.telemetry['month'] = self.telemetry['timestamp'].dt.month
            self.telemetry['day'] = self.telemetry['timestamp'].dt.day
            self.telemetry['hour'] = self.telemetry['timestamp'].dt.hour
            self.telemetry['day_of_week'] = self.telemetry['timestamp'].dt.dayofweek
            self.telemetry['day_of_year'] = self.telemetry['timestamp'].dt.dayofyear
            self.telemetry['week_of_year'] = self.telemetry['timestamp'].dt.isocalendar().week
            self.telemetry['quarter'] = self.telemetry['timestamp'].dt.quarter
            
            # Time-based categories
            self.telemetry['time_of_day'] = pd.cut(
                self.telemetry['hour'],
                bins=[0, 6, 12, 18, 24],
                labels=['Night', 'Morning', 'Afternoon', 'Evening'],
                include_lowest=True
            )
            
            self.telemetry['is_weekend'] = self.telemetry['day_of_week'].isin([5, 6]).astype(int)
            
            # Rush hour indicator
            self.telemetry['is_rush_hour'] = (
                ((self.telemetry['hour'] >= 7) & (self.telemetry['hour'] <= 9)) |
                ((self.telemetry['hour'] >= 16) & (self.telemetry['hour'] <= 18))
            ).astype(int)
            
            # Season indicator
            self.telemetry['season'] = self.telemetry['month'].apply(self.get_season)
    
    def get_season(self, month: int) -> str:
        """Get season from month"""
        if month in [12, 1, 2]:
            return 'Winter'
        elif month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        else:
            return 'Fall'
    
    def add_performance_metrics(self):
        """Add vehicle performance metrics"""
        
        # Merge with vehicle data for performance calculations
        if 'vehid' in self.telemetry.columns and 'vehid' in self.vehicles.columns:
            self.telemetry = pd.merge(
                self.telemetry,
                self.vehicles[['vehid', 'vehicle_type', 'vehicle_class', 'engine_type',
                              'displacement_l', 'is_turbo', 'is_hybrid', 'weight_category']],
                on='vehid',
                how='left'
            )
        
        # Expected efficiency based on vehicle characteristics
        if all(col in self.telemetry.columns for col in ['vehicle_class', 'engine_type', 'displacement_l']):
            self.telemetry['expected_efficiency'] = self.telemetry.apply(
                lambda row: self.calculate_expected_efficiency(
                    row['vehicle_class'],
                    row['engine_type'],
                    row.get('displacement_l', 2.0)
                ),
                axis=1
            )
            
            # Efficiency deviation
            if 'fuel_efficiency_kmpl' in self.telemetry.columns:
                self.telemetry['efficiency_deviation'] = (
                    self.telemetry['fuel_efficiency_kmpl'] - 
                    self.telemetry['expected_efficiency']
                )
                self.telemetry['efficiency_deviation_percent'] = (
                    self.telemetry['efficiency_deviation'] / 
                    self.telemetry['expected_efficiency'] * 100
                ).replace([np.inf, -np.inf], 0)
        
        # Power-to-weight ratio (simplified)
        if 'displacement_l' in self.telemetry.columns:
            # Estimate power from displacement
            self.telemetry['estimated_power_hp'] = (
                self.telemetry['displacement_l'] * 50  # Simplified: 50 HP per liter
            )
            
            if 'is_turbo' in self.telemetry.columns:
                self.telemetry.loc[self.telemetry['is_turbo'], 'estimated_power_hp'] *= 1.3
        
        # Acceleration capability (simplified)
        if all(col in self.telemetry.columns for col in ['estimated_power_hp', 'weight_category']):
            weight_map = {'Light': 1500, 'Medium-Light': 2000, 'Medium': 2500, 
                         'Medium-Heavy': 3000, 'Heavy': 3500}
            self.telemetry['estimated_weight_kg'] = self.telemetry['weight_category'].map(weight_map)
            
            self.telemetry['power_to_weight'] = (
                self.telemetry['estimated_power_hp'] / 
                self.telemetry['estimated_weight_kg']
            ).replace([np.inf, -np.inf], 0)
    
    def calculate_expected_efficiency(self, vehicle_class: str, engine_type: str, 
                                     displacement: float) -> float:
        """Calculate expected fuel efficiency based on vehicle characteristics"""
        
        # Base efficiencies by vehicle class and engine type
        base_efficiencies = {
            'Compact': {'Gasoline': 18.0, 'Diesel': 22.0, 'Hybrid': 25.0, 'Electric': 100.0},
            'Midsize': {'Gasoline': 15.0, 'Diesel': 18.0, 'Hybrid': 22.0, 'Electric': 100.0},
            'SUV': {'Gasoline': 12.0, 'Diesel': 15.0, 'Hybrid': 20.0, 'Electric': 100.0},
            'Truck': {'Gasoline': 10.0, 'Diesel': 13.0, 'Hybrid': 18.0, 'Electric': 100.0}
        }
        
        # Get base efficiency
        base_eff = base_efficiencies.get(vehicle_class, {}).get(engine_type, 15.0)
        
        # Adjust for displacement
        if engine_type in ['Gasoline', 'Diesel']:
            # Larger engines typically less efficient
            displacement_factor = 1.0 - (displacement - 2.0) * 0.1
            base_eff *= max(0.5, displacement_factor)
        
        return round(base_eff, 2)
    
    def add_driver_behavior_features(self):
        """Add driver behavior analysis features"""
        
        # Harsh event indicators
        if 'harsh_acceleration_count' in self.telemetry.columns:
            self.telemetry['has_harsh_acceleration'] = (
                self.telemetry['harsh_acceleration_count'] > 0
            ).astype(int)
        
        if 'harsh_braking_count' in self.telemetry.columns:
            self.telemetry['has_harsh_braking'] = (
                self.telemetry['harsh_braking_count'] > 0
            ).astype(int)
        
        if 'speeding_count' in self.telemetry.columns:
            self.telemetry['has_speeding'] = (
                self.telemetry['speeding_count'] > 0
            ).astype(int)
        
        # Calculate driver behavior score if not present
        if 'driver_behavior_score' not in self.telemetry.columns:
            # Start with perfect score
            self.telemetry['driver_behavior_score'] = 1.0
            
            # Deduct for harsh events
            if 'harsh_acceleration_count' in self.telemetry.columns:
                self.telemetry['driver_behavior_score'] -= (
                    self.telemetry['harsh_acceleration_count'] * 0.05
                )
            
            if 'harsh_braking_count' in self.telemetry.columns:
                self.telemetry['driver_behavior_score'] -= (
                    self.telemetry['harsh_braking_count'] * 0.08
                )
            
            if 'speeding_count' in self.telemetry.columns:
                self.telemetry['driver_behavior_score'] -= (
                    self.telemetry['speeding_count'] * 0.03
                )
            
            # Ensure score stays within bounds
            self.telemetry['driver_behavior_score'] = self.telemetry['driver_behavior_score'].clip(0, 1)
        
        # Driving style classification
        self.telemetry['driving_style'] = pd.cut(
            self.telemetry['driver_behavior_score'],
            bins=[0, 0.6, 0.8, 0.9, 1.0],
            labels=['Aggressive', 'Moderate', 'Safe', 'Excellent'],
            include_lowest=True
        )
        
        # Idle behavior
        if 'idle_time_min' in self.telemetry.columns and 'trip_duration_min' in self.telemetry.columns:
            self.telemetry['idle_percentage'] = (
                self.telemetry['idle_time_min'] / 
                self.telemetry['trip_duration_min'] * 100
            ).replace([np.inf, -np.inf], 0)
            
            self.telemetry['has_excessive_idling'] = (
                self.telemetry['idle_percentage'] > 20
            ).astype(int)
        
        # Speed consistency
        if 'avg_speed_kmph' in self.telemetry.columns:
            # Calculate speed variation for each vehicle
            vehicle_speed_std = self.telemetry.groupby('vehid')['avg_speed_kmph'].std()
            self.telemetry['speed_consistency'] = self.telemetry['vehid'].map(vehicle_speed_std)
            
            # Normalize consistency score (lower std = more consistent)
            max_std = self.telemetry['speed_consistency'].max()
            if max_std > 0:
                self.telemetry['speed_consistency_score'] = 1 - (
                    self.telemetry['speed_consistency'] / max_std
                )
            else:
                self.telemetry['speed_consistency_score'] = 1
    
    def add_maintenance_indicators(self):
        """Add maintenance-related features"""
        
        # Engine stress indicator
        if all(col in self.telemetry.columns for col in ['avg_engine_temp_c', 'avg_rpm']):
            self.telemetry['engine_stress'] = (
                (self.telemetry['avg_engine_temp_c'] - 90) / 20 *  # Normalize temperature
                (self.telemetry['avg_rpm'] / 3000)  # Normalize RPM
            ).clip(0, 2)
        
        # Brake wear indicator (simplified)
        if 'harsh_braking_count' in self.telemetry.columns:
            self.telemetry['brake_wear_indicator'] = (
                self.telemetry['harsh_braking_count'] * 0.1
            )
        
        # Tire wear indicator (simplified - based on distance)
        if 'trip_distance_km' in self.telemetry.columns:
            # Assume tire life of 50,000 km
            self.telemetry['tire_wear_indicator'] = (
                self.telemetry['trip_distance_km'] / 50000
            )
        
        # Battery health indicator (simplified)
        if 'battery_voltage' in self.telemetry.columns:
            self.telemetry['battery_health'] = pd.cut(
                self.telemetry['battery_voltage'],
                bins=[0, 11.5, 12.0, 12.5, 15],
                labels=['Critical', 'Poor', 'Fair', 'Good'],
                include_lowest=True
            )
        
        # Overall maintenance score
        maintenance_score = 100  # Start with perfect score
        
        # Deduct for high engine stress
        if 'engine_stress' in self.telemetry.columns:
            maintenance_score -= self.telemetry['engine_stress'] * 10
        
        # Deduct for brake wear
        if 'brake_wear_indicator' in self.telemetry.columns:
            maintenance_score -= self.telemetry['brake_wear_indicator'] * 20
        
        # Deduct for tire wear
        if 'tire_wear_indicator' in self.telemetry.columns:
            maintenance_score -= self.telemetry['tire_wear_indicator'] * 15
        
        # Battery health impact
        if 'battery_health' in self.telemetry.columns:
            health_map = {'Critical': -30, 'Poor': -15, 'Fair': -5, 'Good': 0}
            maintenance_score += self.telemetry['battery_health'].map(health_map).fillna(0)
        
        self.telemetry['maintenance_score'] = maintenance_score.clip(0, 100)
        
        # Maintenance urgency
        self.telemetry['maintenance_urgency'] = pd.cut(
            self.telemetry['maintenance_score'],
            bins=[0, 40, 60, 80, 100],
            labels=['Critical', 'High', 'Medium', 'Low'],
            include_lowest=True
        )
    
    def add_environmental_features(self):
        """Add weather and environmental features"""
        
        # Weather impact on efficiency
        if 'weather_condition' in self.telemetry.columns:
            weather_efficiency_factor = {
                'Clear': 1.0,
                'Rain': 0.95,
                'Snow': 0.85,
                'Fog': 0.90,
                'Windy': 0.92
            }
            
            self.telemetry['weather_efficiency_factor'] = self.telemetry['weather_condition'].map(
                weather_efficiency_factor
            ).fillna(1.0)
        
        # Road type impact
        if 'road_type' in self.telemetry.columns:
            road_efficiency_factor = {
                'Urban': 0.85,
                'Highway': 1.10,
                'Mixed': 1.0,
                'Rural': 0.95,
                'Mountain': 0.80
            }
            
            self.telemetry['road_efficiency_factor'] = self.telemetry['road_type'].map(
                road_efficiency_factor
            ).fillna(1.0)
        
        # Combined environmental factor
        if 'weather_efficiency_factor' in self.telemetry.columns and 'road_efficiency_factor' in self.telemetry.columns:
            self.telemetry['environmental_factor'] = (
                self.telemetry['weather_efficiency_factor'] * 
                self.telemetry['road_efficiency_factor']
            )
        
        # Adjusted efficiency (factoring in environment)
        if all(col in self.telemetry.columns for col in ['fuel_efficiency_kmpl', 'environmental_factor']):
            self.telemetry['adjusted_efficiency'] = (
                self.telemetry['fuel_efficiency_kmpl'] / 
                self.telemetry['environmental_factor']
            )
    
    def add_location_features(self):
        """Add location-based features"""
        
        # Check if location data is available
        if all(col in self.telemetry.columns for col in ['location_lat', 'location_lon']):
            try:
                # Calculate distance from a reference point (e.g., depot)
                # Using a fixed reference for simplicity
                depot_location = (40.7128, -74.0060)  # NYC coordinates
                
                self.telemetry['distance_from_depot_km'] = self.telemetry.apply(
                    lambda row: self.calculate_distance(
                        (row['location_lat'], row['location_lon']),
                        depot_location
                    ) if pd.notna(row['location_lat']) and pd.notna(row['location_lon']) else np.nan,
                    axis=1
                )
                
                # Urban vs rural classification based on density (simplified)
                # Assuming higher density near city center
                self.telemetry['location_density'] = pd.cut(
                    self.telemetry['distance_from_depot_km'],
                    bins=[0, 10, 30, 100, np.inf],
                    labels=['City Center', 'Suburban', 'Rural', 'Remote'],
                    include_lowest=True
                )
                
            except Exception as e:
                print(f"Location feature calculation failed: {e}")
    
    def calculate_distance(self, point1: Tuple[float, float], 
                          point2: Tuple[float, float]) -> float:
        """Calculate distance between two points in kilometers"""
        try:
            return geodesic(point1, point2).kilometers
        except:
            return np.nan
    
    def add_anomaly_features(self):
        """Add features for anomaly detection"""
        
        # Z-score based anomaly indicators
        numeric_cols = self.telemetry.select_dtypes(include=[np.number]).columns
        
        for col in ['fuel_efficiency_kmpl', 'avg_engine_temp_c', 'avg_rpm', 'avg_speed_kmph']:
            if col in numeric_cols:
                # Calculate z-scores by vehicle type for fair comparison
                for vehicle_type in self.telemetry['vehicle_type'].unique():
                    mask = self.telemetry['vehicle_type'] == vehicle_type
                    if mask.sum() > 10:  # Need sufficient data
                        col_data = self.telemetry.loc[mask, col]
                        mean_val = col_data.mean()
                        std_val = col_data.std()
                        
                        if std_val > 0:
                            z_scores = (col_data - mean_val) / std_val
                            self.telemetry.loc[mask, f'{col}_zscore'] = z_scores
                            
                            # Flag anomalies (|z-score| > 3)
                            self.telemetry.loc[mask, f'{col}_anomaly'] = (
                                abs(z_scores) > 3
                            ).astype(int)
        
        # Combined anomaly score
        anomaly_cols = [col for col in self.telemetry.columns if col.endswith('_anomaly')]
        if anomaly_cols:
            self.telemetry['total_anomalies'] = self.telemetry[anomaly_cols].sum(axis=1)
            self.telemetry['has_anomaly'] = (self.telemetry['total_anomalies'] > 0).astype(int)
        
        # Rate of change anomalies
        if 'timestamp' in self.telemetry.columns and 'fuel_efficiency_kmpl' in self.telemetry.columns:
            self.telemetry.sort_values(['vehid', 'timestamp'], inplace=True)
            
            # Calculate efficiency change from previous trip
            self.telemetry['efficiency_change'] = self.telemetry.groupby('vehid')['fuel_efficiency_kmpl'].diff()
            
            # Flag large changes
            self.telemetry['efficiency_change_anomaly'] = (
                abs(self.telemetry['efficiency_change']) > 5
            ).astype(int)
    
    def add_cost_features(self):
        """Add cost-related features"""
        
        # Fuel cost
        if 'fuel_consumed_l' in self.telemetry.columns:
            fuel_price = 1.2  # USD per liter
            self.telemetry['fuel_cost_usd'] = (
                self.telemetry['fuel_consumed_l'] * fuel_price
            ).round(2)
        
        # Cost per kilometer
        if all(col in self.telemetry.columns for col in ['fuel_cost_usd', 'trip_distance_km']):
            self.telemetry['cost_per_km'] = (
                self.telemetry['fuel_cost_usd'] / 
                self.telemetry['trip_distance_km']
            ).replace([np.inf, -np.inf], 0).round(4)
        
        # Idle cost (fuel wasted while idling)
        if 'idle_time_min' in self.telemetry.columns:
            # Assume 0.5 liters per hour while idling
            idle_fuel_rate = 0.5 / 60  # liters per minute
            self.telemetry['idle_fuel_cost_usd'] = (
                self.telemetry['idle_time_min'] * idle_fuel_rate * 1.2
            ).round(2)
        
        # Maintenance cost indicator
        if 'maintenance_score' in self.telemetry.columns:
            # Lower maintenance score = higher expected maintenance cost
            self.telemetry['expected_maintenance_cost'] = (
                100 - self.telemetry['maintenance_score']
            ) * 10  # Scale factor
        
        # Total operating cost (fuel + maintenance indicator)
        cost_components = []
        if 'fuel_cost_usd' in self.telemetry.columns:
            cost_components.append('fuel_cost_usd')
        if 'expected_maintenance_cost' in self.telemetry.columns:
            cost_components.append('expected_maintenance_cost')
        if 'idle_fuel_cost_usd' in self.telemetry.columns:
            cost_components.append('idle_fuel_cost_usd')
        
        if cost_components:
            self.telemetry['total_operating_cost'] = self.telemetry[cost_components].sum(axis=1).round(2)
    
    def add_efficiency_metrics(self):
        """Add comprehensive efficiency metrics"""
        
        # Energy efficiency (for EVs/HEVs)
        if 'vehicle_type' in self.telemetry.columns:
            # Convert fuel efficiency to energy equivalents
            # Gasoline: 1 liter = 8.9 kWh energy content
            # Diesel: 1 liter = 10.7 kWh
            # Electricity: 1 kWh = 1 kWh
            
            def calculate_energy_efficiency(row):
                if row['vehicle_type'] == 'BEV':
                    # For EVs, we need energy consumption data
                    return np.nan
                elif row['vehicle_type'] in ['HEV', 'PHEV']:
                    # Hybrid: use both fuel and electric
                    return row.get('fuel_efficiency_kmpl', 0) * 8.9  # Convert to kWh equivalent
                else:
                    # ICE vehicles
                    energy_content = 8.9 if row.get('engine_type') != 'Diesel' else 10.7
                    return row.get('fuel_efficiency_kmpl', 0) * energy_content
            
            if 'fuel_efficiency_kmpl' in self.telemetry.columns:
                self.telemetry['energy_efficiency_kwh_per_100km'] = self.telemetry.apply(
                    calculate_energy_efficiency, axis=1
                )
        
        # CO2 emissions
        if 'fuel_consumed_l' in self.telemetry.columns and 'engine_type' in self.telemetry.columns:
            # CO2 emission factors (kg CO2 per liter)
            co2_factors = {
                'Gasoline': 2.31,
                'Diesel': 2.68,
                'Hybrid': 1.85,
                'Electric': 0.0
            }
            
            self.telemetry['co2_emissions_kg'] = (
                self.telemetry['fuel_consumed_l'] * 
                self.telemetry['engine_type'].map(co2_factors).fillna(2.31)
            ).round(2)
            
            # CO2 per km
            if 'trip_distance_km' in self.telemetry.columns:
                self.telemetry['co2_per_km_g'] = (
                    self.telemetry['co2_emissions_kg'] * 1000 / 
                    self.telemetry['trip_distance_km']
                ).replace([np.inf, -np.inf], 0).round(1)
        
        # Efficiency benchmark comparison
        if all(col in self.telemetry.columns for col in ['fuel_efficiency_kmpl', 'expected_efficiency']):
            self.telemetry['efficiency_benchmark_gap'] = (
                self.telemetry['fuel_efficiency_kmpl'] - 
                self.telemetry['expected_efficiency']
            )
            self.telemetry['efficiency_benchmark_percent'] = (
                self.telemetry['fuel_efficiency_kmpl'] / 
                self.telemetry['expected_efficiency'] * 100
            ).replace([np.inf, -np.inf], 100).clip(0, 200)
        
        # Rolling efficiency metrics
        if 'timestamp' in self.telemetry.columns and 'fuel_efficiency_kmpl' in self.telemetry.columns:
            self.telemetry.sort_values(['vehid', 'timestamp'], inplace=True)
            
            # 10-trip moving average
            self.telemetry['efficiency_ma_10'] = self.telemetry.groupby('vehid')['fuel_efficiency_kmpl'].transform(
                lambda x: x.rolling(window=10, min_periods=5).mean()
            )
            
            # Efficiency trend (slope of last 10 trips)
            def calculate_trend(series):
                if len(series) >= 5:
                    x = np.arange(len(series))
                    slope, _ = np.polyfit(x, series.values, 1)
                    return slope
                return np.nan
            
            self.telemetry['efficiency_trend'] = self.telemetry.groupby('vehid')['fuel_efficiency_kmpl'].transform(
                lambda x: x.rolling(window=10, min_periods=5).apply(calculate_trend, raw=False)
            )
    
    def save_enriched_data(self, output_path: str):
        """Save enriched data to file"""
        
        if self.enriched_data is not None:
            # Save to CSV
            self.enriched_data.to_csv(output_path, index=False)
            print(f"Enriched data saved to {output_path}")
            
            # Also save a summary
            self.save_enrichment_summary(output_path.replace('.csv', '_summary.txt'))
        else:
            print("No enriched data available. Run enrich_all() first.")
    
    def save_enrichment_summary(self, output_path: str):
        """Save summary of enrichment process"""
        
        if self.enriched_data is None:
            return
        
        summary_lines = []
        summary_lines.append("=" * 60)
        summary_lines.append("DATA ENRICHMENT SUMMARY")
        summary_lines.append("=" * 60)
        summary_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        summary_lines.append(f"Original records: {len(self.telemetry)}")
        summary_lines.append(f"Original columns: {len(self.telemetry.columns)}")
        summary_lines.append("")
        
        # Count new features by category
        feature_categories = {
            'Temporal Features': ['year', 'month', 'day', 'hour', 'day_of_week', 
                                 'time_of_day', 'is_weekend', 'is_rush_hour', 'season'],
            'Performance Metrics': ['expected_efficiency', 'efficiency_deviation', 
                                   'estimated_power_hp', 'power_to_weight'],
            'Driver Behavior': ['driver_behavior_score', 'driving_style', 
                               'idle_percentage', 'has_excessive_idling', 'speed_consistency_score'],
            'Maintenance Indicators': ['engine_stress', 'brake_wear_indicator', 
                                      'tire_wear_indicator', 'battery_health', 
                                      'maintenance_score', 'maintenance_urgency'],
            'Environmental Features': ['weather_efficiency_factor', 'road_efficiency_factor',
                                      'environmental_factor', 'adjusted_efficiency'],
            'Cost Features': ['fuel_cost_usd', 'cost_per_km', 'idle_fuel_cost_usd',
                             'expected_maintenance_cost', 'total_operating_cost'],
            'Efficiency Metrics': ['energy_efficiency_kwh_per_100km', 'co2_emissions_kg',
                                  'co2_per_km_g', 'efficiency_benchmark_gap',
                                  'efficiency_ma_10', 'efficiency_trend'],
            'Anomaly Features': [col for col in self.enriched_data.columns if 'anomaly' in col or 'zscore' in col]
        }
        
        summary_lines.append("FEATURE CATEGORIES:")
        summary_lines.append("-" * 40)
        
        total_new_features = 0
        for category, features in feature_categories.items():
            # Count features that actually exist in the data
            existing_features = [f for f in features if f in self.enriched_data.columns]
            if existing_features:
                count = len(existing_features)
                total_new_features += count
                summary_lines.append(f"{category}: {count} features")
        
        summary_lines.append("")
        summary_lines.append(f"Total new features added: {total_new_features}")
        summary_lines.append(f"Total columns after enrichment: {len(self.enriched_data.columns)}")
        summary_lines.append("")
        
        # Sample of new features
        summary_lines.append("SAMPLE OF NEW FEATURES:")
        summary_lines.append("-" * 40)
        
        sample_features = []
        for category in feature_categories:
            features = [f for f in feature_categories[category] if f in self.enriched_data.columns]
            sample_features.extend(features[:2])  # Take 2 from each category
        
        for feature in sample_features[:20]:  # Show first 20
            if feature in self.enriched_data.columns:
                dtype = self.enriched_data[feature].dtype
                non_null = self.enriched_data[feature].notna().sum()
                summary_lines.append(f"{feature}: {dtype} ({non_null} non-null values)")
        
        # Write to file
        with open(output_path, 'w') as f:
            f.write('\n'.join(summary_lines))
        
        print(f"Enrichment summary saved to {output_path}")
"""
Efficiency Calculator Module
Advanced fuel efficiency calculations and benchmarking
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

@dataclass
class EfficiencyMetrics:
    """Data class for efficiency metrics"""
    vehid: int
    vehicle_type: str
    vehicle_class: str
    current_efficiency: float
    benchmark_efficiency: float
    efficiency_score: float
    percentile_rank: float
    improvement_potential: float
    cost_savings_potential: float
    category: str

class EfficiencyCalculator:
    """Advanced fuel efficiency analysis and benchmarking"""
    
    def __init__(self, telemetry_data: pd.DataFrame, vehicle_data: pd.DataFrame):
        """
        Initialize efficiency calculator
        
        Args:
            telemetry_data: DataFrame with telemetry data
            vehicle_data: DataFrame with vehicle master data
        """
        self.telemetry = telemetry_data
        self.vehicles = vehicle_data
        self.benchmarks = self.load_efficiency_benchmarks()
        
    def load_efficiency_benchmarks(self) -> pd.DataFrame:
        """Load efficiency benchmarks for different vehicle types"""
        
        # Standard benchmarks (can be loaded from database in production)
        benchmarks_data = {
            'vehicle_class': ['Compact', 'Compact', 'Midsize', 'Midsize', 'SUV', 'SUV', 'Truck'],
            'engine_type': ['Gasoline', 'Hybrid', 'Gasoline', 'Hybrid', 'Gasoline', 'Hybrid', 'Gasoline'],
            'displacement_range': ['1.0-1.5L', '1.5-2.0L', '2.0-2.5L', '2.0-2.5L', '2.5-3.0L', '2.5-3.0L', '3.5-4.0L'],
            'urban_efficiency_kmpl': [14.5, 20.0, 11.5, 18.0, 9.5, 16.0, 7.5],
            'highway_efficiency_kmpl': [18.0, 22.0, 16.0, 20.0, 13.0, 18.0, 10.0],
            'mixed_efficiency_kmpl': [16.0, 21.0, 13.5, 19.0, 11.0, 17.0, 8.5]
        }
        
        return pd.DataFrame(benchmarks_data)
    
    def calculate_vehicle_efficiency(self, vehid: int, days: int = 30) -> Dict:
        """
        Calculate comprehensive efficiency metrics for a vehicle
        
        Args:
            vehid: Vehicle ID
            days: Number of days to analyze
            
        Returns:
            Dictionary with efficiency metrics
        """
        # Filter telemetry for the vehicle and time period
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        vehicle_telemetry = self.telemetry[
            (self.telemetry['vehid'] == vehid) &
            (self.telemetry['timestamp'] >= start_date) &
            (self.telemetry['timestamp'] <= end_date)
        ]
        
        if len(vehicle_telemetry) == 0:
            raise ValueError(f"No telemetry data found for vehicle {vehid}")
        
        # Get vehicle information
        vehicle_info = self.vehicles[self.vehicles['vehid'] == vehid].iloc[0]
        
        # Calculate basic efficiency metrics
        current_efficiency = vehicle_telemetry['fuel_efficiency_kmpl'].mean()
        efficiency_std = vehicle_telemetry['fuel_efficiency_kmpl'].std()
        efficiency_p25 = vehicle_telemetry['fuel_efficiency_kmpl'].quantile(0.25)
        efficiency_p75 = vehicle_telemetry['fuel_efficiency_kmpl'].quantile(0.75)
        
        # Get benchmark efficiency
        benchmark = self.get_benchmark_efficiency(
            vehicle_info['vehicle_class'],
            vehicle_info['engine_type'],
            vehicle_info['displacement_l']
        )
        
        # Calculate efficiency score (0-100)
        if benchmark > 0:
            efficiency_score = min(100, (current_efficiency / benchmark) * 100)
        else:
            efficiency_score = 0
        
        # Calculate improvement potential
        improvement_potential = max(0, benchmark - current_efficiency)
        
        # Calculate cost savings potential
        avg_distance_per_day = vehicle_telemetry['trip_distance_km'].sum() / days
        annual_distance = avg_distance_per_day * 365
        fuel_price = 1.2  # USD per liter
        
        if current_efficiency > 0:
            current_annual_fuel = annual_distance / current_efficiency
            potential_annual_fuel = annual_distance / benchmark
            cost_savings_potential = (current_annual_fuel - potential_annual_fuel) * fuel_price
        else:
            cost_savings_potential = 0
        
        # Determine efficiency category
        if efficiency_score >= 90:
            category = "Excellent"
        elif efficiency_score >= 80:
            category = "Good"
        elif efficiency_score >= 70:
            category = "Average"
        elif efficiency_score >= 60:
            category = "Below Average"
        else:
            category = "Poor"
        
        # Calculate efficiency by road type
        efficiency_by_road_type = {}
        if 'road_type' in vehicle_telemetry.columns:
            for road_type in vehicle_telemetry['road_type'].unique():
                road_data = vehicle_telemetry[vehicle_telemetry['road_type'] == road_type]
                if len(road_data) > 0:
                    efficiency_by_road_type[road_type] = {
                        'avg_efficiency': road_data['fuel_efficiency_kmpl'].mean(),
                        'trip_count': len(road_data),
                        'total_distance': road_data['trip_distance_km'].sum()
                    }
        
        # Calculate efficiency trends
        vehicle_telemetry['date'] = pd.to_datetime(vehicle_telemetry['timestamp']).dt.date
        daily_efficiency = vehicle_telemetry.groupby('date').agg({
            'fuel_efficiency_kmpl': 'mean',
            'trip_distance_km': 'sum'
        }).reset_index()
        
        # Calculate trend (simple linear regression)
        if len(daily_efficiency) > 1:
            x = np.arange(len(daily_efficiency))
            y = daily_efficiency['fuel_efficiency_kmpl'].values
            slope, _ = np.polyfit(x, y, 1)
            trend_per_day = slope
            trend_percentage = (trend_per_day * 30) / current_efficiency * 100 if current_efficiency > 0 else 0
        else:
            trend_per_day = 0
            trend_percentage = 0
        
        return {
            'vehid': vehid,
            'vehicle_type': vehicle_info['vehicle_type'],
            'vehicle_class': vehicle_info['vehicle_class'],
            'current_efficiency': round(current_efficiency, 2),
            'benchmark_efficiency': round(benchmark, 2),
            'efficiency_score': round(efficiency_score, 1),
            'improvement_potential': round(improvement_potential, 2),
            'cost_savings_potential': round(cost_savings_potential, 2),
            'category': category,
            'statistics': {
                'std_dev': round(efficiency_std, 2),
                'p25': round(efficiency_p25, 2),
                'p75': round(efficiency_p75, 2),
                'min': round(vehicle_telemetry['fuel_efficiency_kmpl'].min(), 2),
                'max': round(vehicle_telemetry['fuel_efficiency_kmpl'].max(), 2)
            },
            'efficiency_by_road_type': efficiency_by_road_type,
            'trend_analysis': {
                'trend_per_day': round(trend_per_day, 3),
                'trend_percentage_30_days': round(trend_percentage, 1),
                'trend_direction': 'improving' if trend_per_day > 0 else 'declining' if trend_per_day < 0 else 'stable'
            },
            'analysis_period': {
                'days': days,
                'start_date': start_date.date().isoformat(),
                'end_date': end_date.date().isoformat(),
                'trip_count': len(vehicle_telemetry),
                'total_distance': round(vehicle_telemetry['trip_distance_km'].sum(), 2)
            }
        }
    
    def get_benchmark_efficiency(self, vehicle_class: str, engine_type: str, displacement: float) -> float:
        """
        Get benchmark efficiency for a vehicle
        
        Args:
            vehicle_class: Vehicle class (Compact, SUV, etc.)
            engine_type: Engine type (Gasoline, Hybrid, etc.)
            displacement: Engine displacement in liters
            
        Returns:
            Benchmark efficiency in km/L
        """
        # Find matching benchmark
        for _, benchmark in self.benchmarks.iterrows():
            if (benchmark['vehicle_class'] == vehicle_class and 
                benchmark['engine_type'] == engine_type):
                
                # Parse displacement range
                range_str = benchmark['displacement_range']
                if '-' in range_str and 'L' in range_str:
                    min_disp, max_disp = map(float, range_str.replace('L', '').split('-'))
                    if min_disp <= displacement <= max_disp:
                        return benchmark['mixed_efficiency_kmpl']
        
        # Return default benchmark if no exact match
        default_benchmarks = {
            'Compact': {'Gasoline': 16.0, 'Hybrid': 21.0, 'Diesel': 18.0},
            'Midsize': {'Gasoline': 13.5, 'Hybrid': 19.0, 'Diesel': 15.0},
            'SUV': {'Gasoline': 11.0, 'Hybrid': 17.0, 'Diesel': 13.0},
            'Truck': {'Gasoline': 8.5, 'Diesel': 10.5}
        }
        
        return default_benchmarks.get(vehicle_class, {}).get(engine_type, 12.0)
    
    def compare_fleet_efficiency(self, group_by: str = 'vehicle_type') -> pd.DataFrame:
        """
        Compare efficiency across the fleet
        
        Args:
            group_by: Field to group by (vehicle_type, vehicle_class, etc.)
            
        Returns:
            DataFrame with comparison metrics
        """
        if group_by not in self.vehicles.columns:
            raise ValueError(f"Group by field '{group_by}' not found in vehicle data")
        
        # Merge telemetry with vehicle data
        merged_data = pd.merge(
            self.telemetry,
            self.vehicles[['vehid', group_by]],
            on='vehid',
            how='left'
        )
        
        # Group and calculate metrics
        comparison = merged_data.groupby(group_by).agg({
            'vehid': 'nunique',
            'fuel_efficiency_kmpl': ['mean', 'std', 'min', 'max'],
            'trip_distance_km': 'sum',
            'fuel_consumed_l': 'sum'
        }).round(2)
        
        # Flatten column names
        comparison.columns = [
            'vehicle_count',
            'avg_efficiency',
            'efficiency_std',
            'min_efficiency',
            'max_efficiency',
            'total_distance',
            'total_fuel'
        ]
        
        # Calculate additional metrics
        comparison['fuel_cost'] = comparison['total_fuel'] * 1.2
        comparison['cost_per_km'] = comparison['fuel_cost'] / comparison['total_distance']
        
        # Calculate efficiency score relative to best in group
        best_efficiency = comparison['avg_efficiency'].max()
        comparison['efficiency_score'] = (
            comparison['avg_efficiency'] / best_efficiency * 100
        ).round(1)
        
        # Calculate potential savings
        comparison['potential_savings'] = (
            (best_efficiency - comparison['avg_efficiency']) / 
            comparison['avg_efficiency'] * comparison['fuel_cost']
        ).round(2)
        
        return comparison.reset_index()
    
    def identify_efficiency_outliers(self, threshold: float = 2.0) -> pd.DataFrame:
        """
        Identify vehicles with abnormal efficiency patterns
        
        Args:
            threshold: Z-score threshold for outliers
            
        Returns:
            DataFrame with outlier vehicles
        """
        # Calculate vehicle-level efficiency statistics
        vehicle_stats = self.telemetry.groupby('vehid').agg({
            'fuel_efficiency_kmpl': ['mean', 'std', 'count']
        }).round(2)
        
        vehicle_stats.columns = ['mean_efficiency', 'std_efficiency', 'trip_count']
        vehicle_stats = vehicle_stats.reset_index()
        
        # Filter vehicles with sufficient data
        vehicle_stats = vehicle_stats[vehicle_stats['trip_count'] >= 10]
        
        if len(vehicle_stats) == 0:
            return pd.DataFrame()
        
        # Calculate z-scores
        overall_mean = vehicle_stats['mean_efficiency'].mean()
        overall_std = vehicle_stats['mean_efficiency'].std()
        
        if overall_std > 0:
            vehicle_stats['z_score'] = (
                (vehicle_stats['mean_efficiency'] - overall_mean) / overall_std
            ).abs()
        else:
            vehicle_stats['z_score'] = 0
        
        # Identify outliers
        outliers = vehicle_stats[vehicle_stats['z_score'] > threshold].copy()
        
        if len(outliers) == 0:
            return pd.DataFrame()
        
        # Add vehicle information
        outliers = pd.merge(
            outliers,
            self.vehicles[['vehid', 'vehicle_type', 'vehicle_class', 'engine_type']],
            on='vehid',
            how='left'
        )
        
        # Determine outlier type (high or low efficiency)
        outliers['outlier_type'] = outliers.apply(
            lambda row: 'High Efficiency' if row['mean_efficiency'] > overall_mean else 'Low Efficiency',
            axis=1
        )
        
        # Calculate deviation percentage
        outliers['deviation_percent'] = (
            (outliers['mean_efficiency'] - overall_mean) / overall_mean * 100
        ).round(1)
        
        # Sort by deviation magnitude
        outliers = outliers.sort_values('z_score', ascending=False)
        
        return outliers[['vehid', 'vehicle_type', 'vehicle_class', 'mean_efficiency', 
                        'z_score', 'deviation_percent', 'outlier_type', 'trip_count']]
    
    def analyze_efficiency_factors(self) -> Dict:
        """
        Analyze factors affecting fuel efficiency
        
        Returns:
            Dictionary with factor analysis results
        """
        # Merge data for analysis
        analysis_data = pd.merge(
            self.telemetry,
            self.vehicles[['vehid', 'vehicle_type', 'vehicle_class', 'engine_type', 
                          'displacement_l', 'is_turbo', 'is_hybrid']],
            on='vehid',
            how='left'
        )
        
        results = {}
        
        # 1. Analyze by vehicle characteristics
        for factor in ['vehicle_type', 'vehicle_class', 'engine_type', 'is_turbo', 'is_hybrid']:
            if factor in analysis_data.columns:
                factor_analysis = analysis_data.groupby(factor).agg({
                    'fuel_efficiency_kmpl': 'mean',
                    'vehid': 'nunique',
                    'trip_distance_km': 'sum'
                }).round(2)
                
                factor_analysis.columns = ['avg_efficiency', 'vehicle_count', 'total_distance']
                factor_analysis = factor_analysis.sort_values('avg_efficiency', ascending=False)
                results[f'efficiency_by_{factor}'] = factor_analysis.reset_index().to_dict('records')
        
        # 2. Analyze correlation with continuous variables
        if 'displacement_l' in analysis_data.columns:
            correlation = analysis_data[['fuel_efficiency_kmpl', 'displacement_l', 
                                        'trip_distance_km', 'avg_speed_kmph']].corr()
            results['correlation_matrix'] = correlation.round(3).to_dict()
        
        # 3. Analyze by road type and weather
        for factor in ['road_type', 'weather_condition']:
            if factor in analysis_data.columns:
                factor_stats = analysis_data.groupby(factor).agg({
                    'fuel_efficiency_kmpl': ['mean', 'std', 'count']
                }).round(2)
                
                # Flatten column names
                factor_stats.columns = [f'{col[0]}_{col[1]}' for col in factor_stats.columns]
                factor_stats = factor_stats.rename(columns={
                    'fuel_efficiency_kmpl_mean': 'avg_efficiency',
                    'fuel_efficiency_kmpl_std': 'std_efficiency',
                    'fuel_efficiency_kmpl_count': 'trip_count'
                })
                
                results[f'efficiency_by_{factor}'] = factor_stats.reset_index().to_dict('records')
        
        # 4. Speed-efficiency relationship
        if 'avg_speed_kmph' in analysis_data.columns:
            # Create speed bins
            analysis_data['speed_bin'] = pd.cut(
                analysis_data['avg_speed_kmph'],
                bins=[0, 30, 50, 70, 90, 120, 200],
                labels=['0-30', '30-50', '50-70', '70-90', '90-120', '120+']
            )
            
            speed_analysis = analysis_data.groupby('speed_bin').agg({
                'fuel_efficiency_kmpl': 'mean',
                'trip_distance_km': 'sum',
                'vehid': 'nunique'
            }).round(2)
            
            speed_analysis.columns = ['avg_efficiency', 'total_distance', 'vehicle_count']
            results['efficiency_by_speed'] = speed_analysis.reset_index().to_dict('records')
        
        return results
    
    def generate_efficiency_report(self, output_format: str = 'dict') -> Dict:
        """
        Generate comprehensive efficiency report
        
        Args:
            output_format: Output format ('dict', 'dataframe', 'json')
            
        Returns:
            Efficiency report in specified format
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {},
            'fleet_comparison': {},
            'outliers': {},
            'factor_analysis': {},
            'recommendations': []
        }
        
        # 1. Fleet summary
        total_distance = self.telemetry['trip_distance_km'].sum()
        total_fuel = self.telemetry['fuel_consumed_l'].sum()
        avg_efficiency = total_distance / total_fuel if total_fuel > 0 else 0
        
        report['summary'] = {
            'total_vehicles': self.vehicles['vehid'].nunique(),
            'total_trips': len(self.telemetry),
            'total_distance_km': round(total_distance, 2),
            'total_fuel_consumed_l': round(total_fuel, 2),
            'avg_fleet_efficiency_kmpl': round(avg_efficiency, 2),
            'total_fuel_cost_usd': round(total_fuel * 1.2, 2)
        }
        
        # 2. Fleet comparison
        fleet_comparison = self.compare_fleet_efficiency('vehicle_type')
        report['fleet_comparison'] = fleet_comparison.to_dict('records')
        
        # 3. Identify outliers
        outliers = self.identify_efficiency_outliers()
        if not outliers.empty:
            report['outliers'] = {
                'high_efficiency': outliers[outliers['outlier_type'] == 'High Efficiency']
                                   .to_dict('records'),
                'low_efficiency': outliers[outliers['outlier_type'] == 'Low Efficiency']
                                  .to_dict('records')
            }
        
        # 4. Factor analysis
        report['factor_analysis'] = self.analyze_efficiency_factors()
        
        # 5. Generate recommendations
        report['recommendations'] = self.generate_recommendations(fleet_comparison, outliers)
        
        # Convert to requested format
        if output_format == 'dataframe':
            # Convert relevant parts to DataFrames
            pass  # Implementation would depend on specific needs
        elif output_format == 'json':
            import json
            return json.dumps(report, indent=2, default=str)
        
        return report
    
    def generate_recommendations(self, fleet_comparison: pd.DataFrame, 
                                outliers: pd.DataFrame) -> List[Dict]:
        """
        Generate actionable efficiency improvement recommendations
        
        Args:
            fleet_comparison: Fleet comparison DataFrame
            outliers: Outlier vehicles DataFrame
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        # Recommendation 1: Focus on low-performing vehicle types
        low_performers = fleet_comparison.sort_values('avg_efficiency').head(3)
        for _, row in low_performers.iterrows():
            recommendations.append({
                'type': 'Fleet Optimization',
                'priority': 'High',
                'title': f'Improve efficiency of {row["vehicle_type"]} vehicles',
                'description': f'{row["vehicle_type"]} vehicles have average efficiency of {row["avg_efficiency"]} km/L, '
                             f'which is {row["efficiency_score"]}% of the best performing type.',
                'action': 'Consider replacing oldest/lowest efficiency vehicles with newer/hybrid models',
                'expected_impact': f'Potential savings: ${row["potential_savings"]:,.2f} annually',
                'vehicles_affected': int(row['vehicle_count'])
            })
        
        # Recommendation 2: Address low efficiency outliers
        if not outliers.empty:
            low_outliers = outliers[outliers['outlier_type'] == 'Low Efficiency'].head(5)
            if len(low_outliers) > 0:
                vehids = low_outliers['vehid'].tolist()
                recommendations.append({
                    'type': 'Vehicle Maintenance',
                    'priority': 'Medium',
                    'title': 'Investigate low efficiency outliers',
                    'description': f'{len(low_outliers)} vehicles show significantly lower efficiency than fleet average',
                    'action': 'Schedule diagnostic checks for vehicles: ' + ', '.join(map(str, vehids)),
                    'expected_impact': 'Potential 10-15% efficiency improvement for affected vehicles',
                    'vehicles_affected': len(low_outliers)
                })
        
        # Recommendation 3: Driver training for high idling
        if 'idle_time_min' in self.telemetry.columns:
            avg_idle = self.telemetry['idle_time_min'].mean()
            if avg_idle > 10:  # More than 10 minutes average idle time
                recommendations.append({
                    'type': 'Driver Behavior',
                    'priority': 'Medium',
                    'title': 'Reduce vehicle idling time',
                    'description': f'Average idle time is {avg_idle:.1f} minutes per trip',
                    'action': 'Implement anti-idling policy and driver training',
                    'expected_impact': 'Potential 5-10% fuel savings from reduced idling',
                    'vehicles_affected': 'All fleet vehicles'
                })
        
        # Recommendation 4: Route optimization
        urban_trips = len(self.telemetry[self.telemetry['road_type'] == 'Urban'])
        total_trips = len(self.telemetry)
        if total_trips > 0 and urban_trips / total_trips > 0.7:
            recommendations.append({
                'type': 'Route Optimization',
                'priority': 'Medium',
                'title': 'Optimize urban route planning',
                'description': f'{urban_trips/total_trips*100:.1f}% of trips are in urban areas with lower efficiency',
                'action': 'Implement route optimization software for urban deliveries',
                'expected_impact': 'Potential 8-12% efficiency improvement for urban trips',
                'vehicles_affected': 'Vehicles primarily used in urban areas'
            })
        
        return recommendations
    
    def calculate_potential_savings(self, improvement_target: float = 0.1) -> Dict:
        """
        Calculate potential cost savings from efficiency improvements
        
        Args:
            improvement_target: Target efficiency improvement (e.g., 0.1 for 10%)
            
        Returns:
            Dictionary with savings calculations
        """
        # Current fuel consumption and cost
        total_fuel = self.telemetry['fuel_consumed_l'].sum()
        total_distance = self.telemetry['trip_distance_km'].sum()
        current_efficiency = total_distance / total_fuel if total_fuel > 0 else 0
        current_cost = total_fuel * 1.2
        
        # Projected improvements
        improved_efficiency = current_efficiency * (1 + improvement_target)
        projected_fuel = total_distance / improved_efficiency
        projected_cost = projected_fuel * 1.2
        
        # Calculate savings
        fuel_savings = total_fuel - projected_fuel
        cost_savings = current_cost - projected_cost
        efficiency_gain = improved_efficiency - current_efficiency
        
        # CO2 reduction (kg CO2 per liter: Gasoline=2.31, Diesel=2.68)
        avg_co2_per_liter = 2.31  # Assuming gasoline fleet
        co2_reduction = fuel_savings * avg_co2_per_liter
        
        # Breakdown by vehicle type
        savings_by_type = {}
        for vehicle_type in self.vehicles['vehicle_type'].unique():
            type_vehicles = self.vehicles[self.vehicles['vehicle_type'] == vehicle_type]['vehid']
            type_telemetry = self.telemetry[self.telemetry['vehid'].isin(type_vehicles)]
            
            if len(type_telemetry) > 0:
                type_fuel = type_telemetry['fuel_consumed_l'].sum()
                type_distance = type_telemetry['trip_distance_km'].sum()
                type_efficiency = type_distance / type_fuel if type_fuel > 0 else 0
                
                savings_by_type[vehicle_type] = {
                    'current_efficiency': round(type_efficiency, 2),
                    'fuel_consumed': round(type_fuel, 2),
                    'potential_savings': round(type_fuel * improvement_target * 1.2, 2),
                    'vehicle_count': len(type_vehicles)
                }
        
        return {
            'current_metrics': {
                'total_distance_km': round(total_distance, 2),
                'total_fuel_l': round(total_fuel, 2),
                'avg_efficiency_kmpl': round(current_efficiency, 2),
                'total_cost_usd': round(current_cost, 2)
            },
            'improvement_target': {
                'percentage': improvement_target * 100,
                'target_efficiency_kmpl': round(improved_efficiency, 2),
                'efficiency_gain': round(efficiency_gain, 2)
            },
            'projected_savings': {
                'fuel_savings_l': round(fuel_savings, 2),
                'cost_savings_usd': round(cost_savings, 2),
                'co2_reduction_kg': round(co2_reduction, 2),
                'annualized_savings': round(cost_savings * 12, 2)  # Assuming monthly data
            },
            'savings_by_vehicle_type': savings_by_type,
            'implementation_considerations': [
                'Driver training programs',
                'Regular maintenance schedule',
                'Tire pressure monitoring',
                'Route optimization',
                'Consider hybrid/electric replacements for high-mileage vehicles'
            ]
        }
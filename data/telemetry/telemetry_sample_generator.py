"""
Telemetry Sample Generator
Generates realistic vehicle telemetry data for testing and demonstration
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker
import json
import os
from typing import Dict, List, Tuple
import argparse

class TelemetrySampleGenerator:
    def __init__(self, seed=42):
        """Initialize telemetry generator"""
        np.random.seed(seed)
        random.seed(seed)
        self.fake = Faker()
        self.vehicle_types = ['ICE', 'HEV', 'PHEV', 'BEV']
        self.vehicle_classes = ['Compact', 'Midsize', 'SUV', 'Truck', 'Van']
        self.engine_types = ['Gasoline', 'Diesel', 'Hybrid', 'Electric']
        self.weather_conditions = ['Clear', 'Rain', 'Snow', 'Fog', 'Windy']
        self.road_types = ['Urban', 'Highway', 'Mixed', 'Rural', 'Mountain']
        
    def generate_vehicle_master_data(self, num_vehicles=100) -> pd.DataFrame:
        """Generate vehicle master data"""
        
        vehicles = []
        for i in range(num_vehicles):
            vehicle_type = random.choice(self.vehicle_types)
            vehicle_class = random.choice(self.vehicle_classes)
            
            # Generate realistic specifications based on type and class
            if vehicle_type == 'BEV':
                engine_type = 'Electric'
                displacement = 0.0
                is_turbo = False
                is_hybrid = False
                is_electric = True
            elif vehicle_type == 'HEV' or vehicle_type == 'PHEV':
                engine_type = 'Hybrid'
                displacement = round(random.uniform(1.5, 3.0), 1)
                is_turbo = random.choice([True, False])
                is_hybrid = True
                is_electric = False
            else:  # ICE
                engine_type = random.choice(['Gasoline', 'Diesel'])
                displacement = round(random.uniform(1.0, 6.0), 1)
                is_turbo = random.choice([True, False])
                is_hybrid = False
                is_electric = False
            
            # Generate weight based on class
            weight_map = {
                'Compact': random.randint(1200, 1800),
                'Midsize': random.randint(1800, 2200),
                'SUV': random.randint(2200, 2800),
                'Truck': random.randint(2500, 4000),
                'Van': random.randint(2000, 3000)
            }
            
            # Generate cylinders based on displacement
            if displacement == 0:
                cylinders = 0
            elif displacement <= 1.6:
                cylinders = random.choice([3, 4])
            elif displacement <= 2.5:
                cylinders = 4
            elif displacement <= 4.0:
                cylinders = random.choice([4, 6])
            else:
                cylinders = random.choice([6, 8, 10])
            
            vehicle = {
                'vehid': i + 1,
                'vehicle_type': vehicle_type,
                'vehicle_class': vehicle_class,
                'engine_type': engine_type,
                'displacement_l': displacement,
                'engine_cylinders': cylinders,
                'is_turbo': is_turbo,
                'is_hybrid': is_hybrid,
                'is_electric': is_electric,
                'weight_kg': weight_map.get(vehicle_class, 2000),
                'transmission_type': random.choice(['Automatic', 'Manual', 'CVT', 'DCT']),
                'fuel_type': 'Electric' if vehicle_type == 'BEV' else 
                            'Gasoline' if engine_type == 'Gasoline' else 
                            'Diesel',
                'manufacture_year': random.randint(2015, 2023),
                'odometer_km': random.randint(1000, 200000),
                'last_service_km': random.randint(0, 10000),
                'service_interval_km': random.choice([10000, 15000, 20000])
            }
            vehicles.append(vehicle)
        
        return pd.DataFrame(vehicles)
    
    def generate_telemetry_sample(self, 
                                  vehicle_df: pd.DataFrame, 
                                  hours: int = 24,
                                  events_per_hour: int = 100) -> pd.DataFrame:
        """Generate telemetry data for specified period"""
        
        all_telemetry = []
        start_time = datetime.now() - timedelta(hours=hours)
        
        for hour_offset in range(hours):
            hour_start = start_time + timedelta(hours=hour_offset)
            
            # Select random vehicles for this hour
            vehicles_this_hour = vehicle_df.sample(
                n=min(len(vehicle_df), events_per_hour // 10),
                replace=True
            ).to_dict('records')
            
            for vehicle in vehicles_this_hour:
                # Generate 1-5 trips per vehicle per hour
                num_trips = random.randint(1, 5)
                
                for trip in range(num_trips):
                    trip_data = self.generate_single_trip(vehicle, hour_start, trip)
                    all_telemetry.append(trip_data)
        
        return pd.DataFrame(all_telemetry)
    
    def generate_single_trip(self, vehicle: Dict, base_time: datetime, trip_num: int) -> Dict:
        """Generate a single trip telemetry record"""
        
        # Trip start time (within the hour)
        trip_start = base_time + timedelta(
            minutes=random.randint(0, 50),
            seconds=random.randint(0, 59)
        )
        
        # Trip characteristics based on vehicle type
        if vehicle['vehicle_type'] == 'BEV':
            trip_distance = random.uniform(5, 50)  # EVs for shorter trips
            base_efficiency = random.uniform(5.0, 8.0)  # km/kWh
            fuel_field = 'energy_consumed_kwh'
            fuel_value = trip_distance / base_efficiency
        elif vehicle['vehicle_type'] in ['HEV', 'PHEV']:
            trip_distance = random.uniform(10, 100)
            base_efficiency = random.uniform(18.0, 25.0)  # km/L
            fuel_field = 'fuel_consumed_l'
            fuel_value = trip_distance / base_efficiency
        else:  # ICE
            trip_distance = random.uniform(5, 200)
            base_efficiency = random.uniform(8.0, 15.0)  # km/L
            fuel_field = 'fuel_consumed_l'
            fuel_value = trip_distance / base_efficiency
        
        # Adjust efficiency based on vehicle characteristics
        efficiency_factor = 1.0
        if vehicle['is_turbo']:
            efficiency_factor *= 0.9
        if vehicle['vehicle_class'] in ['SUV', 'Truck']:
            efficiency_factor *= 0.8
        
        actual_efficiency = base_efficiency * efficiency_factor
        actual_fuel = trip_distance / actual_efficiency
        
        # Generate sensor data
        avg_speed = random.uniform(20, 100)
        trip_duration = (trip_distance / avg_speed) * 60  # minutes
        
        # Generate fault with probability
        has_fault = random.random() < 0.05  # 5% chance of fault
        fault_codes = []
        if has_fault:
            fault_codes = random.sample([
                'P0300', 'P0301', 'P0302', 'P0303', 'P0304',  # Misfire
                'P0420', 'P0430',  # Catalyst efficiency
                'P0171', 'P0172',  # Fuel system
                'P0442', 'P0455',  # EVAP system
                'P0500', 'P0501'   # Vehicle speed sensor
            ], random.randint(1, 2))
        
        # Generate maintenance flag
        maintenance_flag = random.random() < 0.03  # 3% chance
        
        # Generate location data
        location = self.fake.local_latlng()
        
        # Generate driver behavior score
        base_score = random.uniform(0.7, 1.0)
        if has_fault:
            base_score *= 0.8
        if random.random() < 0.1:  # 10% chance of poor driving
            base_score *= 0.6
        
        telemetry_record = {
            'timestamp': trip_start.isoformat(),
            'vehid': vehicle['vehid'],
            'vehicle_type': vehicle['vehicle_type'],
            'vehicle_class': vehicle['vehicle_class'],
            'engine_type': vehicle['engine_type'],
            'trip_id': f"{vehicle['vehid']}_{trip_start.strftime('%Y%m%d_%H%M')}_{trip_num}",
            'trip_distance_km': round(trip_distance, 2),
            'trip_duration_min': round(trip_duration, 1),
            'fuel_efficiency_kmpl': round(actual_efficiency, 2),
            fuel_field: round(actual_fuel, 2),
            'avg_speed_kmph': round(avg_speed, 1),
            'max_speed_kmph': round(avg_speed * random.uniform(1.1, 1.5), 1),
            'avg_rpm': random.randint(1500, 3500),
            'max_rpm': random.randint(4000, 6000),
            'avg_engine_temp_c': round(random.uniform(85, 105), 1),
            'max_engine_temp_c': round(random.uniform(95, 115), 1),
            'idle_time_min': round(trip_duration * random.uniform(0.05, 0.2), 1),
            'has_fault': has_fault,
            'fault_codes': fault_codes,
            'maintenance_flag': maintenance_flag,
            'weather_condition': random.choice(self.weather_conditions),
            'road_type': random.choice(self.road_types),
            'driver_behavior_score': round(base_score, 2),
            'harsh_acceleration_count': random.randint(0, 3),
            'harsh_braking_count': random.randint(0, 2),
            'speeding_count': random.randint(0, 5),
            'location_lat': float(location[0]),
            'location_lon': float(location[1]),
            'altitude_m': round(random.uniform(0, 2000), 1),
            'tire_pressure_front_left': round(random.uniform(2.0, 2.5), 1),
            'tire_pressure_front_right': round(random.uniform(2.0, 2.5), 1),
            'tire_pressure_rear_left': round(random.uniform(2.0, 2.5), 1),
            'tire_pressure_rear_right': round(random.uniform(2.0, 2.5), 1),
            'brake_pad_thickness_front': round(random.uniform(5.0, 12.0), 1),
            'brake_pad_thickness_rear': round(random.uniform(5.0, 12.0), 1),
            'battery_voltage': round(random.uniform(12.0, 14.5), 1),
            'battery_soc_percent': round(random.uniform(60.0, 100.0), 1) if vehicle['vehicle_type'] == 'BEV' else None
        }
        
        return telemetry_record
    
    def generate_maintenance_history(self, vehicle_df: pd.DataFrame) -> pd.DataFrame:
        """Generate maintenance history for vehicles"""
        
        maintenance_records = []
        
        for _, vehicle in vehicle_df.iterrows():
            # Generate 0-5 maintenance records per vehicle
            num_records = random.randint(0, 5)
            
            for i in range(num_records):
                record_date = datetime.now() - timedelta(days=random.randint(1, 365*3))
                
                maintenance_types = [
                    'Oil Change', 'Tire Rotation', 'Brake Service',
                    'Engine Tune-up', 'Transmission Service',
                    'Battery Replacement', 'Coolant Flush',
                    'Air Filter Replacement', 'Spark Plug Replacement',
                    'Wheel Alignment'
                ]
                
                maintenance_type = random.choice(maintenance_types)
                
                # Generate cost based on maintenance type
                cost_map = {
                    'Oil Change': random.uniform(50, 150),
                    'Tire Rotation': random.uniform(30, 80),
                    'Brake Service': random.uniform(200, 500),
                    'Engine Tune-up': random.uniform(300, 800),
                    'Transmission Service': random.uniform(400, 1200),
                    'Battery Replacement': random.uniform(100, 300),
                    'Coolant Flush': random.uniform(80, 200),
                    'Air Filter Replacement': random.uniform(20, 60),
                    'Spark Plug Replacement': random.uniform(100, 300),
                    'Wheel Alignment': random.uniform(80, 150)
                }
                
                record = {
                    'maintenance_id': len(maintenance_records) + 1,
                    'vehid': vehicle['vehid'],
                    'maintenance_date': record_date.date().isoformat(),
                    'maintenance_type': maintenance_type,
                    'description': f"{maintenance_type} performed",
                    'cost_usd': round(cost_map.get(maintenance_type, 100), 2),
                    'mileage_km': random.randint(1000, vehicle['odometer_km']),
                    'service_center': self.fake.company(),
                    'technician': self.fake.name(),
                    'parts_replaced': random.choice([
                        None,
                        'Oil Filter',
                        'Brake Pads',
                        'Air Filter',
                        'Spark Plugs',
                        'Battery',
                        'Tires'
                    ]),
                    'next_service_km': random.randint(5000, 20000),
                    'warranty_claimed': random.choice([True, False])
                }
                maintenance_records.append(record)
        
        return pd.DataFrame(maintenance_records)
    
    def generate_driver_behavior(self, telemetry_df: pd.DataFrame) -> pd.DataFrame:
        """Generate driver behavior analytics from telemetry"""
        
        driver_summary = []
        
        # Group by vehicle
        for vehid in telemetry_df['vehid'].unique():
            vehicle_data = telemetry_df[telemetry_df['vehid'] == vehid]
            
            if len(vehicle_data) == 0:
                continue
            
            # Calculate behavior metrics
            avg_score = vehicle_data['driver_behavior_score'].mean()
            total_harsh_acc = vehicle_data['harsh_acceleration_count'].sum()
            total_harsh_brake = vehicle_data['harsh_braking_count'].sum()
            total_speeding = vehicle_data['speeding_count'].sum()
            total_distance = vehicle_data['trip_distance_km'].sum()
            
            # Calculate scores
            safety_score = max(0, 100 - (total_harsh_acc * 5 + total_harsh_brake * 10 + total_speeding * 3))
            efficiency_score = (vehicle_data['fuel_efficiency_kmpl'].mean() / 20) * 100
            overall_score = (safety_score * 0.6 + efficiency_score * 0.4)
            
            # Determine driver category
            if overall_score >= 90:
                driver_category = 'Excellent'
            elif overall_score >= 80:
                driver_category = 'Good'
            elif overall_score >= 70:
                driver_category = 'Average'
            elif overall_score >= 60:
                driver_category = 'Needs Improvement'
            else:
                driver_category = 'Poor'
            
            # Generate recommendations
            recommendations = []
            if total_harsh_acc > 5:
                recommendations.append("Reduce harsh acceleration")
            if total_harsh_brake > 3:
                recommendations.append("Improve braking technique")
            if total_speeding > 10:
                recommendations.append("Observe speed limits")
            if avg_score < 0.8:
                recommendations.append("Consider defensive driving course")
            
            driver_record = {
                'vehid': vehid,
                'driver_id': f"DRV{vehid:03d}",
                'analysis_date': datetime.now().date().isoformat(),
                'total_trips_analyzed': len(vehicle_data),
                'total_distance_km': round(total_distance, 2),
                'avg_driver_score': round(avg_score, 2),
                'safety_score': round(safety_score, 1),
                'efficiency_score': round(efficiency_score, 1),
                'overall_score': round(overall_score, 1),
                'driver_category': driver_category,
                'harsh_acceleration_count': total_harsh_acc,
                'harsh_braking_count': total_harsh_brake,
                'speeding_count': total_speeding,
                'night_driving_hours': round(random.uniform(0, 50), 1),
                'idle_time_percentage': round(vehicle_data['idle_time_min'].sum() / 
                                            vehicle_data['trip_duration_min'].sum() * 100, 1),
                'recommendations': '; '.join(recommendations) if recommendations else 'No specific recommendations',
                'co2_emissions_kg': round(vehicle_data['fuel_consumed_l'].sum() * 2.31, 1),
                'fuel_cost_usd': round(vehicle_data['fuel_consumed_l'].sum() * 1.2, 2),
                'insurance_premium_factor': round(1.0 - (overall_score / 200), 2)
            }
            
            driver_summary.append(driver_record)
        
        return pd.DataFrame(driver_summary)
    
    def save_to_files(self, 
                     vehicle_df: pd.DataFrame, 
                     telemetry_df: pd.DataFrame,
                     maintenance_df: pd.DataFrame,
                     driver_df: pd.DataFrame,
                     output_dir: str = 'data/telemetry'):
        """Save generated data to files"""
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Save as CSV
        vehicle_df.to_csv(f'{output_dir}/vehicle_master_data.csv', index=False)
        telemetry_df.to_csv(f'{output_dir}/telemetry_data.csv', index=False)
        maintenance_df.to_csv(f'{output_dir}/maintenance_history.csv', index=False)
        driver_df.to_csv(f'{output_dir}/driver_behavior.csv', index=False)
        
        # Save as JSON for API consumption
        telemetry_df.head(1000).to_json(
            f'{output_dir}/telemetry_sample.json',
            orient='records',
            date_format='iso'
        )
        
        # Save sample data for quick testing
        sample_data = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'num_vehicles': len(vehicle_df),
                'num_telemetry_records': len(telemetry_df),
                'num_maintenance_records': len(maintenance_df),
                'num_driver_records': len(driver_df)
            },
            'summary_stats': {
                'vehicle_types': vehicle_df['vehicle_type'].value_counts().to_dict(),
                'vehicle_classes': vehicle_df['vehicle_class'].value_counts().to_dict(),
                'avg_efficiency': telemetry_df['fuel_efficiency_kmpl'].mean(),
                'fault_rate': telemetry_df['has_fault'].mean() * 100,
                'maintenance_rate': telemetry_df['maintenance_flag'].mean() * 100
            }
        }
        
        with open(f'{output_dir}/sample_metadata.json', 'w') as f:
            json.dump(sample_data, f, indent=2)
        
        print(f"Data saved to {output_dir}/")
        print(f"- Vehicles: {len(vehicle_df)} records")
        print(f"- Telemetry: {len(telemetry_df)} records")
        print(f"- Maintenance: {len(maintenance_df)} records")
        print(f"- Driver Behavior: {len(driver_df)} records")

def main():
    parser = argparse.ArgumentParser(description='Generate vehicle telemetry sample data')
    parser.add_argument('--num-vehicles', type=int, default=50,
                       help='Number of vehicles to generate')
    parser.add_argument('--hours', type=int, default=24,
                       help='Hours of telemetry data to generate')
    parser.add_argument('--events-per-hour', type=int, default=100,
                       help='Telemetry events per hour')
    parser.add_argument('--output-dir', type=str, default='data/telemetry',
                       help='Output directory for generated files')
    parser.add_argument('--seed', type=int, default=42,
                       help='Random seed for reproducibility')
    
    args = parser.parse_args()
    
    print("Generating vehicle telemetry sample data...")
    print(f"Number of vehicles: {args.num_vehicles}")
    print(f"Time period: {args.hours} hours")
    print(f"Events per hour: {args.events_per_hour}")
    
    # Initialize generator
    generator = TelemetrySampleGenerator(seed=args.seed)
    
    # Generate data
    print("\n1. Generating vehicle master data...")
    vehicle_df = generator.generate_vehicle_master_data(args.num_vehicles)
    
    print("2. Generating telemetry data...")
    telemetry_df = generator.generate_telemetry_sample(
        vehicle_df, 
        hours=args.hours,
        events_per_hour=args.events_per_hour
    )
    
    print("3. Generating maintenance history...")
    maintenance_df = generator.generate_maintenance_history(vehicle_df)
    
    print("4. Generating driver behavior analytics...")
    driver_df = generator.generate_driver_behavior(telemetry_df)
    
    # Save to files
    print("\n5. Saving data to files...")
    generator.save_to_files(
        vehicle_df, 
        telemetry_df,
        maintenance_df,
        driver_df,
        args.output_dir
    )
    
    print("\nData generation complete!")
    print(f"Output directory: {args.output_dir}")

if __name__ == "__main__":
    main()
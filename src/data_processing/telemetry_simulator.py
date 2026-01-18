import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker

class TelemetrySimulator:
    def __init__(self, vehicle_data: pd.DataFrame, days: int = 30):
        self.vehicle_data = vehicle_data
        self.days = days
        self.fake = Faker()
        
    def generate_telemetry_data(self, vehicles_per_day: int = 50) -> pd.DataFrame:
        """Generate simulated telemetry data"""
        all_records = []
        current_date = datetime.now() - timedelta(days=self.days)
        
        for day in range(self.days):
            date = current_date + timedelta(days=day)
            
            # Select random vehicles for the day
            daily_vehicles = self.vehicle_data.sample(
                n=min(vehicles_per_day, len(self.vehicle_data)),
                replace=True
            )
            
            for _, vehicle in daily_vehicles.iterrows():
                records = self._generate_daily_telemetry(vehicle, date)
                all_records.extend(records)
        
        telemetry_df = pd.DataFrame(all_records)
        return telemetry_df
    
    def _generate_daily_telemetry(self, vehicle: pd.Series, date: datetime) -> List[Dict]:
        """Generate telemetry for a single vehicle on a single day"""
        records = []
        vehid = vehicle['vehid']
        
        # Generate trip characteristics based on vehicle type
        if vehicle['is_hybrid']:
            num_trips = random.randint(3, 8)
            avg_distance = random.uniform(8, 25)  # km
        elif vehicle['vehicle_type'] == 'SUV':
            num_trips = random.randint(2, 5)
            avg_distance = random.uniform(15, 50)
        else:  # Regular ICE cars
            num_trips = random.randint(4, 10)
            avg_distance = random.uniform(5, 20)
        
        total_distance = 0
        total_fuel = 0
        
        for trip in range(num_trips):
            trip_start = date.replace(
                hour=random.randint(6, 20),
                minute=random.randint(0, 59)
            )
            
            # Trip distance with some randomness
            trip_distance = max(0.5, np.random.normal(avg_distance, avg_distance * 0.3))
            total_distance += trip_distance
            
            # Calculate fuel consumption based on vehicle characteristics
            base_consumption = vehicle['displacement_l'] * 0.8  # L/100km base
            if vehicle['is_turbo']:
                base_consumption *= 1.1
            if vehicle['is_hybrid']:
                base_consumption *= 0.6
            
            # Add efficiency factor
            efficiency_factor = 1 / (vehicle['efficiency_score'] / 1000)
            trip_fuel = (trip_distance / 100) * base_consumption * efficiency_factor
            total_fuel += trip_fuel
            
            # Calculate fuel efficiency
            fuel_efficiency = trip_distance / trip_fuel if trip_fuel > 0 else 0
            
            # Generate sensor readings
            engine_temp = np.random.normal(90, 5)  # Celsius
            rpm = np.random.normal(2500, 500)
            speed = np.random.normal(60, 15)
            
            # Generate fault indicators
            fault_codes = self._generate_fault_codes(vehicle, total_distance)
            
            # Calculate idle time (5-15% of trip time)
            trip_duration = trip_distance / max(1, speed) * 60  # minutes
            idle_time = trip_duration * random.uniform(0.05, 0.15)
            
            record = {
                'timestamp': trip_start,
                'vehid': vehid,
                'vehicle_type': vehicle['vehicle_type'],
                'vehicle_class': vehicle['vehicle_class'],
                'trip_id': f"{vehid}_{date.strftime('%Y%m%d')}_{trip}",
                'trip_distance_km': round(trip_distance, 2),
                'trip_duration_min': round(trip_duration, 1),
                'fuel_consumed_l': round(trip_fuel, 2),
                'fuel_efficiency_kmpl': round(fuel_efficiency, 2),
                'avg_speed_kmph': round(speed, 1),
                'avg_rpm': round(rpm),
                'avg_engine_temp_c': round(engine_temp, 1),
                'idle_time_min': round(idle_time, 1),
                'fault_codes': ','.join(fault_codes) if fault_codes else None,
                'has_fault': len(fault_codes) > 0,
                'maintenance_flag': self._check_maintenance_needed(vehicle, total_distance),
                'weather_condition': random.choice(['Clear', 'Rain', 'Snow', 'Fog']),
                'road_type': random.choice(['Urban', 'Highway', 'Mixed', 'Rural']),
                'driver_behavior_score': random.uniform(0.7, 1.0)  # 0-1 scale
            }
            
            records.append(record)
        
        # Add daily summary
        daily_avg_efficiency = total_distance / total_fuel if total_fuel > 0 else 0
        
        summary_record = {
            'timestamp': date.replace(hour=23, minute=59, second=59),
            'vehid': vehid,
            'vehicle_type': vehicle['vehicle_type'],
            'vehicle_class': vehicle['vehicle_class'],
            'trip_id': f"{vehid}_{date.strftime('%Y%m%d')}_SUMMARY",
            'trip_distance_km': round(total_distance, 2),
            'trip_duration_min': round(total_distance / 40 * 60, 1),  # Assuming 40 kmph avg
            'fuel_consumed_l': round(total_fuel, 2),
            'fuel_efficiency_kmpl': round(daily_avg_efficiency, 2),
            'avg_speed_kmph': round(total_distance / (total_distance / 40), 1),
            'avg_rpm': 0,
            'avg_engine_temp_c': 0,
            'idle_time_min': round(total_distance / 40 * 60 * 0.1, 1),  # 10% idle
            'fault_codes': '',
            'has_fault': False,
            'maintenance_flag': False,
            'weather_condition': 'Daily Summary',
            'road_type': 'Mixed',
            'driver_behavior_score': random.uniform(0.7, 1.0)
        }
        
        records.append(summary_record)
        
        return records
    
    def _generate_fault_codes(self, vehicle: pd.Series, total_distance: float) -> List[str]:
        """Generate realistic fault codes based on vehicle age and usage"""
        fault_codes = []
        fault_probability = min(0.1, total_distance / 100000)  # Up to 10% chance
        
        if random.random() < fault_probability:
            common_faults = {
                'ICE': ['P0300', 'P0420', 'P0171', 'P0442'],
                'HEV': ['P0A80', 'P1A00', 'P3000', 'P3100']
            }
            
            fault_type = common_faults.get(vehicle['vehicle_type'], common_faults['ICE'])
            fault_codes.append(random.choice(fault_type))
            
            # Additional fault based on vehicle characteristics
            if vehicle['is_turbo'] and random.random() < 0.3:
                fault_codes.append('P0299')  # Turbo underboost
            
            if vehicle['is_hybrid'] and random.random() < 0.2:
                fault_codes.append('P0A1F')  # Hybrid battery fault
        
        return fault_codes
    
    def _check_maintenance_needed(self, vehicle: pd.Series, total_distance: float) -> bool:
        """Check if maintenance is needed based on distance and vehicle type"""
        maintenance_intervals = {
            'ICE': 10000,  # km
            'HEV': 15000,  # km
        }
        
        interval = maintenance_intervals.get(vehicle['vehicle_type'], 12000)
        cycles = total_distance // interval
        
        # Maintenance probability increases with cycles
        return random.random() < min(0.8, cycles * 0.1)
    
    def save_telemetry_data(self, df: pd.DataFrame, output_path: str):
        """Save telemetry data to CSV"""
        df.to_csv(output_path, index=False)
        print(f"Telemetry data saved to {output_path}")
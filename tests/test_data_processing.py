"""
Unit tests for data processing modules in Vehicle Telemetry Analytics Platform.
"""
import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_processing.data_cleaner import DataCleaner
from data_processing.data_enricher import DataEnricher
from data_processing.telemetry_simulator import TelemetrySimulator


class TestDataCleaner(unittest.TestCase):
    """Test cases for DataCleaner."""
    
    def setUp(self):
        """Set up test data with various issues."""
        np.random.seed(42)
        
        # Create sample data with issues
        self.dirty_data = pd.DataFrame({
            'vehicle_id': ['VH001', 'VH001', 'VH001', 'VH001', 'VH001', 
                          'VH002', 'VH002', None, 'VH003', 'VH003'],
            'timestamp': [
                '2024-01-01 10:00:00',
                '2024-01-01 10:00:10',
                '2024-01-01 10:00:20',
                'invalid_date',
                '2024-01-01 10:00:40',
                '2024-01-01 10:00:50',
                '2024-01-01 10:01:00',
                '2024-01-01 10:01:10',
                '2024-01-01 10:01:20',
                '2024-01-01 10:01:30'
            ],
            'speed_kmh': [60, 65, 70, 75, 80, 85, 90, 95, 100, 105],
            'rpm': [2000, 2100, 2200, 2300, 2400, 2500, 2600, 2700, 2800, 2900],
            'engine_temperature_c': [90, 92, 94, 96, 98, 100, 102, 104, 106, 108],
            'fuel_level_percent': [50, 49, 48, 47, 46, 45, 44, 43, 42, 41],
            'latitude': [40.7128, 40.7130, 40.7132, 40.7134, 40.7136,
                        40.7138, 40.7140, 40.7142, 40.7144, 40.7146],
            'longitude': [-74.0060, -74.0058, -74.0056, -74.0054, -74.0052,
                         -74.0050, -74.0048, -74.0046, -74.0044, -74.0042],
            'battery_voltage': [12.6, 12.5, 12.4, 12.3, 12.2,
                               12.1, 12.0, 11.9, 11.8, 11.7]
        })
        
        # Add some outliers and NaN values
        self.dirty_data.loc[2, 'speed_kmh'] = 200  # Unrealistically high speed
        self.dirty_data.loc[5, 'rpm'] = -100       # Negative RPM
        self.dirty_data.loc[7, 'engine_temperature_c'] = np.nan  # Missing value
        self.dirty_data.loc[9, 'fuel_level_percent'] = 150       # Out of range
        
        self.cleaner = DataCleaner(self.dirty_data)
    
    def test_initialization(self):
        """Test DataCleaner initialization."""
        self.assertIsNotNone(self.cleaner)
        self.assertEqual(len(self.cleaner.data), 10)
        self.assertEqual(self.cleaner.data['vehicle_id'].iloc[0], 'VH001')
    
    def test_clean_timestamps(self):
        """Test timestamp cleaning."""
        cleaned = self.cleaner.clean_timestamps()
        
        # Should convert string timestamps to datetime
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(cleaned['timestamp']))
        
        # Should handle invalid dates
        self.assertEqual(cleaned['timestamp'].isna().sum(), 1)  # One invalid date
        
        # Check specific conversion
        self.assertEqual(cleaned['timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M:%S'),
                        '2024-01-01 10:00:00')
    
    def test_handle_missing_values(self):
        """Test missing value handling."""
        # Test different strategies
        cleaner_interpolate = DataCleaner(self.dirty_data.copy())
        cleaned_interpolate = cleaner_interpolate.handle_missing_values(
            strategy='interpolate'
        )
        
        cleaner_mean = DataCleaner(self.dirty_data.copy())
        cleaned_mean = cleaner_mean.handle_missing_values(strategy='mean')
        
        cleaner_drop = DataCleaner(self.dirty_data.copy())
        cleaned_drop = cleaner_drop.handle_missing_values(strategy='drop')
        
        # Check interpolate strategy
        self.assertFalse(cleaned_interpolate['engine_temperature_c'].isna().any())
        
        # Check mean strategy
        self.assertFalse(cleaned_mean['engine_temperature_c'].isna().any())
        
        # Check drop strategy (should have fewer rows)
        self.assertLess(len(cleaned_drop), len(self.dirty_data))
        self.assertFalse(cleaned_drop['engine_temperature_c'].isna().any())
    
    def test_remove_outliers(self):
        """Test outlier removal."""
        cleaned = self.cleaner.remove_outliers()
        
        # Speed outlier should be removed
        self.assertNotIn(200, cleaned['speed_kmh'].values)
        
        # Negative RPM should be removed or corrected
        self.assertTrue((cleaned['rpm'] >= 0).all())
        
        # Fuel level should be within 0-100
        self.assertTrue((cleaned['fuel_level_percent'] >= 0).all())
        self.assertTrue((cleaned['fuel_level_percent'] <= 100).all())
        
        # Should have fewer rows than original
        self.assertLessEqual(len(cleaned), len(self.dirty_data))
    
    def test_validate_ranges(self):
        """Test range validation."""
        validation = self.cleaner.validate_ranges()
        
        self.assertIsInstance(validation, dict)
        self.assertIn('valid_rows', validation)
        self.assertIn('invalid_rows', validation)
        self.assertIn('violations', validation)
        
        # Check specific violations
        violations = validation['violations']
        
        # Should detect speed violation
        self.assertIn('speed_kmh', violations)
        self.assertGreater(violations['speed_kmh'], 0)
        
        # Should detect RPM violation
        self.assertIn('rpm', violations)
        self.assertGreater(violations['rpm'], 0)
        
        # Should detect fuel level violation
        self.assertIn('fuel_level_percent', violations)
        self.assertGreater(violations['fuel_level_percent'], 0)
    
    def test_deduplicate_data(self):
        """Test data deduplication."""
        # Create data with duplicates
        duplicate_data = pd.concat([self.dirty_data, self.dirty_data.iloc[:3]], 
                                  ignore_index=True)
        cleaner = DataCleaner(duplicate_data)
        deduplicated = cleaner.deduplicate_data()
        
        # Should have fewer rows than with duplicates
        self.assertLess(len(deduplicated), len(duplicate_data))
        
        # Check no exact duplicates remain
        duplicate_mask = deduplicated.duplicated(subset=['vehicle_id', 'timestamp'], 
                                                keep=False)
        self.assertFalse(duplicate_mask.any())
    
    def test_clean_all(self):
        """Test comprehensive cleaning."""
        cleaned = self.cleaner.clean_all()
        
        # Check all cleaning operations were applied
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(cleaned['timestamp']))
        self.assertFalse(cleaned.isna().any().any())  # No missing values
        self.assertTrue((cleaned['speed_kmh'] >= 0).all())
        self.assertTrue((cleaned['speed_kmh'] <= 200).all())  # Reasonable max
        self.assertTrue((cleaned['rpm'] >= 0).all())
        self.assertTrue((cleaned['fuel_level_percent'] >= 0).all())
        self.assertTrue((cleaned['fuel_level_percent'] <= 100).all())
        
        # Should have valid vehicle IDs
        self.assertFalse(cleaned['vehicle_id'].isna().any())
    
    def test_get_cleaning_report(self):
        """Test cleaning report generation."""
        report = self.cleaner.get_cleaning_report()
        
        self.assertIsInstance(report, dict)
        self.assertIn('original_row_count', report)
        self.assertIn('cleaned_row_count', report)
        self.assertIn('rows_removed', report)
        self.assertIn('missing_values_handled', report)
        self.assertIn('outliers_removed', report)
        self.assertIn('validation_violations', report)
        
        # Check counts make sense
        self.assertEqual(report['original_row_count'], len(self.dirty_data))
        self.assertLessEqual(report['cleaned_row_count'], report['original_row_count'])
        self.assertEqual(report['rows_removed'], 
                        report['original_row_count'] - report['cleaned_row_count'])


class TestDataEnricher(unittest.TestCase):
    """Test cases for DataEnricher."""
    
    def setUp(self):
        """Set up test data."""
        np.random.seed(42)
        
        self.clean_data = pd.DataFrame({
            'vehicle_id': ['VH001'] * 10,
            'timestamp': pd.date_range(start='2024-01-01 10:00:00', 
                                      periods=10, freq='10S'),
            'speed_kmh': np.random.uniform(0, 100, 10),
            'latitude': np.random.uniform(40.0, 41.0, 10),
            'longitude': np.random.uniform(-74.0, -73.0, 10),
            'engine_temperature_c': np.random.uniform(80, 110, 10),
            'fuel_level_percent': np.random.uniform(20, 100, 10)
        })
        
        # Add static vehicle data
        self.vehicle_data = pd.DataFrame({
            'vehicle_id': ['VH001', 'VH002', 'VH003'],
            'make': ['Toyota', 'Honda', 'Ford'],
            'model': ['Camry', 'Civic', 'F-150'],
            'year': [2020, 2021, 2019],
            'fuel_type': ['Petrol', 'Petrol', 'Diesel'],
            'engine_capacity_cc': [2500, 1800, 3500]
        })
        
        self.enricher = DataEnricher(self.clean_data, self.vehicle_data)
    
    def test_enrich_with_vehicle_info(self):
        """Test enrichment with vehicle information."""
        enriched = self.enricher.enrich_with_vehicle_info()
        
        # Check new columns added
        expected_columns = ['make', 'model', 'year', 'fuel_type', 'engine_capacity_cc']
        for col in expected_columns:
            self.assertIn(col, enriched.columns)
        
        # Check values match
        self.assertEqual(enriched['make'].iloc[0], 'Toyota')
        self.assertEqual(enriched['model'].iloc[0], 'Camry')
        self.assertEqual(enriched['year'].iloc[0], 2020)
        
        # Should handle missing vehicle info gracefully
        test_data = pd.DataFrame({
            'vehicle_id': ['UNKNOWN'] * 3,
            'timestamp': pd.date_range(start='2024-01-01', periods=3),
            'speed_kmh': [60, 65, 70]
        })
        
        enricher = DataEnricher(test_data, self.vehicle_data)
        enriched_unknown = enricher.enrich_with_vehicle_info()
        
        # Unknown vehicle should have NaN or default values
        self.assertTrue(enriched_unknown['make'].isna().all())
    
    def test_calculate_derived_metrics(self):
        """Test calculation of derived metrics."""
        # Add some realistic data patterns
        data = self.clean_data.copy()
        data['speed_kmh'] = [0, 30, 60, 90, 60, 30, 0, 30, 60, 90]  # Simulate acceleration
        
        enricher = DataEnricher(data, self.vehicle_data)
        enriched = enricher.calculate_derived_metrics()
        
        # Check derived columns
        self.assertIn('acceleration_kmh_s', enriched.columns)
        self.assertIn('is_moving', enriched.columns)
        self.assertIn('speed_category', enriched.columns)
        
        # Check acceleration calculation
        # From 0 to 30 km/h in 10 seconds = 3 km/h/s
        self.assertAlmostEqual(enriched['acceleration_kmh_s'].iloc[1], 3.0, delta=0.1)
        
        # Check speed categories
        self.assertIn('stopped', enriched['speed_category'].values)
        self.assertIn('urban', enriched['speed_category'].values)
        self.assertIn('highway', enriched['speed_category'].values)
    
    def test_add_time_features(self):
        """Test addition of time-based features."""
        enriched = self.enricher.add_time_features()
        
        # Check time features added
        time_features = ['hour', 'minute', 'day_of_week', 'month', 'year', 
                        'is_weekend', 'time_of_day']
        
        for feature in time_features:
            self.assertIn(feature, enriched.columns)
        
        # Check specific values
        self.assertEqual(enriched['hour'].iloc[0], 10)
        self.assertEqual(enriched['day_of_week'].iloc[0], 0)  # Monday
        self.assertEqual(enriched['month'].iloc[0], 1)  # January
        
        # Check time_of_day categories
        valid_categories = ['morning', 'afternoon', 'evening', 'night']
        self.assertTrue(enriched['time_of_day'].isin(valid_categories).all())
    
    def test_calculate_distance_traveled(self):
        """Test distance calculation."""
        # Create data with realistic coordinates
        data = pd.DataFrame({
            'vehicle_id': ['VH001'] * 5,
            'timestamp': pd.date_range(start='2024-01-01', periods=5, freq='10S'),
            'latitude': [40.7128, 40.7130, 40.7132, 40.7134, 40.7136],
            'longitude': [-74.0060, -74.0058, -74.0056, -74.0054, -74.0052]
        })
        
        enricher = DataEnricher(data, self.vehicle_data)
        enriched = enricher.calculate_distance_traveled()
        
        # Check distance columns
        self.assertIn('distance_km', enriched.columns)
        self.assertIn('cumulative_distance_km', enriched.columns)
        self.assertIn('speed_calculated_kmh', enriched.columns)
        
        # Distance should increase
        distances = enriched['distance_km'].dropna()
        self.assertTrue((distances > 0).all())
        
        # Cumulative distance should be monotonic increasing
        cumulative = enriched['cumulative_distance_km'].dropna()
        self.assertTrue((cumulative.diff().dropna() >= 0).all())
    
    def test_detect_events(self):
        """Test event detection."""
        # Create data with events
        data = pd.DataFrame({
            'vehicle_id': ['VH001'] * 20,
            'timestamp': pd.date_range(start='2024-01-01', periods=20, freq='1S'),
            'speed_kmh': [0] * 5 + [30] * 5 + [0] * 5 + [60] * 5,  # Stop-start pattern
            'engine_temperature_c': [90] * 20,
            'fuel_level_percent': [50] * 20
        })
        
        enricher = DataEnricher(data, self.vehicle_data)
        enriched = enricher.detect_events()
        
        # Check event columns
        self.assertIn('event_type', enriched.columns)
        self.assertIn('event_timestamp', enriched.columns)
        
        # Should detect some events
        events = enriched[enriched['event_type'].notna()]
        self.assertGreater(len(events), 0)
        
        # Check event types
        event_types = events['event_type'].unique()
        possible_events = ['engine_start', 'engine_stop', 'harsh_acceleration', 
                         'harsh_braking', 'speeding']
        
        for event in event_types:
            self.assertIn(event, possible_events)
    
    def test_enrich_all(self):
        """Test comprehensive enrichment."""
        enriched = self.enricher.enrich_all()
        
        # Check all enrichment operations were applied
        expected_columns = [
            'make', 'model', 'year',  # From vehicle info
            'acceleration_kmh_s', 'is_moving', 'speed_category',  # Derived metrics
            'hour', 'day_of_week', 'is_weekend', 'time_of_day',  # Time features
            'distance_km', 'cumulative_distance_km',  # Distance
            'event_type'  # Events
        ]
        
        for col in expected_columns:
            self.assertIn(col, enriched.columns)
        
        # Should have same number of rows as input
        self.assertEqual(len(enriched), len(self.clean_data))
    
    def test_get_enrichment_report(self):
        """Test enrichment report generation."""
        report = self.enricher.get_enrichment_report()
        
        self.assertIsInstance(report, dict)
        self.assertIn('original_columns', report)
        self.assertIn('enriched_columns', report)
        self.assertIn('columns_added', report)
        self.assertIn('events_detected', report)
        self.assertIn('vehicle_info_added', report)
        
        # Check counts
        self.assertLess(report['original_columns'], report['enriched_columns'])
        self.assertEqual(report['columns_added'], 
                        report['enriched_columns'] - report['original_columns'])


class TestTelemetrySimulator(unittest.TestCase):
    """Test cases for TelemetrySimulator."""
    
    def setUp(self):
        """Set up simulator."""
        self.simulator = TelemetrySimulator(
            vehicle_count=3,
            simulation_duration_hours=1,
            frequency_seconds=10
        )
    
    def test_initialization(self):
        """Test simulator initialization."""
        self.assertIsNotNone(self.simulator)
        self.assertEqual(self.simulator.vehicle_count, 3)
        self.assertEqual(self.simulator.simulation_duration_hours, 1)
        self.assertEqual(self.simulator.frequency_seconds, 10)
        
        # Check vehicle IDs
        self.assertEqual(len(self.simulator.vehicle_ids), 3)
        self.assertTrue(all(vid.startswith('VH') for vid in self.simulator.vehicle_ids))
    
    def test_generate_telemetry_data(self):
        """Test telemetry data generation."""
        data = self.simulator.generate_telemetry_data()
        
        # Check DataFrame structure
        self.assertIsInstance(data, pd.DataFrame)
        self.assertGreater(len(data), 0)
        
        # Check columns
        expected_columns = [
            'vehicle_id', 'timestamp', 'speed_kmh', 'rpm', 
            'engine_temperature_c', 'fuel_level_percent',
            'latitude', 'longitude', 'battery_voltage'
        ]
        
        for col in expected_columns:
            self.assertIn(col, data.columns)
        
        # Check data types
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(data['timestamp']))
        self.assertTrue(pd.api.types.is_numeric_dtype(data['speed_kmh']))
        self.assertTrue(pd.api.types.is_numeric_dtype(data['rpm']))
        
        # Check value ranges
        self.assertTrue((data['speed_kmh'] >= 0).all())
        self.assertTrue((data['speed_kmh'] <= 200).all())
        self.assertTrue((data['rpm'] >= 0).all())
        self.assertTrue((data['rpm'] <= 8000).all())
        self.assertTrue((data['engine_temperature_c'] >= 70).all())
        self.assertTrue((data['engine_temperature_c'] <= 120).all())
        self.assertTrue((data['fuel_level_percent'] >= 0).all())
        self.assertTrue((data['fuel_level_percent'] <= 100).all())
        
        # Check timestamps are sequential
        for vehicle_id in self.simulator.vehicle_ids:
            vehicle_data = data[data['vehicle_id'] == vehicle_id]
            timestamps = vehicle_data['timestamp'].sort_values()
            self.assertTrue(timestamps.is_monotonic_increasing)
    
    def test_add_anomalies(self):
        """Test anomaly addition."""
        normal_data = self.simulator.generate_telemetry_data()
        anomalous_data = self.simulator.add_anomalies(normal_data, anomaly_rate=0.1)
        
        # Should have same structure
        self.assertEqual(len(anomalous_data), len(normal_data))
        self.assertEqual(set(anomalous_data.columns), set(normal_data.columns))
        
        # Check for anomalies (some values should be outside normal range)
        # Speed anomalies
        speed_anomalies = anomalous_data[
            (anomalous_data['speed_kmh'] > 150) | 
            (anomalous_data['speed_kmh'] < 0)
        ]
        
        # Temperature anomalies
        temp_anomalies = anomalous_data[
            (anomalous_data['engine_temperature_c'] > 120) | 
            (anomalous_data['engine_temperature_c'] < 70)
        ]
        
        # Should have some anomalies based on anomaly_rate
        total_expected_anomalies = len(normal_data) * 0.1
        total_actual_anomalies = len(speed_anomalies) + len(temp_anomalies)
        
        # Allow some variance
        self.assertLessEqual(abs(total_actual_anomalies - total_expected_anomalies), 
                            total_expected_anomalies * 0.5)
    
    def test_simulate_driving_patterns(self):
        """Test driving pattern simulation."""
        # Test different patterns
        patterns = ['urban', 'highway', 'mixed', 'delivery']
        
        for pattern in patterns:
            simulator = TelemetrySimulator(
                vehicle_count=1,
                simulation_duration_hours=0.5,
                frequency_seconds=10,
                driving_pattern=pattern
            )
            
            data = simulator.generate_telemetry_data()
            
            # Check pattern characteristics
            if pattern == 'urban':
                # Urban driving has frequent stops and lower speeds
                avg_speed = data['speed_kmh'].mean()
                zero_speed_count = (data['speed_kmh'] == 0).sum()
                
                self.assertLess(avg_speed, 60)  # Lower average speed
                self.assertGreater(zero_speed_count, 0)  # Some stops
            
            elif pattern == 'highway':
                # Highway driving has higher speeds, fewer stops
                avg_speed = data['speed_kmh'].mean()
                zero_speed_count = (data['speed_kmh'] == 0).sum()
                
                self.assertGreater(avg_speed, 60)  # Higher average speed
                self.assertEqual(zero_speed_count, 0)  # No stops
            
            elif pattern == 'delivery':
                # Delivery pattern has many stops/starts
                speed_changes = data['speed_kmh'].diff().abs()
                significant_changes = (speed_changes > 20).sum()
                
                self.assertGreater(significant_changes, 0)  # Many speed changes
    
    def test_generate_fault_data(self):
        """Test fault data generation."""
        telemetry_data = self.simulator.generate_telemetry_data()
        fault_data = self.simulator.generate_fault_data(telemetry_data, fault_rate=0.05)
        
        self.assertIsInstance(fault_data, pd.DataFrame)
        
        # Check fault data structure
        expected_columns = [
            'vehicle_id', 'timestamp', 'fault_code', 
            'fault_description', 'severity', 'component'
        ]
        
        for col in expected_columns:
            self.assertIn(col, fault_data.columns)
        
        # Check fault types
        valid_severities = ['low', 'medium', 'high', 'critical']
        self.assertTrue(fault_data['severity'].isin(valid_severities).all())
        
        # Check fault rate
        total_telemetry_points = len(telemetry_data)
        expected_faults = total_telemetry_points * 0.05
        actual_faults = len(fault_data)
        
        # Allow some variance
        self.assertLessEqual(abs(actual_faults - expected_faults), 
                            expected_faults * 0.5)
    
    def test_save_and_load_data(self):
        """Test data saving and loading."""
        import tempfile
        import os
        
        # Generate test data
        data = self.simulator.generate_telemetry_data()
        
        # Save to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            # Test saving
            self.simulator.save_data(data, temp_path, format='csv')
            self.assertTrue(os.path.exists(temp_path))
            
            # Test loading
            loaded_data = self.simulator.load_data(temp_path, format='csv')
            
            # Check data integrity
            self.assertEqual(len(loaded_data), len(data))
            self.assertEqual(set(loaded_data.columns), set(data.columns))
            
            # Check a few values
            self.assertEqual(loaded_data['vehicle_id'].iloc[0], data['vehicle_id'].iloc[0])
            
        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_generate_simulation_report(self):
        """Test simulation report generation."""
        telemetry_data = self.simulator.generate_telemetry_data()
        fault_data = self.simulator.generate_fault_data(telemetry_data)
        
        report = self.simulator.generate_simulation_report(telemetry_data, fault_data)
        
        self.assertIsInstance(report, dict)
        self.assertIn('simulation_parameters', report)
        self.assertIn('telemetry_summary', report)
        self.assertIn('fault_summary', report)
        self.assertIn('data_quality', report)
        
        # Check simulation parameters
        params = report['simulation_parameters']
        self.assertEqual(params['vehicle_count'], self.simulator.vehicle_count)
        self.assertEqual(params['simulation_duration_hours'], 
                        self.simulator.simulation_duration_hours)
        
        # Check telemetry summary
        summary = report['telemetry_summary']
        self.assertIn('total_records', summary)
        self.assertIn('unique_vehicles', summary)
        self.assertIn('time_range', summary)
        
        # Check fault summary
        fault_summary = report['fault_summary']
        if len(fault_data) > 0:
            self.assertIn('total_faults', fault_summary)
            self.assertIn('faults_by_severity', fault_summary)
            self.assertIn('faults_by_component', fault_summary)


class TestDataProcessingIntegration(unittest.TestCase):
    """Integration tests for data processing pipeline."""
    
    def test_complete_data_pipeline(self):
        """Test complete data processing pipeline."""
        # Step 1: Generate simulated data
        simulator = TelemetrySimulator(
            vehicle_count=2,
            simulation_duration_hours=0.5,
            frequency_seconds=30,
            driving_pattern='mixed'
        )
        
        raw_data = simulator.generate_telemetry_data()
        fault_data = simulator.generate_fault_data(raw_data, fault_rate=0.1)
        
        # Step 2: Clean the data
        cleaner = DataCleaner(raw_data)
        cleaned_data = cleaner.clean_all()
        cleaning_report = cleaner.get_cleaning_report()
        
        # Step 3: Enrich the data
        # Create sample vehicle data
        vehicle_data = pd.DataFrame({
            'vehicle_id': ['VH001', 'VH002'],
            'make': ['Toyota', 'Honda'],
            'model': ['Camry', 'Civic'],
            'year': [2020, 2021],
            'fuel_type': ['Petrol', 'Petrol']
        })
        
        enricher = DataEnricher(cleaned_data, vehicle_data)
        enriched_data = enricher.enrich_all()
        enrichment_report = enricher.get_enrichment_report()
        
        # Verify pipeline results
        self.assertGreater(len(raw_data), 0)
        self.assertGreaterEqual(len(cleaned_data), 0)
        self.assertGreaterEqual(len(enriched_data), 0)
        
        # Check data quality improved
        self.assertLessEqual(cleaning_report['rows_removed'], 
                            cleaning_report['original_row_count'] * 0.2)  # Reasonable removal
        
        # Check enrichment added value
        self.assertGreater(enrichment_report['enriched_columns'],
                          enrichment_report['original_columns'])
        
        # Check final data quality
        self.assertFalse(enriched_data.isna().any().any())  # No missing values
        self.assertTrue((enriched_data['speed_kmh'] >= 0).all())
        self.assertTrue((enriched_data['speed_kmh'] <= 200).all())
        self.assertTrue((enriched_data['fuel_level_percent'] >= 0).all())
        self.assertTrue((enriched_data['fuel_level_percent'] <= 100).all())
        
        # Check derived features exist
        self.assertIn('acceleration_kmh_s', enriched_data.columns)
        self.assertIn('distance_km', enriched_data.columns)
        self.assertIn('make', enriched_data.columns)
        self.assertIn('model', enriched_data.columns)
    
    def test_data_consistency(self):
        """Test data consistency through processing pipeline."""
        # Create simple, predictable data
        simple_data = pd.DataFrame({
            'vehicle_id': ['TEST'] * 5,
            'timestamp': pd.date_range(start='2024-01-01 10:00:00', 
                                      periods=5, freq='10S'),
            'speed_kmh': [0, 10, 20, 10, 0],
            'latitude': [40.0, 40.0001, 40.0002, 40.0003, 40.0004],
            'longitude': [-74.0, -74.0001, -74.0002, -74.0003, -74.0004],
            'engine_temperature_c': [90, 91, 92, 93, 94],
            'fuel_level_percent': [50, 49, 48, 47, 46]
        })
        
        # Process through pipeline
        cleaner = DataCleaner(simple_data)
        cleaned = cleaner.clean_all()
        
        vehicle_data = pd.DataFrame({
            'vehicle_id': ['TEST'],
            'make': ['TestMake'],
            'model': ['TestModel'],
            'year': [2024]
        })
        
        enricher = DataEnricher(cleaned, vehicle_data)
        enriched = enricher.enrich_all()
        
        # Check specific calculations
        # Distance calculation
        distances = enriched['distance_km'].dropna()
        self.assertEqual(len(distances), 4)  # 4 intervals between 5 points
        
        # All distances should be positive and similar
        self.assertTrue((distances > 0).all())
        self.assertAlmostEqual(distances.std(), 0, delta=0.01)  # Very small variance
        
        # Acceleration calculation
        # From 0 to 10 km/h in 10 seconds = 1 km/h/s
        self.assertAlmostEqual(enriched['acceleration_kmh_s'].iloc[1], 1.0, delta=0.1)
        
        # From 20 to 10 km/h in 10 seconds = -1 km/h/s
        self.assertAlmostEqual(enriched['acceleration_kmh_s'].iloc[3], -1.0, delta=0.1)
        
        # Vehicle info
        self.assertEqual(enriched['make'].iloc[0], 'TestMake')
        self.assertEqual(enriched['model'].iloc[0], 'TestModel')
        self.assertEqual(enriched['year'].iloc[0], 2024)
    
    def test_error_handling(self):
        """Test error handling in data processing pipeline."""
        # Test with completely invalid data
        invalid_data = pd.DataFrame({
            'wrong_column': [1, 2, 3],
            'another_wrong': ['a', 'b', 'c']
        })
        
        # Should handle gracefully
        cleaner = DataCleaner(invalid_data)
        cleaned = cleaner.clean_all()
        
        # Should still return a DataFrame (possibly empty or with minimal processing)
        self.assertIsInstance(cleaned, pd.DataFrame)
        
        # Test with mixed valid/invalid data
        mixed_data = pd.DataFrame({
            'vehicle_id': ['VH001', None, 'VH001'],
            'timestamp': ['2024-01-01 10:00:00', 'invalid', '2024-01-01 10:00:20'],
            'speed_kmh': [60, -10, 2000],  # Invalid values
            'rpm': [2000, 'not_a_number', 3000]
        })
        
        cleaner = DataCleaner(mixed_data)
        cleaned = cleaner.clean_all()
        report = cleaner.get_cleaning_report()
        
        # Should handle errors and produce a report
        self.assertIsInstance(report, dict)
        self.assertIn('rows_removed', report)
        self.assertIn('validation_violations', report)


def run_data_processing_tests():
    """Run all data processing tests and return results."""
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTest(unittest.makeSuite(TestDataCleaner))
    suite.addTest(unittest.makeSuite(TestDataEnricher))
    suite.addTest(unittest.makeSuite(TestTelemetrySimulator))
    suite.addTest(unittest.makeSuite(TestDataProcessingIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Return summary
    return {
        'tests_run': result.testsRun,
        'failures': len(result.failures),
        'errors': len(result.errors),
        'success': result.wasSuccessful()
    }


if __name__ == '__main__':
    # Run tests when script is executed directly
    test_results = run_data_processing_tests()
    
    print("\n" + "="*60)
    print("DATA PROCESSING TEST SUMMARY")
    print("="*60)
    print(f"Tests Run: {test_results['tests_run']}")
    print(f"Failures: {test_results['failures']}")
    print(f"Errors: {test_results['errors']}")
    print(f"Success: {test_results['success']}")
    print("="*60)
    
    # Exit with appropriate code
    sys.exit(0 if test_results['success'] else 1)
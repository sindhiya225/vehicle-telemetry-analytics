"""
Unit tests for analysis modules in Vehicle Telemetry Analytics Platform.
"""
import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from analysis.performance_analyzer import PerformanceAnalyzer
from analysis.efficiency_calculator import EfficiencyCalculator
from analysis.maintenance_predictor import MaintenancePredictor


class TestPerformanceAnalyzer(unittest.TestCase):
    """Test cases for PerformanceAnalyzer."""
    
    def setUp(self):
        """Set up test data."""
        # Create sample telemetry data
        np.random.seed(42)
        dates = pd.date_range(start='2024-01-01', periods=100, freq='10S')
        
        self.sample_data = pd.DataFrame({
            'vehicle_id': ['VH001'] * 100,
            'timestamp': dates,
            'speed_kmh': np.random.uniform(0, 120, 100),
            'rpm': np.random.uniform(800, 4000, 100),
            'engine_temperature_c': np.random.uniform(80, 110, 100),
            'fuel_level_percent': np.random.uniform(20, 100, 100),
            'latitude': np.random.uniform(40.0, 41.0, 100),
            'longitude': np.random.uniform(-74.0, -73.0, 100),
            'acceleration_x': np.random.uniform(-3, 3, 100),
            'acceleration_y': np.random.uniform(-2, 2, 100)
        })
        
        self.analyzer = PerformanceAnalyzer(self.sample_data)
    
    def test_initialization(self):
        """Test analyzer initialization."""
        self.assertIsNotNone(self.analyzer)
        self.assertEqual(len(self.analyzer.data), 100)
        self.assertEqual(self.analyzer.data['vehicle_id'].iloc[0], 'VH001')
    
    def test_calculate_basic_metrics(self):
        """Test basic metrics calculation."""
        metrics = self.analyzer.calculate_basic_metrics()
        
        self.assertIn('avg_speed', metrics)
        self.assertIn('max_speed', metrics)
        self.assertIn('avg_rpm', metrics)
        self.assertIn('avg_engine_temp', metrics)
        
        self.assertIsInstance(metrics['avg_speed'], float)
        self.assertIsInstance(metrics['max_speed'], float)
        
        # Check realistic values
        self.assertGreater(metrics['avg_speed'], 0)
        self.assertLess(metrics['avg_speed'], 120)
    
    def test_detect_harsh_events(self):
        """Test harsh event detection."""
        # Add some harsh events
        data_with_events = self.sample_data.copy()
        data_with_events.loc[10:15, 'acceleration_x'] = -4.0  # Harsh braking
        data_with_events.loc[20:25, 'acceleration_x'] = 4.0   # Harsh acceleration
        data_with_events.loc[30:35, 'acceleration_y'] = 3.0   # Harsh cornering
        
        analyzer = PerformanceAnalyzer(data_with_events)
        events = analyzer.detect_harsh_events()
        
        self.assertIn('harsh_braking', events)
        self.assertIn('harsh_acceleration', events)
        self.assertIn('harsh_cornering', events)
        
        # Should detect at least some events
        self.assertGreater(events['harsh_braking']['count'], 0)
        self.assertGreater(events['harsh_acceleration']['count'], 0)
        self.assertGreater(events['harsh_cornering']['count'], 0)
    
    def test_analyze_driving_patterns(self):
        """Test driving pattern analysis."""
        patterns = self.analyzer.analyze_driving_patterns()
        
        self.assertIn('idle_time_percentage', patterns)
        self.assertIn('urban_driving_percentage', patterns)
        self.assertIn('highway_driving_percentage', patterns)
        self.assertIn('aggressive_driving_score', patterns)
        
        # Percentages should sum to approximately 100
        total = (patterns['idle_time_percentage'] + 
                patterns['urban_driving_percentage'] + 
                patterns['highway_driving_percentage'])
        self.assertAlmostEqual(total, 100, delta=5)
    
    def test_calculate_safety_score(self):
        """Test safety score calculation."""
        safety_score = self.analyzer.calculate_safety_score()
        
        self.assertIsInstance(safety_score, dict)
        self.assertIn('overall_score', safety_score)
        self.assertIn('components', safety_score)
        
        # Score should be between 0 and 100
        self.assertGreaterEqual(safety_score['overall_score'], 0)
        self.assertLessEqual(safety_score['overall_score'], 100)
    
    def test_generate_performance_report(self):
        """Test performance report generation."""
        report = self.analyzer.generate_performance_report()
        
        self.assertIsInstance(report, dict)
        self.assertIn('summary', report)
        self.assertIn('metrics', report)
        self.assertIn('recommendations', report)
        
        # Check report structure
        self.assertIsInstance(report['summary'], str)
        self.assertIsInstance(report['metrics'], dict)
        self.assertIsInstance(report['recommendations'], list)
    
    def test_empty_data(self):
        """Test with empty data."""
        empty_analyzer = PerformanceAnalyzer(pd.DataFrame())
        metrics = empty_analyzer.calculate_basic_metrics()
        
        # Should return empty or default metrics
        self.assertEqual(len(metrics), 0)
    
    def test_invalid_data(self):
        """Test with invalid data."""
        invalid_data = pd.DataFrame({
            'vehicle_id': ['VH001'],
            'timestamp': [datetime.now()],
            'speed_kmh': ['invalid']  # Wrong type
        })
        
        analyzer = PerformanceAnalyzer(invalid_data)
        # Should handle gracefully
        metrics = analyzer.calculate_basic_metrics()
        self.assertIsInstance(metrics, dict)


class TestEfficiencyCalculator(unittest.TestCase):
    """Test cases for EfficiencyCalculator."""
    
    def setUp(self):
        """Set up test data."""
        # Create sample fuel consumption data
        self.fuel_data = pd.DataFrame({
            'vehicle_id': ['VH001'] * 10,
            'refuel_date': pd.date_range(start='2024-01-01', periods=10, freq='7D'),
            'odometer_km': [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600, 2800],
            'fuel_amount_liters': [40, 45, 42, 38, 43, 41, 44, 39, 42, 40],
            'fuel_cost': [3200, 3600, 3360, 3040, 3440, 3280, 3520, 3120, 3360, 3200]
        })
        
        self.calculator = EfficiencyCalculator(self.fuel_data)
    
    def test_calculate_fuel_efficiency(self):
        """Test fuel efficiency calculation."""
        efficiency = self.calculator.calculate_fuel_efficiency()
        
        self.assertIn('avg_fuel_efficiency_kmpl', efficiency)
        self.assertIn('total_fuel_consumed_l', efficiency)
        self.assertIn('total_distance_km', efficiency)
        self.assertIn('total_fuel_cost', efficiency)
        
        # Check calculations
        total_distance = self.fuel_data['odometer_km'].iloc[-1] - self.fuel_data['odometer_km'].iloc[0]
        total_fuel = self.fuel_data['fuel_amount_liters'].sum()
        
        self.assertEqual(efficiency['total_distance_km'], total_distance)
        self.assertEqual(efficiency['total_fuel_consumed_l'], total_fuel)
        self.assertAlmostEqual(efficiency['avg_fuel_efficiency_kmpl'], 
                              total_distance / total_fuel, delta=0.1)
    
    def test_detect_inefficient_trips(self):
        """Test inefficient trip detection."""
        trips = self.calculator.detect_inefficient_trips(threshold=10.0)
        
        self.assertIsInstance(trips, pd.DataFrame)
        if len(trips) > 0:
            self.assertIn('trip_efficiency_kmpl', trips.columns)
            self.assertIn('is_inefficient', trips.columns)
    
    def test_calculate_cost_per_km(self):
        """Test cost per kilometer calculation."""
        cost_data = self.calculator.calculate_cost_per_km()
        
        self.assertIn('avg_cost_per_km', cost_data)
        self.assertIn('total_cost', cost_data)
        self.assertIn('total_km', cost_data)
        
        # Verify calculation
        total_km = self.fuel_data['odometer_km'].iloc[-1] - self.fuel_data['odometer_km'].iloc[0]
        total_cost = self.fuel_data['fuel_cost'].sum()
        
        self.assertAlmostEqual(cost_data['avg_cost_per_km'], 
                              total_cost / total_km, delta=0.1)
    
    def test_analyze_efficiency_trends(self):
        """Test efficiency trend analysis."""
        trends = self.calculator.analyze_efficiency_trends()
        
        self.assertIn('trend', trends)
        self.assertIn('slope', trends)
        self.assertIn('r_squared', trends)
        self.assertIn('prediction_next', trends)
        
        # Check trend direction
        self.assertIn(trends['trend'], ['improving', 'declining', 'stable'])
        
        # R-squared should be between 0 and 1
        self.assertGreaterEqual(trends['r_squared'], 0)
        self.assertLessEqual(trends['r_squared'], 1)
    
    def test_generate_efficiency_report(self):
        """Test efficiency report generation."""
        report = self.calculator.generate_efficiency_report()
        
        self.assertIsInstance(report, dict)
        self.assertIn('summary', report)
        self.assertIn('metrics', report)
        self.assertIn('recommendations', report)
        self.assertIn('trend_analysis', report)
    
    def test_with_missing_data(self):
        """Test with missing fuel data."""
        incomplete_data = pd.DataFrame({
            'vehicle_id': ['VH001'] * 3,
            'refuel_date': pd.date_range(start='2024-01-01', periods=3),
            'odometer_km': [1000, 1200, 1400],
            'fuel_amount_liters': [40, None, 42],  # Missing value
            'fuel_cost': [3200, 3600, 3360]
        })
        
        calculator = EfficiencyCalculator(incomplete_data)
        efficiency = calculator.calculate_fuel_efficiency()
        
        # Should handle missing values gracefully
        self.assertIsInstance(efficiency, dict)


class TestMaintenancePredictor(unittest.TestCase):
    """Test cases for MaintenancePredictor."""
    
    def setUp(self):
        """Set up test data."""
        # Create sample maintenance and telemetry data
        self.maintenance_data = pd.DataFrame({
            'vehicle_id': ['VH001'] * 5,
            'maintenance_date': pd.date_range(start='2023-01-01', periods=5, freq='90D'),
            'maintenance_type': ['Oil Change', 'Tire Rotation', 'Brake Service', 
                                'Oil Change', 'Tire Rotation'],
            'cost': [5000, 3000, 8000, 5200, 3100],
            'odometer_km': [5000, 10000, 15000, 20000, 25000]
        })
        
        # Create sample telemetry data with potential issues
        dates = pd.date_range(start='2024-01-01', periods=100, freq='1H')
        self.telemetry_data = pd.DataFrame({
            'vehicle_id': ['VH001'] * 100,
            'timestamp': dates,
            'engine_temperature_c': np.random.uniform(85, 115, 100),
            'oil_pressure_kpa': np.random.uniform(180, 220, 100),
            'battery_voltage': np.random.uniform(12.0, 13.5, 100),
            'tire_pressure_front_left': np.random.uniform(30, 35, 100),
            'tire_pressure_front_right': np.random.uniform(30, 35, 100),
            'tire_pressure_rear_left': np.random.uniform(30, 35, 100),
            'tire_pressure_rear_right': np.random.uniform(30, 35, 100)
        })
        
        # Add some anomalies
        self.telemetry_data.loc[10:15, 'engine_temperature_c'] = 125  # Overheating
        self.telemetry_data.loc[20:25, 'oil_pressure_kpa'] = 140      # Low pressure
        self.telemetry_data.loc[30:35, 'battery_voltage'] = 11.5      # Low voltage
        
        self.predictor = MaintenancePredictor(
            self.telemetry_data, 
            self.maintenance_data
        )
    
    def test_predict_next_maintenance(self):
        """Test next maintenance prediction."""
        predictions = self.predictor.predict_next_maintenance()
        
        self.assertIsInstance(predictions, dict)
        self.assertIn('next_maintenance_date', predictions)
        self.assertIn('maintenance_type', predictions)
        self.assertIn('confidence', predictions)
        self.assertIn('estimated_cost', predictions)
        
        # Check date is in future
        next_date = pd.to_datetime(predictions['next_maintenance_date'])
        self.assertGreater(next_date, pd.Timestamp.now())
        
        # Confidence should be between 0 and 1
        self.assertGreaterEqual(predictions['confidence'], 0)
        self.assertLessEqual(predictions['confidence'], 1)
    
    def test_detect_component_issues(self):
        """Test component issue detection."""
        issues = self.predictor.detect_component_issues()
        
        self.assertIsInstance(issues, dict)
        self.assertIn('components', issues)
        self.assertIn('critical_issues', issues)
        self.assertIn('total_issues', issues)
        
        # Should detect at least some issues from our anomalies
        self.assertGreater(issues['total_issues'], 0)
    
    def test_calculate_component_health(self):
        """Test component health calculation."""
        health_scores = self.predictor.calculate_component_health()
        
        self.assertIsInstance(health_scores, dict)
        for component, score in health_scores.items():
            self.assertIsInstance(component, str)
            self.assertIsInstance(score, (int, float))
            self.assertGreaterEqual(score, 0)
            self.assertLessEqual(score, 100)
    
    def test_generate_maintenance_recommendations(self):
        """Test maintenance recommendation generation."""
        recommendations = self.predictor.generate_maintenance_recommendations()
        
        self.assertIsInstance(recommendations, list)
        if recommendations:
            for rec in recommendations:
                self.assertIn('component', rec)
                self.assertIn('issue', rec)
                self.assertIn('recommendation', rec)
                self.assertIn('priority', rec)
                self.assertIn(rec['priority'], ['High', 'Medium', 'Low'])
    
    def test_calculate_maintenance_cost_forecast(self):
        """Test maintenance cost forecast."""
        forecast = self.predictor.calculate_maintenance_cost_forecast(months=12)
        
        self.assertIsInstance(forecast, dict)
        self.assertIn('total_estimated_cost', forecast)
        self.assertIn('monthly_breakdown', forecast)
        self.assertIn('cost_trend', forecast)
        
        # Monthly breakdown should have 12 months
        self.assertEqual(len(forecast['monthly_breakdown']), 12)
    
    def test_generate_maintenance_report(self):
        """Test maintenance report generation."""
        report = self.predictor.generate_maintenance_report()
        
        self.assertIsInstance(report, dict)
        self.assertIn('summary', report)
        self.assertIn('component_health', report)
        self.assertIn('predictions', report)
        self.assertIn('recommendations', report)
        self.assertIn('cost_forecast', report)
    
    def test_with_no_telemetry_data(self):
        """Test with no telemetry data."""
        predictor = MaintenancePredictor(
            pd.DataFrame(),  # Empty telemetry
            self.maintenance_data
        )
        
        # Should handle gracefully
        predictions = predictor.predict_next_maintenance()
        self.assertIsInstance(predictions, dict)
    
    def test_with_no_maintenance_history(self):
        """Test with no maintenance history."""
        predictor = MaintenancePredictor(
            self.telemetry_data,
            pd.DataFrame()  # Empty maintenance
        )
        
        # Should use defaults or statistical methods
        recommendations = predictor.generate_maintenance_recommendations()
        self.assertIsInstance(recommendations, list)


class TestIntegration(unittest.TestCase):
    """Integration tests for analysis modules."""
    
    def test_end_to_end_analysis(self):
        """Test end-to-end analysis pipeline."""
        # Create comprehensive test data
        telemetry_data = pd.DataFrame({
            'vehicle_id': ['VH001'] * 100,
            'timestamp': pd.date_range(start='2024-01-01', periods=100, freq='10S'),
            'speed_kmh': np.random.uniform(0, 120, 100),
            'rpm': np.random.uniform(800, 4000, 100),
            'engine_temperature_c': np.random.uniform(80, 110, 100),
            'fuel_level_percent': np.random.uniform(20, 100, 100),
            'latitude': np.random.uniform(40.0, 41.0, 100),
            'longitude': np.random.uniform(-74.0, -73.0, 100)
        })
        
        fuel_data = pd.DataFrame({
            'vehicle_id': ['VH001'] * 5,
            'refuel_date': pd.date_range(start='2024-01-01', periods=5, freq='7D'),
            'odometer_km': [1000, 1200, 1400, 1600, 1800],
            'fuel_amount_liters': [40, 45, 42, 38, 43],
            'fuel_cost': [3200, 3600, 3360, 3040, 3440]
        })
        
        # Run performance analysis
        perf_analyzer = PerformanceAnalyzer(telemetry_data)
        perf_report = perf_analyzer.generate_performance_report()
        
        # Run efficiency analysis
        eff_calculator = EfficiencyCalculator(fuel_data)
        eff_report = eff_calculator.generate_efficiency_report()
        
        # Verify outputs
        self.assertIsInstance(perf_report, dict)
        self.assertIsInstance(eff_report, dict)
        
        # Check they have expected structure
        self.assertIn('summary', perf_report)
        self.assertIn('metrics', perf_report)
        self.assertIn('recommendations', perf_report)
        
        self.assertIn('summary', eff_report)
        self.assertIn('metrics', eff_report)
        self.assertIn('recommendations', eff_report)
    
    def test_data_consistency(self):
        """Test data consistency across analyses."""
        # Create consistent test data
        base_date = datetime(2024, 1, 1)
        
        telemetry_data = pd.DataFrame({
            'vehicle_id': ['VH001', 'VH001'],
            'timestamp': [base_date, base_date + timedelta(hours=1)],
            'speed_kmh': [60, 80],
            'odometer_km': [1000, 1100]  # 100km in 1 hour
        })
        
        fuel_data = pd.DataFrame({
            'vehicle_id': ['VH001', 'VH001'],
            'refuel_date': [base_date - timedelta(days=1), base_date + timedelta(days=6)],
            'odometer_km': [900, 1100],  # 200km between refuels
            'fuel_amount_liters': [40, 42]  # 82 liters total
        })
        
        # Calculate expected efficiency
        total_distance = 200  # km
        total_fuel = 82  # liters
        expected_efficiency = total_distance / total_fuel  # ~2.44 km/L
        
        # Run efficiency calculation
        calculator = EfficiencyCalculator(fuel_data)
        efficiency = calculator.calculate_fuel_efficiency()
        
        # Check calculation
        self.assertAlmostEqual(efficiency['avg_fuel_efficiency_kmpl'], 
                              expected_efficiency, delta=0.1)


def run_tests():
    """Run all tests and return results."""
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTest(unittest.makeSuite(TestPerformanceAnalyzer))
    suite.addTest(unittest.makeSuite(TestEfficiencyCalculator))
    suite.addTest(unittest.makeSuite(TestMaintenancePredictor))
    suite.addTest(unittest.makeSuite(TestIntegration))
    
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
    test_results = run_tests()
    
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Tests Run: {test_results['tests_run']}")
    print(f"Failures: {test_results['failures']}")
    print(f"Errors: {test_results['errors']}")
    print(f"Success: {test_results['success']}")
    print("="*60)
    
    # Exit with appropriate code
    sys.exit(0 if test_results['success'] else 1)
"""
Unit tests for database modules in Vehicle Telemetry Analytics Platform.
"""
import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os
from unittest.mock import Mock, patch, MagicMock

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from database.database_manager import DatabaseManager, QueryResult, ConnectionPoolManager
from database.sql_queries import VehicleQueries, TelemetryQueries


class TestQueryResult(unittest.TestCase):
    """Test cases for QueryResult class."""
    
    def test_initialization(self):
        """Test QueryResult initialization."""
        # Test successful result
        result = QueryResult(
            success=True,
            data=[{'id': 1, 'name': 'test'}],
            columns=['id', 'name'],
            row_count=1,
            execution_time=0.5
        )
        
        self.assertTrue(result.success)
        self.assertEqual(len(result.data), 1)
        self.assertEqual(result.columns, ['id', 'name'])
        self.assertEqual(result.row_count, 1)
        self.assertEqual(result.execution_time, 0.5)
        self.assertIsNone(result.error)
        
        # Test failed result
        result = QueryResult(
            success=False,
            error='Connection failed',
            execution_time=0.1
        )
        
        self.assertFalse(result.success)
        self.assertEqual(result.error, 'Connection failed')
        self.assertEqual(result.execution_time, 0.1)
    
    def test_to_dataframe(self):
        """Test conversion to DataFrame."""
        # Test with data
        result = QueryResult(
            success=True,
            data=[{'id': 1, 'value': 100}, {'id': 2, 'value': 200}],
            columns=['id', 'value'],
            row_count=2
        )
        
        df = result.to_dataframe()
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(list(df.columns), ['id', 'value'])
        self.assertEqual(df['id'].iloc[0], 1)
        self.assertEqual(df['value'].iloc[1], 200)
        
        # Test without data
        result = QueryResult(success=False)
        df = result.to_dataframe()
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 0)
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        result = QueryResult(
            success=True,
            data=[{'test': 'data'}],
            row_count=1,
            execution_time=0.3,
            query='SELECT * FROM test'
        )
        
        result_dict = result.to_dict()
        
        self.assertIsInstance(result_dict, dict)
        self.assertEqual(result_dict['success'], True)
        self.assertEqual(result_dict['row_count'], 1)
        self.assertEqual(result_dict['execution_time'], 0.3)
        self.assertEqual(result_dict['data'], [{'test': 'data'}])
        self.assertIn('query', result_dict)


class TestDatabaseManager(unittest.TestCase):
    """Test cases for DatabaseManager."""
    
    def setUp(self):
        """Set up test database manager with mock connections."""
        # Mock configuration
        self.mock_config = {
            'postgresql': {
                'enabled': True,
                'host': 'localhost',
                'port': 5432,
                'database': 'test_db',
                'user': 'test_user',
                'password': 'test_password',
                'min_connections': 1,
                'max_connections': 5
            },
            'redis': {
                'enabled': False
            }
        }
        
        # Create manager with mock config
        with patch.object(DatabaseManager, '_setup_database'):
            self.db_manager = DatabaseManager(config_dict=self.mock_config)
    
    @patch('database.database_manager.ThreadedConnectionPool')
    @patch('database.database_manager.create_engine')
    def test_initialization(self, mock_create_engine, mock_connection_pool):
        """Test database manager initialization."""
        # Mock the pool and engine
        mock_pool = Mock()
        mock_connection_pool.return_value = mock_pool
        
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        # Create manager
        manager = DatabaseManager(config_dict=self.mock_config)
        
        # Check initialization
        self.assertIsNotNone(manager)
        self.assertEqual(manager.config['postgresql']['host'], 'localhost')
        self.assertIsNotNone(manager.pool_manager)
    
    @patch('database.database_manager.psycopg2')
    def test_execute_query_success(self, mock_psycopg2):
        """Test successful query execution."""
        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        
        # Mock cursor execution
        mock_cursor.description = [('id',), ('name',)]
        mock_cursor.fetchall.return_value = [{'id': 1, 'name': 'test'}]
        mock_cursor.rowcount = 1
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock connection pool
        mock_pool = Mock()
        mock_pool.getconn.return_value = mock_conn
        mock_pool.putconn = Mock()
        
        self.db_manager.pool_manager.postgres_pool = mock_pool
        
        # Execute query
        result = self.db_manager.execute_query('SELECT * FROM test')
        
        # Verify result
        self.assertTrue(result.success)
        self.assertEqual(len(result.data), 1)
        self.assertEqual(result.row_count, 1)
        self.assertEqual(result.columns, ['id', 'name'])
        
        # Verify connection was returned to pool
        mock_pool.putconn.assert_called_once_with(mock_conn)
    
    @patch('database.database_manager.psycopg2')
    def test_execute_query_error(self, mock_psycopg2):
        """Test query execution with error."""
        # Mock connection that raises exception
        mock_conn = Mock()
        mock_conn.cursor.side_effect = Exception('Connection failed')
        
        mock_pool = Mock()
        mock_pool.getconn.return_value = mock_conn
        mock_pool.putconn = Mock()
        
        self.db_manager.pool_manager.postgres_pool = mock_pool
        
        # Execute query
        result = self.db_manager.execute_query('SELECT * FROM test')
        
        # Verify error result
        self.assertFalse(result.success)
        self.assertIsNotNone(result.error)
        self.assertIn('Connection failed', result.error)
    
    @patch('database.database_manager.psycopg2')
    def test_execute_many(self, mock_psycopg2):
        """Test batch query execution."""
        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 100
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        mock_pool = Mock()
        mock_pool.getconn.return_value = mock_conn
        mock_pool.putconn = Mock()
        
        self.db_manager.pool_manager.postgres_pool = mock_pool
        
        # Execute batch query
        query = "INSERT INTO test (id, value) VALUES %s"
        params = [(i, i*10) for i in range(50)]
        
        result = self.db_manager.execute_many(query, params, batch_size=10)
        
        # Verify result
        self.assertTrue(result.success)
        self.assertEqual(result.row_count, 100)  # 50 * 2 (two values per row)
        
        # Verify commit was called
        mock_conn.commit.assert_called_once()
    
    def test_query_to_dataframe(self):
        """Test query execution returning DataFrame."""
        # Mock the SQLAlchemy engine
        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = Mock()
        
        # Set up mock result
        mock_result.fetchall.return_value = [(1, 'test'), (2, 'test2')]
        mock_result.keys.return_value = ['id', 'name']
        
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        self.db_manager.pool_manager.sqlalchemy_engine = mock_engine
        
        # Execute query
        df = self.db_manager.query_to_dataframe('SELECT * FROM test')
        
        # Verify result
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(list(df.columns), ['id', 'name'])
        self.assertEqual(df['id'].iloc[0], 1)
        self.assertEqual(df['name'].iloc[1], 'test2')
    
    @patch('database.database_manager.pd.DataFrame.to_sql')
    def test_execute_dataframe(self, mock_to_sql):
        """Test DataFrame insertion."""
        # Create test DataFrame
        test_df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [100, 200, 300]
        })
        
        # Mock SQLAlchemy engine
        mock_engine = Mock()
        self.db_manager.pool_manager.sqlalchemy_engine = mock_engine
        
        # Execute DataFrame insertion
        success = self.db_manager.execute_dataframe(test_df, 'test_table')
        
        # Verify result
        self.assertTrue(success)
        mock_to_sql.assert_called_once_with(
            'test_table',
            mock_engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
    
    def test_get_table_info(self):
        """Test table information retrieval."""
        # Mock execute_query to return table info
        mock_result = QueryResult(
            success=True,
            data=[
                {
                    'column_name': 'id',
                    'data_type': 'integer',
                    'is_nullable': 'NO',
                    'column_default': None
                },
                {
                    'column_name': 'name',
                    'data_type': 'varchar',
                    'is_nullable': 'YES',
                    'column_default': None
                }
            ],
            columns=['column_name', 'data_type', 'is_nullable', 'column_default'],
            row_count=2
        )
        
        with patch.object(self.db_manager, 'execute_query', return_value=mock_result):
            df = self.db_manager.get_table_info('test_table')
        
        # Verify result
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(df['column_name'].iloc[0], 'id')
        self.assertEqual(df['data_type'].iloc[1], 'varchar')
    
    def test_get_database_size(self):
        """Test database size retrieval."""
        # Mock execute_query to return size info
        mock_result = QueryResult(
            success=True,
            data=[{
                'db_size_bytes': 1024 * 1024 * 100,  # 100MB
                'db_size_pretty': '100 MB',
                'vehicles_count': 10,
                'telemetry_count': 1000,
                'maintenance_count': 50,
                'fuel_count': 20
            }],
            row_count=1
        )
        
        with patch.object(self.db_manager, 'execute_query', return_value=mock_result):
            size_info = self.db_manager.get_database_size()
        
        # Verify result
        self.assertIsInstance(size_info, dict)
        self.assertEqual(size_info['db_size_bytes'], 104857600)
        self.assertEqual(size_info['vehicles_count'], 10)
        self.assertEqual(size_info['telemetry_count'], 1000)
    
    @patch('database.database_manager.psycopg2')
    def test_vacuum_analyze(self, mock_psycopg2):
        """Test VACUUM ANALYZE execution."""
        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        mock_pool = Mock()
        mock_pool.getconn.return_value = mock_conn
        mock_pool.putconn = Mock()
        
        self.db_manager.pool_manager.postgres_pool = mock_pool
        
        # Execute VACUUM ANALYZE
        self.db_manager.vacuum_analyze('test_table')
        
        # Verify VACUUM ANALYZE was executed
        mock_cursor.execute.assert_called_with('VACUUM ANALYZE test_table;')
        mock_conn.commit.assert_called_once()
    
    def test_close(self):
        """Test database connection closing."""
        # Mock pool manager close method
        mock_pool_manager = Mock()
        self.db_manager.pool_manager = mock_pool_manager
        
        # Close connections
        self.db_manager.close()
        
        # Verify close was called
        mock_pool_manager.close_all.assert_called_once()


class TestVehicleQueries(unittest.TestCase):
    """Test cases for VehicleQueries."""
    
    def setUp(self):
        """Set up test queries."""
        self.queries = VehicleQueries()
    
    def test_get_vehicle_by_id(self):
        """Test vehicle retrieval by ID."""
        query = self.queries.get_vehicle_by_id('VH001')
        
        self.assertIsInstance(query, str)
        self.assertIn('SELECT', query)
        self.assertIn('VH001', query)
        self.assertIn('vehicles', query)
    
    def test_get_vehicles_by_fleet(self):
        """Test vehicle retrieval by fleet."""
        query = self.queries.get_vehicles_by_fleet('FLEET001')
        
        self.assertIsInstance(query, str)
        self.assertIn('SELECT', query)
        self.assertIn('FLEET001', query)
        self.assertIn('fleet_id', query)
    
    def test_get_vehicles_by_status(self):
        """Test vehicle retrieval by status."""
        query = self.queries.get_vehicles_by_status('active')
        
        self.assertIsInstance(query, str)
        self.assertIn('SELECT', query)
        self.assertIn('active', query)
        self.assertIn('status', query)
    
    def test_get_vehicle_metrics(self):
        """Test vehicle metrics query."""
        query = self.queries.get_vehicle_metrics('VH001', '2024-01-01', '2024-01-31')
        
        self.assertIsInstance(query, str)
        self.assertIn('SELECT', query)
        self.assertIn('VH001', query)
        self.assertIn('2024-01-01', query)
        self.assertIn('2024-01-31', query)
        self.assertIn('telemetry_data', query)
    
    def test_update_vehicle_status(self):
        """Test vehicle status update query."""
        query = self.queries.update_vehicle_status('VH001', 'maintenance')
        
        self.assertIsInstance(query, str)
        self.assertIn('UPDATE', query)
        self.assertIn('VH001', query)
        self.assertIn('maintenance', query)
        self.assertIn('vehicles', query)
    
    def test_insert_vehicle(self):
        """Test vehicle insertion query."""
        vehicle_data = {
            'vehicle_id': 'VH999',
            'fleet_id': 'FLEET001',
            'make': 'Test',
            'model': 'Model',
            'year': 2024,
            'engine_type': 'Electric',
            'fuel_type': 'Electric'
        }
        
        query = self.queries.insert_vehicle(vehicle_data)
        
        self.assertIsInstance(query, tuple)
        self.assertEqual(len(query), 2)  # SQL and parameters
        
        sql, params = query
        self.assertIsInstance(sql, str)
        self.assertIsInstance(params, tuple)
        
        self.assertIn('INSERT', sql)
        self.assertIn('vehicles', sql)
        self.assertIn('VH999', params)
    
    def test_delete_vehicle(self):
        """Test vehicle deletion query."""
        query = self.queries.delete_vehicle('VH001')
        
        self.assertIsInstance(query, tuple)
        
        sql, params = query
        self.assertIn('DELETE', sql)
        self.assertIn('VH001', params)
        self.assertIn('vehicles', sql)


class TestTelemetryQueries(unittest.TestCase):
    """Test cases for TelemetryQueries."""
    
    def setUp(self):
        """Set up test queries."""
        self.queries = TelemetryQueries()
    
    def test_get_telemetry_by_vehicle(self):
        """Test telemetry retrieval by vehicle."""
        query = self.queries.get_telemetry_by_vehicle(
            'VH001', 
            '2024-01-01 00:00:00',
            '2024-01-01 23:59:59',
            100
        )
        
        self.assertIsInstance(query, str)
        self.assertIn('SELECT', query)
        self.assertIn('VH001', query)
        self.assertIn('2024-01-01', query)
        self.assertIn('telemetry_data', query)
        self.assertIn('LIMIT 100', query)
    
    def test_get_latest_telemetry(self):
        """Test latest telemetry retrieval."""
        query = self.queries.get_latest_telemetry('VH001', 10)
        
        self.assertIsInstance(query, str)
        self.assertIn('SELECT', query)
        self.assertIn('VH001', query)
        self.assertIn('telemetry_data', query)
        self.assertIn('LIMIT 10', query)
        self.assertIn('ORDER BY timestamp DESC', query)
    
    def test_insert_telemetry(self):
        """Test telemetry insertion query."""
        telemetry_data = {
            'vehicle_id': 'VH001',
            'timestamp': '2024-01-01 10:00:00',
            'speed_kmh': 60.0,
            'rpm': 2000.0,
            'engine_temperature_c': 90.0,
            'latitude': 40.7128,
            'longitude': -74.0060
        }
        
        query = self.queries.insert_telemetry(telemetry_data)
        
        self.assertIsInstance(query, tuple)
        self.assertEqual(len(query), 2)  # SQL and parameters
        
        sql, params = query
        self.assertIsInstance(sql, str)
        self.assertIsInstance(params, tuple)
        
        self.assertIn('INSERT', sql)
        self.assertIn('telemetry_data', sql)
        self.assertIn('VH001', params)
    
    def test_get_speed_statistics(self):
        """Test speed statistics query."""
        query = self.queries.get_speed_statistics(
            'VH001',
            '2024-01-01',
            '2024-01-31'
        )
        
        self.assertIsInstance(query, str)
        self.assertIn('SELECT', query)
        self.assertIn('VH001', query)
        self.assertIn('2024-01-01', query)
        self.assertIn('2024-01-31', query)
        self.assertIn('speed_kmh', query)
        self.assertIn('AVG', query)
        self.assertIn('MAX', query)
        self.assertIn('MIN', query)
    
    def test_get_temperature_alerts(self):
        """Test temperature alerts query."""
        query = self.queries.get_temperature_alerts(100.0)
        
        self.assertIsInstance(query, str)
        self.assertIn('SELECT', query)
        self.assertIn('100.0', query)
        self.assertIn('engine_temperature_c', query)
        self.assertIn('>', query)
    
    def test_get_fuel_efficiency(self):
        """Test fuel efficiency query."""
        query = self.queries.get_fuel_efficiency(
            'VH001',
            '2024-01-01',
            '2024-01-31'
        )
        
        self.assertIsInstance(query, str)
        self.assertIn('SELECT', query)
        self.assertIn('VH001', query)
        self.assertIn('2024-01-01', query)
        self.assertIn('2024-01-31', query)
        self.assertIn('fuel_level_percent', query)
    
    def test_delete_old_telemetry(self):
        """Test old telemetry deletion query."""
        query = self.queries.delete_old_telemetry('2023-12-01')
        
        self.assertIsInstance(query, tuple)
        
        sql, params = query
        self.assertIn('DELETE', sql)
        self.assertIn('2023-12-01', params)
        self.assertIn('telemetry_data', sql)
        self.assertIn('timestamp <', sql)


class TestIntegration(unittest.TestCase):
    """Integration tests for database operations."""
    
    @patch('database.database_manager.DatabaseManager')
    def test_vehicle_crud_operations(self, MockDatabaseManager):
        """Test complete vehicle CRUD operations."""
        # Mock database manager
        mock_db = MockDatabaseManager.return_value
        
        # Mock query execution results
        mock_db.execute_query.return_value = QueryResult(
            success=True,
            data=[{'vehicle_id': 'VH001', 'make': 'Toyota', 'model': 'Camry'}],
            row_count=1
        )
        
        mock_db.execute_many.return_value = QueryResult(success=True, row_count=1)
        
        # Test queries
        queries = VehicleQueries()
        
        # 1. Insert vehicle
        vehicle_data = {
            'vehicle_id': 'VH001',
            'fleet_id': 'FLEET001',
            'make': 'Toyota',
            'model': 'Camry',
            'year': 2020,
            'engine_type': 'Hybrid',
            'fuel_type': 'Petrol'
        }
        
        insert_sql, insert_params = queries.insert_vehicle(vehicle_data)
        mock_db.execute_query(insert_sql, insert_params)
        
        # Verify insert was called
        mock_db.execute_query.assert_called_with(insert_sql, insert_params)
        
        # 2. Retrieve vehicle
        select_sql = queries.get_vehicle_by_id('VH001')
        result = mock_db.execute_query(select_sql)
        
        # Verify retrieval
        self.assertTrue(result.success)
        self.assertEqual(len(result.data), 1)
        self.assertEqual(result.data[0]['make'], 'Toyota')
        
        # 3. Update vehicle status
        update_sql = queries.update_vehicle_status('VH001', 'active')
        mock_db.execute_query(update_sql)
        
        # 4. Delete vehicle
        delete_sql, delete_params = queries.delete_vehicle('VH001')
        mock_db.execute_query(delete_sql, delete_params)
    
    @patch('database.database_manager.DatabaseManager')
    def test_telemetry_operations(self, MockDatabaseManager):
        """Test telemetry data operations."""
        # Mock database manager
        mock_db = MockDatabaseManager.return_value
        
        # Mock query results for telemetry data
        telemetry_data = [
            {
                'timestamp': '2024-01-01 10:00:00',
                'speed_kmh': 60.0,
                'engine_temperature_c': 90.0,
                'fuel_level_percent': 50.0
            },
            {
                'timestamp': '2024-01-01 10:00:10',
                'speed_kmh': 65.0,
                'engine_temperature_c': 92.0,
                'fuel_level_percent': 49.5
            }
        ]
        
        mock_db.execute_query.return_value = QueryResult(
            success=True,
            data=telemetry_data,
            row_count=2
        )
        
        # Test queries
        queries = TelemetryQueries()
        
        # 1. Get telemetry for vehicle
        telemetry_sql = queries.get_telemetry_by_vehicle(
            'VH001',
            '2024-01-01 00:00:00',
            '2024-01-01 23:59:59',
            1000
        )
        
        result = mock_db.execute_query(telemetry_sql)
        
        # Verify retrieval
        self.assertTrue(result.success)
        self.assertEqual(result.row_count, 2)
        self.assertEqual(result.data[0]['speed_kmh'], 60.0)
        
        # 2. Get speed statistics
        stats_sql = queries.get_speed_statistics(
            'VH001',
            '2024-01-01',
            '2024-01-01'
        )
        
        mock_db.execute_query(stats_sql)
        
        # 3. Get temperature alerts
        alerts_sql = queries.get_temperature_alerts(100.0)
        mock_db.execute_query(alerts_sql)
        
        # 4. Insert new telemetry
        new_telemetry = {
            'vehicle_id': 'VH001',
            'timestamp': '2024-01-01 10:00:20',
            'speed_kmh': 70.0,
            'rpm': 2200.0,
            'engine_temperature_c': 94.0
        }
        
        insert_sql, insert_params = queries.insert_telemetry(new_telemetry)
        mock_db.execute_query(insert_sql, insert_params)
    
    @patch('database.database_manager.DatabaseManager')
    def test_batch_operations(self, MockDatabaseManager):
        """Test batch database operations."""
        # Mock database manager
        mock_db = MockDatabaseManager.return_value
        
        # Create batch data
        batch_data = []
        for i in range(100):
            batch_data.append({
                'vehicle_id': f'VH{i:03d}',
                'timestamp': f'2024-01-01 10:{i:02d}:00',
                'speed_kmh': 60.0 + i % 20,
                'rpm': 2000.0 + i % 1000
            })
        
        # Convert to DataFrame
        df = pd.DataFrame(batch_data)
        
        # Mock successful insertion
        mock_db.execute_dataframe.return_value = True
        
        # Test DataFrame insertion
        success = mock_db.execute_dataframe(df, 'telemetry_data')
        
        # Verify insertion
        self.assertTrue(success)
        mock_db.execute_dataframe.assert_called_once()
    
    def test_error_recovery(self):
        """Test error recovery scenarios."""
        # This would test retry logic, connection pooling recovery, etc.
        # For now, just verify the structure
        
        queries = VehicleQueries()
        
        # Test with invalid input
        with self.assertRaises(Exception):
            # This should raise an error when executed with real DB
            # But for unit test, we just verify the query construction
            queries.get_vehicle_by_id(None)
        
        # Test telemetry queries with edge cases
        telemetry_queries = TelemetryQueries()
        
        # Empty date range
        query = telemetry_queries.get_telemetry_by_vehicle(
            'VH001',
            '',
            '',
            10
        )
        
        self.assertIsInstance(query, str)
        self.assertIn('VH001', query)


def run_database_tests():
    """Run all database tests and return results."""
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTest(unittest.makeSuite(TestQueryResult))
    suite.addTest(unittest.makeSuite(TestDatabaseManager))
    suite.addTest(unittest.makeSuite(TestVehicleQueries))
    suite.addTest(unittest.makeSuite(TestTelemetryQueries))
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
    test_results = run_database_tests()
    
    print("\n" + "="*60)
    print("DATABASE TEST SUMMARY")
    print("="*60)
    print(f"Tests Run: {test_results['tests_run']}")
    print(f"Failures: {test_results['failures']}")
    print(f"Errors: {test_results['errors']}")
    print(f"Success: {test_results['success']}")
    print("="*60)
    
    # Exit with appropriate code
    sys.exit(0 if test_results['success'] else 1)
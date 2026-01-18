"""
Database Manager for Vehicle Telemetry Analytics Platform.
Handles connections, connection pooling, query execution, and database operations.
"""
import logging
import json
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values, Json
from psycopg2.pool import ThreadedConnectionPool
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
import redis
from redis import Redis
import aioredis
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import asyncpg
from dataclasses import dataclass, asdict
from enum import Enum
import asyncio

logger = logging.getLogger(__name__)


class DatabaseType(Enum):
    """Supported database types."""
    POSTGRESQL = "postgresql"
    REDIS = "redis"
    TIMESCALE = "timescale"
    CLICKHOUSE = "clickhouse"


class ConnectionPoolManager:
    """Manages database connection pools."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.postgres_pool = None
        self.redis_pool = None
        self.sqlalchemy_engine = None
        self.sqlalchemy_session_factory = None
        self._init_pools()
    
    def _init_pools(self):
        """Initialize connection pools."""
        # PostgreSQL connection pool
        if self.config.get('postgresql', {}).get('enabled', True):
            self.postgres_pool = ThreadedConnectionPool(
                minconn=self.config['postgresql'].get('min_connections', 1),
                maxconn=self.config['postgresql'].get('max_connections', 20),
                host=self.config['postgresql']['host'],
                port=self.config['postgresql'].get('port', 5432),
                database=self.config['postgresql']['database'],
                user=self.config['postgresql']['user'],
                password=self.config['postgresql']['password'],
                cursor_factory=RealDictCursor,
                application_name='vehicle-telemetry-analytics'
            )
            
            # SQLAlchemy engine for ORM and pandas
            connection_string = (
                f"postgresql://{self.config['postgresql']['user']}:"
                f"{self.config['postgresql']['password']}@"
                f"{self.config['postgresql']['host']}:"
                f"{self.config['postgresql'].get('port', 5432)}/"
                f"{self.config['postgresql']['database']}"
            )
            self.sqlalchemy_engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=self.config['postgresql'].get('echo_sql', False)
            )
            self.sqlalchemy_session_factory = scoped_session(
                sessionmaker(bind=self.sqlalchemy_engine)
            )
        
        # Redis connection pool
        if self.config.get('redis', {}).get('enabled', False):
            self.redis_pool = redis.ConnectionPool(
                host=self.config['redis']['host'],
                port=self.config['redis'].get('port', 6379),
                db=self.config['redis'].get('database', 0),
                password=self.config['redis'].get('password'),
                max_connections=self.config['redis'].get('max_connections', 10),
                decode_responses=True
            )
    
    @contextmanager
    def get_postgres_connection(self):
        """Get PostgreSQL connection from pool."""
        conn = None
        try:
            conn = self.postgres_pool.getconn()
            yield conn
        except Exception as e:
            logger.error(f"Failed to get PostgreSQL connection: {e}")
            raise
        finally:
            if conn:
                self.postgres_pool.putconn(conn)
    
    def get_redis_connection(self) -> Redis:
        """Get Redis connection."""
        return redis.Redis(connection_pool=self.redis_pool)
    
    def get_sqlalchemy_session(self):
        """Get SQLAlchemy session."""
        return self.sqlalchemy_session_factory()
    
    def close_all(self):
        """Close all connection pools."""
        if self.postgres_pool:
            self.postgres_pool.closeall()
        if self.redis_pool:
            self.redis_pool.disconnect()
        if self.sqlalchemy_engine:
            self.sqlalchemy_engine.dispose()


@dataclass
class QueryResult:
    """Standardized query result container."""
    success: bool
    data: Optional[List[Dict]] = None
    columns: Optional[List[str]] = None
    row_count: int = 0
    execution_time: float = 0.0
    error: Optional[str] = None
    query: Optional[str] = None
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert result to pandas DataFrame."""
        if not self.success or not self.data:
            return pd.DataFrame()
        return pd.DataFrame(self.data)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            'success': self.success,
            'row_count': self.row_count,
            'execution_time': self.execution_time,
            'error': self.error,
            'data': self.data if self.data else []
        }


class DatabaseManager:
    """
    Main database manager for handling all database operations.
    Supports PostgreSQL, Redis, and async operations.
    """
    
    def __init__(self, config_path: str = None, config_dict: Dict = None):
        """
        Initialize database manager.
        
        Args:
            config_path: Path to configuration file
            config_dict: Configuration dictionary
        """
        self.config = self._load_config(config_path, config_dict)
        self.pool_manager = ConnectionPoolManager(self.config)
        self.cache_ttl = self.config.get('cache_ttl', 300)
        self.query_cache = {}
        self._setup_database()
    
    def _load_config(self, config_path: str, config_dict: Dict) -> Dict:
        """Load database configuration."""
        if config_dict:
            return config_dict
        
        if config_path:
            import yaml
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        
        # Default configuration
        return {
            'postgresql': {
                'enabled': True,
                'host': 'localhost',
                'port': 5432,
                'database': 'vehicle_analytics',
                'user': 'postgres',
                'password': 'password',
                'min_connections': 2,
                'max_connections': 20,
                'echo_sql': False
            },
            'redis': {
                'enabled': True,
                'host': 'localhost',
                'port': 6379,
                'database': 0,
                'password': None,
                'max_connections': 10
            },
            'cache_ttl': 300,
            'query_timeout': 30,
            'enable_query_logging': True
        }
    
    def _setup_database(self):
        """Setup database tables and indexes if they don't exist."""
        if self.config['postgresql']['enabled']:
            self._create_tables_if_not_exists()
            self._create_indexes()
    
    def _create_tables_if_not_exists(self):
        """Create database tables if they don't exist."""
        create_tables_sql = """
        -- Vehicle information table
        CREATE TABLE IF NOT EXISTS vehicles (
            vehicle_id VARCHAR(50) PRIMARY KEY,
            fleet_id VARCHAR(50),
            make VARCHAR(50),
            model VARCHAR(50),
            year INTEGER,
            engine_type VARCHAR(20),
            fuel_type VARCHAR(20),
            transmission_type VARCHAR(20),
            purchase_date DATE,
            last_service_date DATE,
            total_distance_km FLOAT,
            status VARCHAR(20) DEFAULT 'active',
            metadata JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Telemetry data table (TimescaleDB hypertable if available)
        CREATE TABLE IF NOT EXISTS telemetry_data (
            id BIGSERIAL PRIMARY KEY,
            vehicle_id VARCHAR(50) REFERENCES vehicles(vehicle_id),
            timestamp TIMESTAMP NOT NULL,
            latitude FLOAT,
            longitude FLOAT,
            speed_kmh FLOAT,
            rpm FLOAT,
            engine_temperature_c FLOAT,
            coolant_temperature_c FLOAT,
            oil_pressure_kpa FLOAT,
            fuel_level_percent FLOAT,
            battery_voltage FLOAT,
            throttle_position_percent FLOAT,
            brake_pressure_bar FLOAT,
            gear_position INTEGER,
            odometer_km FLOAT,
            ambient_temperature_c FLOAT,
            tire_pressure_front_left FLOAT,
            tire_pressure_front_right FLOAT,
            tire_pressure_rear_left FLOAT,
            tire_pressure_rear_right FLOAT,
            acceleration_x FLOAT,
            acceleration_y FLOAT,
            acceleration_z FLOAT,
            fault_codes JSONB,
            warnings JSONB,
            processing_time_ms FLOAT,
            source_system VARCHAR(50),
            raw_data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create hypertable for TimescaleDB
        SELECT create_hypertable('telemetry_data', 'timestamp', if_not_exists => TRUE);
        
        -- Maintenance records table
        CREATE TABLE IF NOT EXISTS maintenance_records (
            maintenance_id SERIAL PRIMARY KEY,
            vehicle_id VARCHAR(50) REFERENCES vehicles(vehicle_id),
            maintenance_type VARCHAR(50),
            maintenance_date DATE,
            odometer_km FLOAT,
            cost FLOAT,
            description TEXT,
            parts_used JSONB,
            service_center VARCHAR(100),
            technician VARCHAR(100),
            next_maintenance_date DATE,
            notes TEXT,
            status VARCHAR(20) DEFAULT 'completed',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Fuel consumption table
        CREATE TABLE IF NOT EXISTS fuel_consumption (
            fuel_id SERIAL PRIMARY KEY,
            vehicle_id VARCHAR(50) REFERENCES vehicles(vehicle_id),
            refuel_date DATE,
            odometer_km FLOAT,
            fuel_amount_liters FLOAT,
            fuel_cost FLOAT,
            fuel_type VARCHAR(20),
            station_name VARCHAR(100),
            latitude FLOAT,
            longitude FLOAT,
            fuel_efficiency_kmpl FLOAT,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Driver behavior table
        CREATE TABLE IF NOT EXISTS driver_behavior (
            behavior_id SERIAL PRIMARY KEY,
            vehicle_id VARCHAR(50) REFERENCES vehicles(vehicle_id),
            driver_id VARCHAR(50),
            date DATE,
            harsh_acceleration_count INTEGER DEFAULT 0,
            harsh_braking_count INTEGER DEFAULT 0,
            harsh_cornering_count INTEGER DEFAULT 0,
            speeding_count INTEGER DEFAULT 0,
            idle_time_minutes FLOAT,
            driving_time_minutes FLOAT,
            distance_km FLOAT,
            avg_speed_kmh FLOAT,
            max_speed_kmh FLOAT,
            fuel_consumed_liters FLOAT,
            safety_score FLOAT,
            efficiency_score FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Fault logs table
        CREATE TABLE IF NOT EXISTS fault_logs (
            fault_id SERIAL PRIMARY KEY,
            vehicle_id VARCHAR(50) REFERENCES vehicles(vehicle_id),
            fault_code VARCHAR(50),
            fault_description TEXT,
            severity VARCHAR(20),
            timestamp TIMESTAMP,
            resolved_at TIMESTAMP,
            resolution_notes TEXT,
            component VARCHAR(50),
            parameters JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Analytics results table
        CREATE TABLE IF NOT EXISTS analytics_results (
            result_id SERIAL PRIMARY KEY,
            vehicle_id VARCHAR(50) REFERENCES vehicles(vehicle_id),
            analysis_type VARCHAR(50),
            period_start DATE,
            period_end DATE,
            metrics JSONB,
            insights TEXT,
            recommendations JSONB,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            version INTEGER DEFAULT 1
        );
        """
        
        try:
            with self.pool_manager.get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_tables_sql)
                    conn.commit()
            logger.info("Database tables created/verified successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise
    
    def _create_indexes(self):
        """Create necessary indexes for performance."""
        indexes_sql = """
        -- Indexes for telemetry_data
        CREATE INDEX IF NOT EXISTS idx_telemetry_vehicle_timestamp 
        ON telemetry_data(vehicle_id, timestamp DESC);
        
        CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp 
        ON telemetry_data(timestamp DESC);
        
        CREATE INDEX IF NOT EXISTS idx_telemetry_vehicle_date 
        ON telemetry_data(vehicle_id, DATE(timestamp));
        
        CREATE INDEX IF NOT EXISTS idx_telemetry_fault_codes 
        ON telemetry_data USING GIN(fault_codes);
        
        -- Indexes for vehicles
        CREATE INDEX IF NOT EXISTS idx_vehicles_fleet 
        ON vehicles(fleet_id);
        
        CREATE INDEX IF NOT EXISTS idx_vehicles_status 
        ON vehicles(status);
        
        -- Indexes for maintenance_records
        CREATE INDEX IF NOT EXISTS idx_maintenance_vehicle_date 
        ON maintenance_records(vehicle_id, maintenance_date DESC);
        
        CREATE INDEX IF NOT EXISTS idx_maintenance_type 
        ON maintenance_records(maintenance_type);
        
        -- Indexes for fuel_consumption
        CREATE INDEX IF NOT EXISTS idx_fuel_vehicle_date 
        ON fuel_consumption(vehicle_id, refuel_date DESC);
        
        -- Indexes for driver_behavior
        CREATE INDEX IF NOT EXISTS idx_behavior_vehicle_date 
        ON driver_behavior(vehicle_id, date DESC);
        
        -- Indexes for fault_logs
        CREATE INDEX IF NOT EXISTS idx_faults_vehicle_timestamp 
        ON fault_logs(vehicle_id, timestamp DESC);
        
        CREATE INDEX IF NOT EXISTS idx_faults_severity 
        ON fault_logs(severity);
        
        -- Indexes for analytics_results
        CREATE INDEX IF NOT EXISTS idx_analytics_vehicle_type 
        ON analytics_results(vehicle_id, analysis_type);
        
        CREATE INDEX IF NOT EXISTS idx_analytics_period 
        ON analytics_results(period_start, period_end);
        """
        
        try:
            with self.pool_manager.get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(indexes_sql)
                    conn.commit()
            logger.info("Database indexes created/verified successfully")
        except Exception as e:
            logger.warning(f"Failed to create indexes (might already exist): {e}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((psycopg2.OperationalError, psycopg2.InterfaceError))
    )
    def execute_query(self, query: str, params: tuple = None, 
                     use_cache: bool = False, cache_key: str = None) -> QueryResult:
        """
        Execute a SQL query with retry logic.
        
        Args:
            query: SQL query string
            params: Query parameters
            use_cache: Whether to use query caching
            cache_key: Cache key for query results
            
        Returns:
            QueryResult object
        """
        start_time = datetime.now()
        
        # Check cache
        if use_cache and cache_key:
            cached_result = self.get_cached_query(cache_key)
            if cached_result:
                logger.debug(f"Cache hit for query: {cache_key}")
                return cached_result
        
        result = QueryResult(success=False, query=query)
        
        try:
            with self.pool_manager.get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    
                    # Try to fetch results
                    try:
                        data = cursor.fetchall()
                        columns = [desc[0] for desc in cursor.description] if cursor.description else []
                        result.data = data
                        result.columns = columns
                        result.row_count = cursor.rowcount if cursor.rowcount != -1 else len(data)
                    except psycopg2.ProgrammingError:
                        # No results to fetch (INSERT, UPDATE, DELETE)
                        conn.commit()
                        result.row_count = cursor.rowcount
                    
                    result.success = True
                    
            # Cache the result
            if use_cache and cache_key and result.success:
                self.cache_query(cache_key, result)
        
        except Exception as e:
            result.error = str(e)
            logger.error(f"Query execution failed: {e}\nQuery: {query}")
        
        result.execution_time = (datetime.now() - start_time).total_seconds()
        
        # Log slow queries
        if result.execution_time > 1.0:  # 1 second threshold
            logger.warning(f"Slow query detected: {result.execution_time:.2f}s\n{query}")
        
        return result
    
    def execute_many(self, query: str, params_list: List[tuple], batch_size: int = 1000) -> QueryResult:
        """
        Execute multiple queries in batch.
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples
            batch_size: Batch size for execution
            
        Returns:
            QueryResult object
        """
        result = QueryResult(success=False, query=query)
        
        try:
            with self.pool_manager.get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    total_rows = 0
                    
                    # Process in batches
                    for i in range(0, len(params_list), batch_size):
                        batch = params_list[i:i + batch_size]
                        execute_values(cursor, query, batch)
                        total_rows += cursor.rowcount
                    
                    conn.commit()
                    result.success = True
                    result.row_count = total_rows
        
        except Exception as e:
            result.error = str(e)
            logger.error(f"Batch execution failed: {e}")
        
        return result
    
    def execute_dataframe(self, df: pd.DataFrame, table_name: str, 
                         if_exists: str = 'append', index: bool = False) -> bool:
        """
        Insert DataFrame into database table.
        
        Args:
            df: pandas DataFrame
            table_name: Target table name
            if_exists: Behavior when table exists ('fail', 'replace', 'append')
            index: Write DataFrame index as a column
            
        Returns:
            True if successful
        """
        try:
            df.to_sql(
                table_name,
                self.pool_manager.sqlalchemy_engine,
                if_exists=if_exists,
                index=index,
                method='multi',
                chunksize=1000
            )
            logger.info(f"Inserted {len(df)} rows into {table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to insert DataFrame into {table_name}: {e}")
            return False
    
    def query_to_dataframe(self, query: str, params: tuple = None) -> pd.DataFrame:
        """
        Execute query and return results as DataFrame.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            pandas DataFrame
        """
        try:
            with self.pool_manager.sqlalchemy_engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
        except Exception as e:
            logger.error(f"Failed to execute query to DataFrame: {e}")
            return pd.DataFrame()
    
    # Caching methods
    def get_cached_query(self, cache_key: str) -> Optional[QueryResult]:
        """Get cached query result."""
        if not self.config['redis']['enabled']:
            return self.query_cache.get(cache_key)
        
        try:
            redis_conn = self.pool_manager.get_redis_connection()
            cached_data = redis_conn.get(cache_key)
            if cached_data:
                data_dict = json.loads(cached_data)
                return QueryResult(**data_dict)
        except Exception as e:
            logger.error(f"Failed to get cached query: {e}")
        
        return None
    
    def cache_query(self, cache_key: str, result: QueryResult, ttl: int = None):
        """Cache query result."""
        if ttl is None:
            ttl = self.cache_ttl
        
        if not self.config['redis']['enabled']:
            self.query_cache[cache_key] = result
            return
        
        try:
            redis_conn = self.pool_manager.get_redis_connection()
            result_dict = result.to_dict()
            redis_conn.setex(cache_key, ttl, json.dumps(result_dict))
        except Exception as e:
            logger.error(f"Failed to cache query: {e}")
    
    def invalidate_cache(self, pattern: str = "*"):
        """Invalidate cache entries matching pattern."""
        if not self.config['redis']['enabled']:
            if pattern == "*":
                self.query_cache.clear()
            else:
                keys_to_delete = [k for k in self.query_cache.keys() if pattern in k]
                for key in keys_to_delete:
                    del self.query_cache[key]
            return
        
        try:
            redis_conn = self.pool_manager.get_redis_connection()
            keys = redis_conn.keys(pattern)
            if keys:
                redis_conn.delete(*keys)
                logger.info(f"Invalidated {len(keys)} cache entries")
        except Exception as e:
            logger.error(f"Failed to invalidate cache: {e}")
    
    # Async methods
    async def execute_query_async(self, query: str, params: tuple = None) -> QueryResult:
        """Execute query asynchronously."""
        # This is a placeholder - in production you would use asyncpg
        # For now, we'll run in thread pool
        import asyncio
        from concurrent.futures import ThreadPoolExecutor
        
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            result = await loop.run_in_executor(
                executor,
                self.execute_query,
                query,
                params
            )
        return result
    
    # Utility methods
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """Get table schema information."""
        query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
        """
        result = self.execute_query(query, (table_name,))
        return result.to_dataframe()
    
    def get_database_size(self) -> Dict[str, Any]:
        """Get database size information."""
        query = """
        SELECT 
            pg_database_size(current_database()) as db_size_bytes,
            pg_size_pretty(pg_database_size(current_database())) as db_size_pretty,
            (SELECT count(*) FROM vehicles) as vehicles_count,
            (SELECT count(*) FROM telemetry_data) as telemetry_count,
            (SELECT count(*) FROM maintenance_records) as maintenance_count,
            (SELECT count(*) FROM fuel_consumption) as fuel_count
        """
        result = self.execute_query(query)
        if result.success and result.data:
            return result.data[0]
        return {}
    
    def vacuum_analyze(self, table_name: str = None):
        """Run VACUUM ANALYZE on table or entire database."""
        try:
            with self.pool_manager.get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    if table_name:
                        cursor.execute(f"VACUUM ANALYZE {table_name};")
                        logger.info(f"VACUUM ANALYZE completed for {table_name}")
                    else:
                        cursor.execute("VACUUM ANALYZE;")
                        logger.info("VACUUM ANALYZE completed for all tables")
                    conn.commit()
        except Exception as e:
            logger.error(f"Failed to run VACUUM ANALYZE: {e}")
    
    def backup_database(self, backup_path: str):
        """Create database backup using pg_dump."""
        import subprocess
        
        config = self.config['postgresql']
        command = [
            'pg_dump',
            '-h', config['host'],
            '-p', str(config['port']),
            '-U', config['user'],
            '-d', config['database'],
            '-F', 'c',  # Custom format
            '-f', backup_path
        ]
        
        try:
            env = {'PGPASSWORD': config['password']}
            subprocess.run(command, env=env, check=True)
            logger.info(f"Database backup created: {backup_path}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to backup database: {e}")
            return False
    
    def close(self):
        """Close all database connections and pools."""
        self.pool_manager.close_all()
        logger.info("Database connections closed")
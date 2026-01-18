"""
Database package for Vehicle Telemetry Analytics.
Provides database connection management, query execution, and data persistence.
"""
from .database_manager import DatabaseManager
from .sql_queries import (
    VehicleQueries,
    TelemetryQueries,
    AnalyticsQueries,
    MaintenanceQueries
)

__version__ = "1.0.0"
__all__ = [
    'DatabaseManager',
    'VehicleQueries',
    'TelemetryQueries',
    'AnalyticsQueries',
    'MaintenanceQueries'
]
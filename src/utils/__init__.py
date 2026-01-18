"""
Utilities package for Vehicle Telemetry Analytics.
Common utilities, helpers, and configuration management.
"""
from .config import (
    ConfigManager,
    load_config,
    get_config,
    validate_config,
    DatabaseConfig,
    SparkConfig,
    KafkaConfig,
    AppConfig
)
from .helpers import (
    setup_logging,
    Timer,
    format_bytes,
    format_timedelta,
    sanitize_filename,
    retry,
    exponential_backoff,
    generate_id,
    validate_email,
    validate_phone,
    calculate_distance,
    calculate_fuel_efficiency,
    detect_anomalies,
    normalize_data,
    MemoryProfiler,
    PerformanceMonitor
)

__version__ = "1.0.0"
__all__ = [
    # Config
    'ConfigManager',
    'load_config',
    'get_config',
    'validate_config',
    'DatabaseConfig',
    'SparkConfig',
    'KafkaConfig',
    'AppConfig',
    
    # Helpers
    'setup_logging',
    'Timer',
    'format_bytes',
    'format_timedelta',
    'sanitize_filename',
    'retry',
    'exponential_backoff',
    'generate_id',
    'validate_email',
    'validate_phone',
    'calculate_distance',
    'calculate_fuel_efficiency',
    'detect_anomalies',
    'normalize_data',
    'MemoryProfiler',
    'PerformanceMonitor'
]
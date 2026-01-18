# src/data_processing/__init__.py
"""
Vehicle Telemetry Analytics - Data Processing Module
This package contains data cleaning, transformation, and enrichment functions
for processing vehicle telemetry data.
"""

__version__ = "1.0.0"
__author__ = "Vehicle Telemetry Analytics Team"
__description__ = "Data processing pipelines for vehicle telemetry"

# Import key modules for easy access
from .data_cleaner import VehicleDataCleaner
from .data_enricher import DataEnricher
from .telemetry_simulator import TelemetrySimulator

# Define what gets imported with "from src.data_processing import *"
__all__ = [
    'VehicleDataCleaner',
    'DataEnricher', 
    'TelemetrySimulator',
]
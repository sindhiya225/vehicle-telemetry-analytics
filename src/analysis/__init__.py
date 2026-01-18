# src/analysis/__init__.py
"""
Vehicle Telemetry Analytics - Analysis Module
This package contains analytical functions for vehicle performance analysis,
efficiency calculations, maintenance predictions, and business insights.
"""

__version__ = "1.0.0"
__author__ = "Vehicle Telemetry Analytics Team"
__description__ = "Advanced analytics for vehicle telemetry data"

# Import key modules for easy access
from .performance_analyzer import PerformanceAnalyzer
from .efficiency_calculator import EfficiencyCalculator
from .maintenance_predictor import MaintenancePredictor

# Define what gets imported with "from src.analysis import *"
__all__ = [
    'PerformanceAnalyzer',
    'EfficiencyCalculator', 
    'MaintenancePredictor',
]
"""
Visualization package for Vehicle Telemetry Analytics Platform.
Provides dashboard generation, report creation, and data visualization capabilities.
"""
from .dashboard_generator import (
    DashboardGenerator,
    TelemetryDashboard,
    PerformanceDashboard,
    MaintenanceDashboard,
    FleetOverviewDashboard
)
from .report_generator import (
    ReportGenerator,
    PDFReportGenerator,
    HTMLReportGenerator,
    ExcelReportGenerator,
    PerformanceReport,
    MaintenanceReport,
    FleetReport
)

__version__ = "1.0.0"
__all__ = [
    # Dashboard generators
    'DashboardGenerator',
    'TelemetryDashboard',
    'PerformanceDashboard',
    'MaintenanceDashboard',
    'FleetOverviewDashboard',
    
    # Report generators
    'ReportGenerator',
    'PDFReportGenerator',
    'HTMLReportGenerator',
    'ExcelReportGenerator',
    'PerformanceReport',
    'MaintenanceReport',
    'FleetReport'
]
#!/usr/bin/env python3
"""
Main Entry Point for Vehicle Telemetry Analytics Pipeline
Complete end-to-end data processing, analysis, and visualization system
"""

import argparse
import logging
from pathlib import Path
import sys
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from src.utils.config import Config
from src.data_processing.data_cleaner import DataCleaner
from src.data_processing.data_enricher import DataEnricher
from src.data_processing.telemetry_simulator import TelemetrySimulator
from src.database.database_manager import DatabaseManager
from src.analysis.performance_analyzer import PerformanceAnalyzer
from src.analysis.efficiency_calculator import EfficiencyCalculator
from src.analysis.maintenance_predictor import MaintenancePredictor
from src.visualization.dashboard_generator import DashboardGenerator
from src.visualization.report_generator import ReportGenerator
from streaming.kafka.producer import TelemetryProducer
from streaming.spark.streaming_processor import StreamingProcessor
from monitoring.scripts.performance_monitor import PerformanceMonitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/vehicle_analytics.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class VehicleAnalyticsPipeline:
    """Main pipeline orchestrator for vehicle telemetry analytics"""
    
    def __init__(self, config_path='config/config.yaml'):
        self.config = Config.load(config_path)
        self.setup_directories()
        
    def setup_directories(self):
        """Create necessary directories"""
        directories = [
            'data/raw',
            'data/processed',
            'data/telemetry',
            'logs',
            'reports',
            'dashboard/assets',
            'models'
        ]
        for dir_path in directories:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    def run_batch_processing(self):
        """Run complete batch processing pipeline"""
        logger.info("Starting batch processing pipeline")
        
        try:
            # 1. Data Processing
            logger.info("Step 1: Data Cleaning")
            cleaner = DataCleaner()
            raw_data_path = 'data/raw/vehicle_data.csv'
            cleaned_data = cleaner.process(raw_data_path)
            
            logger.info("Step 2: Data Enrichment")
            enricher = DataEnricher()
            enriched_data = enricher.enrich(cleaned_data)
            
            # 2. Database Operations
            logger.info("Step 3: Database Operations")
            db_manager = DatabaseManager()
            db_manager.initialize_database()
            db_manager.insert_batch(enriched_data)
            
            # 3. Analysis
            logger.info("Step 4: Performance Analysis")
            analyzer = PerformanceAnalyzer()
            performance_metrics = analyzer.calculate_metrics()
            
            logger.info("Step 5: Efficiency Analysis")
            efficiency_calc = EfficiencyCalculator()
            efficiency_report = efficiency_calc.generate_report()
            
            logger.info("Step 6: Maintenance Prediction")
            predictor = MaintenancePredictor()
            maintenance_alerts = predictor.predict_failures()
            
            # 4. Visualization
            logger.info("Step 7: Dashboard Generation")
            dashboard_gen = DashboardGenerator()
            dashboard_gen.create_dashboard()
            
            logger.info("Step 8: Report Generation")
            report_gen = ReportGenerator()
            report_gen.generate_report()
            
            logger.info("Batch processing completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Batch processing failed: {str(e)}")
            return False
    
    def run_streaming_pipeline(self):
        """Start real-time streaming pipeline"""
        logger.info("Starting streaming pipeline")
        
        try:
            # Start Kafka producer for telemetry simulation
            producer = TelemetryProducer()
            producer_thread = producer.start_producing()
            
            # Start Spark Streaming processor
            spark_processor = StreamingProcessor()
            spark_processor.start_streaming()
            
            # Start performance monitoring
            monitor = PerformanceMonitor()
            monitor.start_monitoring()
            
            return producer_thread
            
        except Exception as e:
            logger.error(f"Streaming pipeline failed: {str(e)}")
            return None
    
    def run_analysis_only(self):
        """Run only analysis module"""
        logger.info("Running analysis module")
        
        analyzer = PerformanceAnalyzer()
        efficiency_calc = EfficiencyCalculator()
        predictor = MaintenancePredictor()
        
        results = {
            'performance': analyzer.calculate_metrics(),
            'efficiency': efficiency_calc.generate_report(),
            'maintenance': predictor.predict_failures()
        }
        
        return results
    
    def export_for_tableau(self):
        """Export processed data for Tableau dashboard"""
        logger.info("Exporting data for Tableau")
        
        db_manager = DatabaseManager()
        tableau_data = db_manager.export_tableau_data()
        
        export_path = 'dashboard/tableau/vehicle_data.hyper'
        tableau_data.to_csv('dashboard/tableau/vehicle_data.csv', index=False)
        
        logger.info(f"Data exported to {export_path}")
        return export_path

def main():
    parser = argparse.ArgumentParser(
        description='Vehicle Telemetry Analytics Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python main.py --batch          # Run batch processing
  python main.py --streaming      # Start streaming pipeline
  python main.py --analysis       # Run analysis only
  python main.py --tableau        # Export for Tableau
  python main.py --all            # Run complete pipeline
        '''
    )
    
    parser.add_argument('--batch', action='store_true', help='Run batch processing')
    parser.add_argument('--streaming', action='store_true', help='Start streaming pipeline')
    parser.add_argument('--analysis', action='store_true', help='Run analysis only')
    parser.add_argument('--tableau', action='store_true', help='Export data for Tableau')
    parser.add_argument('--all', action='store_true', help='Run complete pipeline')
    parser.add_argument('--config', default='config/config.yaml', help='Configuration file path')
    
    args = parser.parse_args()
    
    pipeline = VehicleAnalyticsPipeline(args.config)
    
    if args.all or (not any([args.batch, args.streaming, args.analysis, args.tableau])):
        # Run complete pipeline
        pipeline.run_batch_processing()
        pipeline.run_streaming_pipeline()
        pipeline.export_for_tableau()
        
    if args.batch:
        pipeline.run_batch_processing()
        
    if args.streaming:
        pipeline.run_streaming_pipeline()
        
    if args.analysis:
        results = pipeline.run_analysis_only()
        logger.info(f"Analysis results: {results}")
        
    if args.tableau:
        pipeline.export_for_tableau()
    
    logger.info("Vehicle Telemetry Analytics Pipeline completed")

if __name__ == "__main__":
    main()
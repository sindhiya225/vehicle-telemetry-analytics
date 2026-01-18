#!/usr/bin/env python3
"""
Comprehensive Health Check Script for Vehicle Telemetry Analytics Platform
Checks all components: Kafka, Spark, Prometheus, Grafana, Database, etc.
"""
import requests
import json
import time
import logging
import subprocess
import sys
from typing import Dict, List, Tuple, Optional
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client.parser import text_string_to_metric_families

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HealthChecker:
    def __init__(self):
        self.config = {
            'kafka_brokers': ['kafka-service:9092'],
            'spark_master': 'http://spark-master:8080',
            'spark_ui': 'http://spark-master:4040',
            'prometheus': 'http://prometheus:9090',
            'grafana': 'http://grafana:3000',
            'postgres': {
                'host': 'postgres-service',
                'port': 5432,
                'database': 'vehicle_analytics',
                'user': 'admin',
                'password': 'admin123'
            },
            'timeout': 10,
            'check_interval': 30
        }
        
        self.health_status = {}
        self.metrics = {}
        
    def check_kafka(self) -> Tuple[bool, Dict]:
        """Check Kafka cluster health"""
        try:
            # Check connectivity
            consumer = KafkaConsumer(
                bootstrap_servers=self.config['kafka_brokers'],
                request_timeout_ms=self.config['timeout'] * 1000,
                api_version=(2, 0, 0)
            )
            topics = consumer.topics()
            consumer.close()
            
            # Check topic health
            producer = KafkaProducer(
                bootstrap_servers=self.config['kafka_brokers'],
                request_timeout_ms=self.config['timeout'] * 1000,
                api_version=(2, 0, 0)
            )
            
            # Test message production
            test_topic = 'health-check-topic'
            future = producer.send(test_topic, b'test_message')
            result = future.get(timeout=self.config['timeout'])
            producer.close()
            
            status = {
                'connected': True,
                'topics_available': len(topics) > 0,
                'topics_count': len(topics),
                'message_produced': True,
                'brokers': self.config['kafka_brokers']
            }
            return True, status
            
        except Exception as e:
            logger.error(f"Kafka health check failed: {e}")
            return False, {'error': str(e)}
    
    def check_spark(self) -> Tuple[bool, Dict]:
        """Check Spark cluster health"""
        try:
            # Check Spark Master
            master_response = requests.get(
                f"{self.config['spark_master']}/json/",
                timeout=self.config['timeout']
            )
            master_data = master_response.json()
            
            # Check Spark UI
            ui_response = requests.get(
                f"{self.config['spark_ui']}/api/v1/applications",
                timeout=self.config['timeout']
            )
            ui_data = ui_response.json() if ui_response.status_code == 200 else []
            
            status = {
                'master_alive': master_response.status_code == 200,
                'workers_alive': master_data.get('workers', []),
                'workers_count': len(master_data.get('workers', [])),
                'active_apps': len(master_data.get('activeapps', [])),
                'ui_accessible': ui_response.status_code == 200,
                'applications_count': len(ui_data)
            }
            return True, status
            
        except Exception as e:
            logger.error(f"Spark health check failed: {e}")
            return False, {'error': str(e)}
    
    def check_prometheus(self) -> Tuple[bool, Dict]:
        """Check Prometheus health and metrics"""
        try:
            # Check Prometheus status
            status_response = requests.get(
                f"{self.config['prometheus']}/-/healthy",
                timeout=self.config['timeout']
            )
            
            # Check metrics endpoint
            metrics_response = requests.get(
                f"{self.config['prometheus']}/api/v1/query?query=up",
                timeout=self.config['timeout']
            )
            metrics_data = metrics_response.json()
            
            # Get scrape targets
            targets_response = requests.get(
                f"{self.config['prometheus']}/api/v1/targets",
                timeout=self.config['timeout']
            )
            targets_data = targets_response.json()
            
            active_targets = 0
            total_targets = 0
            for target in targets_data['data']['activeTargets']:
                total_targets += 1
                if target['health'] == 'up':
                    active_targets += 1
            
            status = {
                'prometheus_healthy': status_response.status_code == 200,
                'metrics_available': len(metrics_data['data']['result']) > 0,
                'targets_total': total_targets,
                'targets_active': active_targets,
                'targets_health_percentage': (active_targets / total_targets * 100) if total_targets > 0 else 0
            }
            return True, status
            
        except Exception as e:
            logger.error(f"Prometheus health check failed: {e}")
            return False, {'error': str(e)}
    
    def check_grafana(self) -> Tuple[bool, Dict]:
        """Check Grafana health"""
        try:
            # Check Grafana health
            health_response = requests.get(
                f"{self.config['grafana']}/api/health",
                timeout=self.config['timeout']
            )
            health_data = health_response.json()
            
            # Check datasources
            auth = ('admin', 'admin123')
            datasources_response = requests.get(
                f"{self.config['grafana']}/api/datasources",
                auth=auth,
                timeout=self.config['timeout']
            )
            datasources = datasources_response.json() if datasources_response.status_code == 200 else []
            
            # Check dashboards
            dashboards_response = requests.get(
                f"{self.config['grafana']}/api/search",
                auth=auth,
                timeout=self.config['timeout']
            )
            dashboards = dashboards_response.json() if dashboards_response.status_code == 200 else []
            
            status = {
                'grafana_healthy': health_data.get('database', '') == 'ok',
                'datasources_count': len(datasources),
                'dashboards_count': len(dashboards),
                'version': health_data.get('version', 'unknown')
            }
            return True, status
            
        except Exception as e:
            logger.error(f"Grafana health check failed: {e}")
            return False, {'error': str(e)}
    
    def check_database(self) -> Tuple[bool, Dict]:
        """Check PostgreSQL database health"""
        try:
            conn = psycopg2.connect(
                host=self.config['postgres']['host'],
                port=self.config['postgres']['port'],
                database=self.config['postgres']['database'],
                user=self.config['postgres']['user'],
                password=self.config['postgres']['password']
            )
            cursor = conn.cursor()
            
            # Check connection and basic queries
            cursor.execute("SELECT version();")
            db_version = cursor.fetchone()
            
            cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
            table_count = cursor.fetchone()[0]
            
            # Check vehicle_data table
            cursor.execute("SELECT COUNT(*) FROM vehicle_data;")
            vehicle_data_count = cursor.fetchone()[0]
            
            # Check telemetry_data table
            cursor.execute("SELECT COUNT(*) FROM telemetry_data;")
            telemetry_data_count = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            status = {
                'connected': True,
                'database_version': db_version[0],
                'tables_count': table_count,
                'vehicle_records': vehicle_data_count,
                'telemetry_records': telemetry_data_count
            }
            return True, status
            
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False, {'error': str(e)}
    
    def check_vehicle_metrics(self) -> Tuple[bool, Dict]:
        """Check vehicle metrics exporter"""
        try:
            response = requests.get(
                "http://vehicle-metrics-exporter:8000/metrics",
                timeout=self.config['timeout']
            )
            
            metrics = {}
            for family in text_string_to_metric_families(response.text):
                for sample in family.samples:
                    if sample.name.startswith('vehicle_'):
                        metrics[sample.name] = sample.value
            
            status = {
                'exporter_accessible': True,
                'metrics_count': len(metrics),
                'has_vehicle_speed': 'vehicle_speed' in metrics,
                'has_engine_temp': 'engine_temperature' in metrics,
                'has_fuel_level': 'fuel_level' in metrics
            }
            return True, status
            
        except Exception as e:
            logger.error(f"Vehicle metrics check failed: {e}")
            return False, {'error': str(e)}
    
    def check_system_resources(self) -> Tuple[bool, Dict]:
        """Check system resources using psutil or system commands"""
        try:
            import psutil
            
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            
            # Disk usage
            disk = psutil.disk_usage('/')
            
            # Network connections
            connections = psutil.net_connections()
            
            status = {
                'cpu_percent': cpu_percent,
                'memory_total_gb': memory.total / (1024**3),
                'memory_used_gb': memory.used / (1024**3),
                'memory_percent': memory.percent,
                'disk_total_gb': disk.total / (1024**3),
                'disk_used_gb': disk.used / (1024**3),
                'disk_percent': disk.percent,
                'network_connections': len(connections)
            }
            return True, status
            
        except ImportError:
            # Fallback to system commands if psutil not available
            try:
                # Get CPU load
                with open('/proc/loadavg', 'r') as f:
                    load_avg = f.read().strip().split()
                
                # Get memory info
                with open('/proc/meminfo', 'r') as f:
                    mem_lines = f.readlines()
                    mem_total = 0
                    mem_available = 0
                    for line in mem_lines:
                        if 'MemTotal:' in line:
                            mem_total = int(line.split()[1]) / 1024  # Convert to MB
                        elif 'MemAvailable:' in line:
                            mem_available = int(line.split()[1]) / 1024
                
                status = {
                    'load_1min': float(load_avg[0]),
                    'load_5min': float(load_avg[1]),
                    'load_15min': float(load_avg[2]),
                    'memory_total_mb': mem_total,
                    'memory_available_mb': mem_available,
                    'memory_percent': ((mem_total - mem_available) / mem_total * 100) if mem_total > 0 else 0
                }
                return True, status
            except Exception as e:
                logger.error(f"System resources check failed: {e}")
                return False, {'error': str(e)}
    
    def generate_health_report(self) -> Dict:
        """Generate comprehensive health report"""
        report = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'overall_status': 'HEALTHY',
            'checks': {}
        }
        
        # Run all health checks
        checks = [
            ('kafka', self.check_kafka),
            ('spark', self.check_spark),
            ('prometheus', self.check_prometheus),
            ('grafana', self.check_grafana),
            ('database', self.check_database),
            ('vehicle_metrics', self.check_vehicle_metrics),
            ('system_resources', self.check_system_resources)
        ]
        
        failed_checks = []
        
        for check_name, check_func in checks:
            try:
                success, details = check_func()
                report['checks'][check_name] = {
                    'status': 'PASS' if success else 'FAIL',
                    'details': details
                }
                if not success:
                    failed_checks.append(check_name)
            except Exception as e:
                report['checks'][check_name] = {
                    'status': 'ERROR',
                    'details': {'error': str(e)}
                }
                failed_checks.append(check_name)
        
        # Determine overall status
        if len(failed_checks) == 0:
            report['overall_status'] = 'HEALTHY'
        elif len(failed_checks) <= 2:
            report['overall_status'] = 'DEGRADED'
        else:
            report['overall_status'] = 'UNHEALTHY'
        
        report['failed_checks'] = failed_checks
        report['health_score'] = (len(checks) - len(failed_checks)) / len(checks) * 100
        
        return report
    
    def send_alert(self, report: Dict):
        """Send alert if system is unhealthy"""
        if report['overall_status'] in ['DEGRADED', 'UNHEALTHY']:
            alert_message = {
                'severity': 'WARNING' if report['overall_status'] == 'DEGRADED' else 'CRITICAL',
                'system': 'Vehicle Analytics Platform',
                'status': report['overall_status'],
                'failed_checks': report['failed_checks'],
                'health_score': report['health_score'],
                'timestamp': report['timestamp']
            }
            
            # Log alert
            logger.warning(f"System health alert: {json.dumps(alert_message, indent=2)}")
            
            # Send to Kafka (if available)
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.config['kafka_brokers'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                producer.send('system-alerts', alert_message)
                producer.flush()
                producer.close()
            except Exception as e:
                logger.error(f"Failed to send alert to Kafka: {e}")
    
    def run_continuous_monitoring(self):
        """Run continuous health monitoring"""
        logger.info("Starting continuous health monitoring...")
        
        while True:
            try:
                report = self.generate_health_report()
                
                # Print report
                print("\n" + "="*60)
                print(f"HEALTH REPORT - {report['timestamp']}")
                print("="*60)
                print(f"Overall Status: {report['overall_status']}")
                print(f"Health Score: {report['health_score']:.1f}%")
                
                for check_name, check_result in report['checks'].items():
                    status_icon = "✅" if check_result['status'] == 'PASS' else "❌"
                    print(f"\n{status_icon} {check_name.upper()}: {check_result['status']}")
                    if 'details' in check_result:
                        for key, value in check_result['details'].items():
                            if isinstance(value, (int, float)):
                                print(f"  {key}: {value}")
                            elif isinstance(value, list):
                                print(f"  {key}: {len(value)} items")
                
                # Send alert if needed
                self.send_alert(report)
                
                # Save report to file
                with open('/var/log/health_reports.json', 'a') as f:
                    f.write(json.dumps(report) + '\n')
                
                time.sleep(self.config['check_interval'])
                
            except KeyboardInterrupt:
                logger.info("Health monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in health monitoring: {e}")
                time.sleep(self.config['check_interval'])

def main():
    """Main entry point"""
    checker = HealthChecker()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--continuous':
        checker.run_continuous_monitoring()
    else:
        # Run single check
        report = checker.generate_health_report()
        print(json.dumps(report, indent=2))
        
        if report['overall_status'] != 'HEALTHY':
            sys.exit(1)

if __name__ == '__main__':
    main()
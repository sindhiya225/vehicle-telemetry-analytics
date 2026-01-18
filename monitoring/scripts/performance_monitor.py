#!/usr/bin/env python3
"""
Performance Monitoring Script for Vehicle Telemetry Analytics Platform
Monitors performance metrics, identifies bottlenecks, and generates optimization recommendations.
"""
import time
import json
import logging
import statistics
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
import psutil
import requests
import numpy as np
from prometheus_client.parser import text_string_to_metric_families
from dataclasses import dataclass
from enum import Enum

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PerformanceLevel(Enum):
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    FAIR = "FAIR"
    POOR = "POOR"
    CRITICAL = "CRITICAL"

@dataclass
class PerformanceMetric:
    name: str
    value: float
    unit: str
    level: PerformanceLevel
    threshold_good: float
    threshold_poor: float
    
    @property
    def score(self) -> float:
        """Calculate performance score (0-100)"""
        if self.value <= self.threshold_good:
            return 100.0
        elif self.value >= self.threshold_poor:
            return 0.0
        else:
            return 100 * (1 - (self.value - self.threshold_good) / 
                         (self.threshold_poor - self.threshold_good))

@dataclass
class OptimizationRecommendation:
    component: str
    issue: str
    impact: str
    recommendation: str
    priority: int  # 1=High, 2=Medium, 3=Low
    estimated_improvement: str

class PerformanceMonitor:
    def __init__(self):
        self.config = {
            'prometheus_url': 'http://prometheus:9090',
            'kafka_url': 'kafka-service:9092',
            'spark_url': 'http://spark-master:8080',
            'metrics_cache_duration': 300,  # 5 minutes
            'performance_thresholds': {
                'cpu_percent': {'good': 50, 'poor': 80},
                'memory_percent': {'good': 60, 'poor': 85},
                'disk_percent': {'good': 70, 'poor': 90},
                'network_latency_ms': {'good': 50, 'poor': 200},
                'kafka_lag': {'good': 100, 'poor': 1000},
                'spark_task_duration_ms': {'good': 1000, 'poor': 5000},
                'query_duration_ms': {'good': 100, 'poor': 1000},
                'api_response_ms': {'good': 100, 'poor': 500}
            }
        }
        
        self.metrics_history = {}
        self.performance_scores = {}
        
    def get_prometheus_metrics(self, query: str) -> List[Dict]:
        """Query Prometheus for metrics"""
        try:
            response = requests.get(
                f"{self.config['prometheus_url']}/api/v1/query",
                params={'query': query},
                timeout=10
            )
            data = response.json()
            return data['data']['result']
        except Exception as e:
            logger.error(f"Failed to query Prometheus: {e}")
            return []
    
    def get_system_metrics(self) -> Dict[str, PerformanceMetric]:
        """Get system-level performance metrics"""
        import psutil
        
        metrics = {}
        
        # CPU Metrics
        cpu_percent = psutil.cpu_percent(interval=1, percpu=True)
        cpu_avg = statistics.mean(cpu_percent)
        metrics['cpu_usage'] = PerformanceMetric(
            name='CPU Usage',
            value=cpu_avg,
            unit='%',
            level=self._get_performance_level('cpu_percent', cpu_avg),
            threshold_good=self.config['performance_thresholds']['cpu_percent']['good'],
            threshold_poor=self.config['performance_thresholds']['cpu_percent']['poor']
        )
        
        # Memory Metrics
        memory = psutil.virtual_memory()
        metrics['memory_usage'] = PerformanceMetric(
            name='Memory Usage',
            value=memory.percent,
            unit='%',
            level=self._get_performance_level('memory_percent', memory.percent),
            threshold_good=self.config['performance_thresholds']['memory_percent']['good'],
            threshold_poor=self.config['performance_thresholds']['memory_percent']['poor']
        )
        
        # Disk Metrics
        disk = psutil.disk_usage('/')
        metrics['disk_usage'] = PerformanceMetric(
            name='Disk Usage',
            value=disk.percent,
            unit='%',
            level=self._get_performance_level('disk_percent', disk.percent),
            threshold_good=self.config['performance_thresholds']['disk_percent']['good'],
            threshold_poor=self.config['performance_thresholds']['disk_percent']['poor']
        )
        
        # Network Metrics
        net_io = psutil.net_io_counters()
        metrics['network_throughput'] = PerformanceMetric(
            name='Network Throughput',
            value=(net_io.bytes_sent + net_io.bytes_recv) / 1024 / 1024,  # MB
            unit='MB',
            level=PerformanceLevel.EXCELLENT,  # This needs context
            threshold_good=100,
            threshold_poor=1000
        )
        
        return metrics
    
    def get_kafka_metrics(self) -> Dict[str, PerformanceMetric]:
        """Get Kafka performance metrics"""
        metrics = {}
        
        try:
            # Get Kafka lag from Prometheus
            lag_query = 'kafka_consumer_group_lag'
            lag_results = self.get_prometheus_metrics(lag_query)
            
            if lag_results:
                max_lag = max(float(r['value'][1]) for r in lag_results)
                metrics['kafka_lag'] = PerformanceMetric(
                    name='Kafka Consumer Lag',
                    value=max_lag,
                    unit='messages',
                    level=self._get_performance_level('kafka_lag', max_lag),
                    threshold_good=self.config['performance_thresholds']['kafka_lag']['good'],
                    threshold_poor=self.config['performance_thresholds']['kafka_lag']['poor']
                )
            
            # Get Kafka throughput
            throughput_query = 'rate(kafka_server_brokertopicmetrics_bytesin_total[5m])'
            throughput_results = self.get_prometheus_metrics(throughput_query)
            
            if throughput_results:
                total_throughput = sum(float(r['value'][1]) for r in throughput_results)
                metrics['kafka_throughput'] = PerformanceMetric(
                    name='Kafka Throughput',
                    value=total_throughput / 1024,  # KB/s
                    unit='KB/s',
                    level=PerformanceLevel.GOOD if total_throughput > 0 else PerformanceLevel.POOR,
                    threshold_good=100,
                    threshold_poor=10000
                )
                
        except Exception as e:
            logger.error(f"Failed to get Kafka metrics: {e}")
            
        return metrics
    
    def get_spark_metrics(self) -> Dict[str, PerformanceMetric]:
        """Get Spark performance metrics"""
        metrics = {}
        
        try:
            # Get Spark application metrics
            apps_query = 'spark_driver_DAGSch_ui_jobs_allJobIds'
            apps_results = self.get_prometheus_metrics(apps_query)
            
            if apps_results:
                metrics['spark_apps_running'] = PerformanceMetric(
                    name='Spark Applications Running',
                    value=len(apps_results),
                    unit='count',
                    level=PerformanceLevel.GOOD if len(apps_results) > 0 else PerformanceLevel.POOR,
                    threshold_good=1,
                    threshold_poor=10
                )
            
            # Get Spark task metrics
            tasks_query = 'spark_executor_metrics_totalTasks_Value'
            tasks_results = self.get_prometheus_metrics(tasks_query)
            
            if tasks_results:
                total_tasks = sum(float(r['value'][1]) for r in tasks_results)
                metrics['spark_total_tasks'] = PerformanceMetric(
                    name='Spark Total Tasks',
                    value=total_tasks,
                    unit='count',
                    level=PerformanceLevel.GOOD,
                    threshold_good=0,
                    threshold_poor=10000
                )
            
            # Get Spark memory metrics
            memory_query = 'spark_driver_BlockManager_memory_memUsed_MB'
            memory_results = self.get_prometheus_metrics(memory_query)
            
            if memory_results:
                memory_used = float(memory_results[0]['value'][1])
                metrics['spark_memory_used'] = PerformanceMetric(
                    name='Spark Memory Used',
                    value=memory_used,
                    unit='MB',
                    level=PerformanceLevel.GOOD if memory_used < 1024 else PerformanceLevel.POOR,
                    threshold_good=512,
                    threshold_poor=2048
                )
                
        except Exception as e:
            logger.error(f"Failed to get Spark metrics: {e}")
            
        return metrics
    
    def get_vehicle_telemetry_metrics(self) -> Dict[str, PerformanceMetric]:
        """Get vehicle telemetry processing metrics"""
        metrics = {}
        
        try:
            # Get telemetry processing rate
            rate_query = 'rate(vehicle_telemetry_processed_total[5m])'
            rate_results = self.get_prometheus_metrics(rate_query)
            
            if rate_results:
                processing_rate = float(rate_results[0]['value'][1])
                metrics['telemetry_processing_rate'] = PerformanceMetric(
                    name='Telemetry Processing Rate',
                    value=processing_rate,
                    unit='messages/sec',
                    level=PerformanceLevel.GOOD if processing_rate > 10 else PerformanceLevel.POOR,
                    threshold_good=10,
                    threshold_poor=1000
                )
            
            # Get processing latency
            latency_query = 'vehicle_telemetry_processing_latency_seconds'
            latency_results = self.get_prometheus_metrics(latency_query)
            
            if latency_results:
                latency = float(latency_results[0]['value'][1]) * 1000  # Convert to ms
                metrics['telemetry_processing_latency'] = PerformanceMetric(
                    name='Telemetry Processing Latency',
                    value=latency,
                    unit='ms',
                    level=self._get_performance_level('api_response_ms', latency),
                    threshold_good=self.config['performance_thresholds']['api_response_ms']['good'],
                    threshold_poor=self.config['performance_thresholds']['api_response_ms']['poor']
                )
            
            # Get data volume
            volume_query = 'vehicle_telemetry_volume_bytes_total'
            volume_results = self.get_prometheus_metrics(volume_query)
            
            if volume_results:
                volume = float(volume_results[0]['value'][1]) / (1024**2)  # Convert to MB
                metrics['telemetry_data_volume'] = PerformanceMetric(
                    name='Telemetry Data Volume',
                    value=volume,
                    unit='MB',
                    level=PerformanceLevel.GOOD,
                    threshold_good=0,
                    threshold_poor=10000
                )
                
        except Exception as e:
            logger.error(f"Failed to get telemetry metrics: {e}")
            
        return metrics
    
    def _get_performance_level(self, metric_type: str, value: float) -> PerformanceLevel:
        """Determine performance level based on thresholds"""
        thresholds = self.config['performance_thresholds'].get(metric_type, {'good': 50, 'poor': 80})
        
        if value <= thresholds['good']:
            return PerformanceLevel.EXCELLENT
        elif value <= (thresholds['good'] + thresholds['poor']) / 2:
            return PerformanceLevel.GOOD
        elif value <= thresholds['poor']:
            return PerformanceLevel.FAIR
        else:
            return PerformanceLevel.POOR
    
    def analyze_bottlenecks(self, metrics: Dict[str, Dict[str, PerformanceMetric]]) -> List[OptimizationRecommendation]:
        """Analyze performance data and identify bottlenecks"""
        recommendations = []
        
        # Analyze system metrics
        system_metrics = metrics.get('system', {})
        if 'cpu_usage' in system_metrics and system_metrics['cpu_usage'].level in [PerformanceLevel.POOR, PerformanceLevel.CRITICAL]:
            recommendations.append(
                OptimizationRecommendation(
                    component='System',
                    issue='High CPU usage detected',
                    impact='Reduced processing capacity, potential delays in data processing',
                    recommendation='Consider scaling up compute resources or optimizing CPU-intensive operations',
                    priority=1,
                    estimated_improvement='20-30% performance improvement'
                )
            )
        
        if 'memory_usage' in system_metrics and system_metrics['memory_usage'].level in [PerformanceLevel.POOR, PerformanceLevel.CRITICAL]:
            recommendations.append(
                OptimizationRecommendation(
                    component='System',
                    issue='High memory usage detected',
                    impact='Potential out-of-memory errors, increased swap usage, slower processing',
                    recommendation='Increase memory allocation or optimize memory usage in applications',
                    priority=1,
                    estimated_improvement='15-25% reduction in processing time'
                )
            )
        
        # Analyze Kafka metrics
        kafka_metrics = metrics.get('kafka', {})
        if 'kafka_lag' in kafka_metrics and kafka_metrics['kafka_lag'].level in [PerformanceLevel.POOR, PerformanceLevel.CRITICAL]:
            recommendations.append(
                OptimizationRecommendation(
                    component='Kafka',
                    issue='High consumer lag detected',
                    impact='Delayed data processing, stale analytics',
                    recommendation='Increase number of consumer partitions or optimize consumer processing logic',
                    priority=2,
                    estimated_improvement='50-70% reduction in processing delay'
                )
            )
        
        # Analyze Spark metrics
        spark_metrics = metrics.get('spark', {})
        if 'spark_memory_used' in spark_metrics and spark_metrics['spark_memory_used'].value > 1024:  # > 1GB
            recommendations.append(
                OptimizationRecommendation(
                    component='Spark',
                    issue='High Spark memory usage',
                    impact='Potential memory spills to disk, slower processing',
                    recommendation='Optimize Spark configuration, increase executor memory, or use broadcast joins',
                    priority=2,
                    estimated_improvement='30-40% faster Spark jobs'
                )
            )
        
        # Analyze telemetry metrics
        telemetry_metrics = metrics.get('telemetry', {})
        if 'telemetry_processing_latency' in telemetry_metrics and telemetry_metrics['telemetry_processing_latency'].level in [PerformanceLevel.POOR, PerformanceLevel.CRITICAL]:
            recommendations.append(
                OptimizationRecommendation(
                    component='Telemetry Processing',
                    issue='High processing latency',
                    impact='Delayed insights, outdated analytics',
                    recommendation='Optimize data pipelines, implement parallel processing, or use more efficient serialization',
                    priority=1,
                    estimated_improvement='40-60% reduction in latency'
                )
            )
        
        # General recommendations based on patterns
        if len(recommendations) == 0:
            # Check for potential improvements
            all_metrics = []
            for category in metrics.values():
                all_metrics.extend(category.values())
            
            avg_score = statistics.mean(m.score for m in all_metrics if hasattr(m, 'score'))
            
            if avg_score < 80:
                recommendations.append(
                    OptimizationRecommendation(
                        component='System',
                        issue='Suboptimal overall performance',
                        impact='Reduced efficiency across multiple components',
                        recommendation='Conduct comprehensive performance profiling and implement systematic optimizations',
                        priority=3,
                        estimated_improvement='10-20% overall performance improvement'
                    )
                )
        
        # Sort recommendations by priority
        recommendations.sort(key=lambda x: x.priority)
        
        return recommendations
    
    def calculate_performance_score(self, metrics: Dict[str, Dict[str, PerformanceMetric]]) -> Dict[str, Any]:
        """Calculate overall and component performance scores"""
        scores = {}
        
        for category, category_metrics in metrics.items():
            if category_metrics:
                category_scores = [m.score for m in category_metrics.values() if hasattr(m, 'score')]
                if category_scores:
                    scores[category] = {
                        'average': statistics.mean(category_scores),
                        'median': statistics.median(category_scores),
                        'min': min(category_scores),
                        'max': max(category_scores),
                        'count': len(category_scores)
                    }
        
        # Calculate overall score
        all_scores = []
        for category_score in scores.values():
            all_scores.append(category_score['average'])
        
        if all_scores:
            scores['overall'] = {
                'average': statistics.mean(all_scores),
                'weighted_average': statistics.mean(all_scores),  # Could add weights here
                'level': self._get_score_level(statistics.mean(all_scores))
            }
        
        return scores
    
    def _get_score_level(self, score: float) -> PerformanceLevel:
        """Convert numerical score to performance level"""
        if score >= 90:
            return PerformanceLevel.EXCELLENT
        elif score >= 75:
            return PerformanceLevel.GOOD
        elif score >= 60:
            return PerformanceLevel.FAIR
        elif score >= 40:
            return PerformanceLevel.POOR
        else:
            return PerformanceLevel.CRITICAL
    
    def generate_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report"""
        # Collect all metrics
        metrics = {
            'system': self.get_system_metrics(),
            'kafka': self.get_kafka_metrics(),
            'spark': self.get_spark_metrics(),
            'telemetry': self.get_vehicle_telemetry_metrics()
        }
        
        # Calculate scores
        scores = self.calculate_performance_score(metrics)
        
        # Analyze bottlenecks
        recommendations = self.analyze_bottlenecks(metrics)
        
        # Generate report
        report = {
            'timestamp': datetime.now().isoformat(),
            'metrics': {},
            'scores': scores,
            'recommendations': [rec.__dict__ for rec in recommendations],
            'summary': {
                'overall_score': scores.get('overall', {}).get('average', 0),
                'overall_level': scores.get('overall', {}).get('level', PerformanceLevel.POOR).value,
                'components_analyzed': len(metrics),
                'issues_identified': len(recommendations)
            }
        }
        
        # Convert metrics to serializable format
        for category, category_metrics in metrics.items():
            report['metrics'][category] = {}
            for metric_name, metric in category_metrics.items():
                report['metrics'][category][metric_name] = {
                    'value': metric.value,
                    'unit': metric.unit,
                    'level': metric.level.value,
                    'score': metric.score
                }
        
        return report
    
    def run_continuous_monitoring(self, interval_seconds: int = 60):
        """Run continuous performance monitoring"""
        logger.info(f"Starting continuous performance monitoring (interval: {interval_seconds}s)")
        
        while True:
            try:
                report = self.generate_performance_report()
                
                # Print summary
                print("\n" + "="*70)
                print(f"PERFORMANCE REPORT - {report['timestamp']}")
                print("="*70)
                print(f"Overall Score: {report['summary']['overall_score']:.1f}/100 ({report['summary']['overall_level']})")
                
                # Print component scores
                print("\nComponent Scores:")
                for category, score_data in report['scores'].items():
                    if category != 'overall':
                        print(f"  {category.upper():20} {score_data['average']:5.1f}/100")
                
                # Print recommendations
                if report['recommendations']:
                    print(f"\nOptimization Recommendations ({len(report['recommendations'])} found):")
                    for i, rec in enumerate(report['recommendations'], 1):
                        priority_icon = '🔴' if rec['priority'] == 1 else '🟡' if rec['priority'] == 2 else '🟢'
                        print(f"\n{priority_icon} #{i}: {rec['component']}")
                        print(f"   Issue: {rec['issue']}")
                        print(f"   Recommendation: {rec['recommendation']}")
                        print(f"   Estimated Improvement: {rec['estimated_improvement']}")
                
                # Save report
                filename = f"/var/log/performance_reports/report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(filename, 'w') as f:
                    json.dump(report, f, indent=2)
                
                # Send to monitoring system if needed
                self._send_to_monitoring(report)
                
                time.sleep(interval_seconds)
                
            except KeyboardInterrupt:
                logger.info("Performance monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in performance monitoring: {e}")
                time.sleep(interval_seconds)
    
    def _send_to_monitoring(self, report: Dict):
        """Send performance report to monitoring system"""
        # Could send to Prometheus, Kafka, or external monitoring service
        pass

def main():
    """Main entry point"""
    monitor = PerformanceMonitor()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--continuous':
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 60
        monitor.run_continuous_monitoring(interval)
    else:
        # Generate single report
        report = monitor.generate_performance_report()
        print(json.dumps(report, indent=2))

if __name__ == '__main__':
    main()
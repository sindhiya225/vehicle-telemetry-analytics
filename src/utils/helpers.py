"""
Utility functions and helpers for Vehicle Telemetry Analytics Platform.
"""
import logging
import time
import hashlib
import json
import inspect
import functools
import asyncio
import random
import string
import re
import math
import statistics
import datetime
from typing import Dict, List, Optional, Any, Callable, Union, Tuple
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import numpy as np
from scipy import stats
from geopy.distance import geodesic
import psutil
import GPUtil
from memory_profiler import memory_usage
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

logger = logging.getLogger(__name__)


class Timer:
    """Context manager for timing code execution."""
    
    def __init__(self, name: str = None, logger: logging.Logger = None):
        self.name = name
        self.logger = logger or logging.getLogger(__name__)
        self.start_time = None
        self.elapsed = None
    
    def __enter__(self):
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.elapsed = time.perf_counter() - self.start_time
        
        if self.name:
            self.logger.info(f"{self.name} took {self.elapsed:.4f} seconds")
        else:
            self.logger.info(f"Execution took {self.elapsed:.4f} seconds")
    
    def get_elapsed(self) -> float:
        """Get elapsed time in seconds."""
        if self.elapsed is None:
            return time.perf_counter() - self.start_time
        return self.elapsed


def setup_logging(
    log_level: str = "INFO",
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    log_file: Optional[str] = None,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Setup logging configuration.
    
    Args:
        log_level: Logging level
        log_format: Log format string
        log_file: Optional log file path
        max_bytes: Maximum log file size
        backup_count: Number of backup files to keep
        
    Returns:
        Configured logger
    """
    # Convert string level to logging level
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Add file handler if log_file is specified
    if log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Suppress noisy libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('matplotlib').setLevel(logging.WARNING)
    
    logger.info(f"Logging configured with level: {log_level}")
    return root_logger


def format_bytes(size_bytes: int) -> str:
    """Format bytes to human readable string."""
    if size_bytes == 0:
        return "0B"
    
    size_names = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    
    return f"{s} {size_names[i]}"


def format_timedelta(seconds: float) -> str:
    """Format seconds to human readable time string."""
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    parts = []
    if hours > 0:
        parts.append(f"{int(hours)}h")
    if minutes > 0:
        parts.append(f"{int(minutes)}m")
    if seconds > 0 or not parts:
        parts.append(f"{seconds:.1f}s")
    
    return " ".join(parts)


def sanitize_filename(filename: str) -> str:
    """Sanitize filename by removing invalid characters."""
    # Replace invalid characters with underscore
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove leading/trailing whitespace and dots
    sanitized = sanitized.strip('. ')
    # Limit length
    return sanitized[:255]


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
):
    """
    Retry decorator with exponential backoff.
    
    Args:
        max_attempts: Maximum number of attempts
        delay: Initial delay between attempts
        backoff: Backoff multiplier
        exceptions: Exceptions to catch and retry
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_attempts} failed for {func.__name__}: {e}. "
                            f"Retrying in {current_delay:.1f} seconds..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"All {max_attempts} attempts failed for {func.__name__}"
                        )
            
            raise last_exception
        
        return wrapper
    return decorator


def exponential_backoff(
    func: Callable,
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: tuple = (Exception,)
):
    """
    Execute function with exponential backoff retry logic.
    
    Args:
        func: Function to execute
        max_retries: Maximum number of retries
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        exceptions: Exceptions to catch and retry
        
    Returns:
        Function result
    """
    last_exception = None
    delay = base_delay
    
    for attempt in range(max_retries + 1):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            
            if attempt < max_retries:
                jitter = random.uniform(0.5, 1.5)
                actual_delay = min(delay * jitter, max_delay)
                
                logger.warning(
                    f"Attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {actual_delay:.1f} seconds..."
                )
                
                time.sleep(actual_delay)
                delay *= 2
            else:
                logger.error(f"All {max_retries} attempts failed")
                raise last_exception


def generate_id(prefix: str = "", length: int = 8) -> str:
    """Generate a unique ID."""
    chars = string.ascii_letters + string.digits
    random_part = ''.join(random.choices(chars, k=length))
    
    if prefix:
        return f"{prefix}_{random_part}"
    return random_part


def validate_email(email: str) -> bool:
    """Validate email address format."""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def validate_phone(phone: str) -> bool:
    """Validate phone number format (international)."""
    pattern = r'^\+?[1-9]\d{1,14}$'
    return bool(re.match(pattern, phone.replace(' ', '')))


def calculate_distance(
    lat1: float, 
    lon1: float, 
    lat2: float, 
    lon2: float,
    unit: str = 'km'
) -> float:
    """
    Calculate distance between two coordinates using geodesic distance.
    
    Args:
        lat1, lon1: First coordinate
        lat2, lon2: Second coordinate
        unit: Output unit ('km', 'm', 'mi')
        
    Returns:
        Distance in specified unit
    """
    distance = geodesic((lat1, lon1), (lat2, lon2))
    
    if unit == 'km':
        return distance.kilometers
    elif unit == 'm':
        return distance.meters
    elif unit == 'mi':
        return distance.miles
    else:
        raise ValueError(f"Unsupported unit: {unit}")


def calculate_fuel_efficiency(
    distance_km: float, 
    fuel_liters: float,
    efficiency_unit: str = 'kmpl'
) -> float:
    """
    Calculate fuel efficiency.
    
    Args:
        distance_km: Distance traveled in kilometers
        fuel_liters: Fuel consumed in liters
        efficiency_unit: Unit of efficiency ('kmpl', 'lpk', 'mpg')
        
    Returns:
        Fuel efficiency
    """
    if fuel_liters <= 0:
        return 0.0
    
    if efficiency_unit == 'kmpl':
        return distance_km / fuel_liters
    elif efficiency_unit == 'lpk':  # liters per 100km
        return (fuel_liters / distance_km) * 100 if distance_km > 0 else 0
    elif efficiency_unit == 'mpg':  # miles per gallon
        # Convert km to miles and liters to gallons
        distance_miles = distance_km * 0.621371
        fuel_gallons = fuel_liters * 0.264172
        return distance_miles / fuel_gallons if fuel_gallons > 0 else 0
    else:
        raise ValueError(f"Unsupported efficiency unit: {efficiency_unit}")


def detect_anomalies(
    data: np.ndarray,
    method: str = 'zscore',
    threshold: float = 3.0
) -> np.ndarray:
    """
    Detect anomalies in data using statistical methods.
    
    Args:
        data: Input data array
        method: Detection method ('zscore', 'iqr', 'modified_zscore')
        threshold: Threshold for anomaly detection
        
    Returns:
        Boolean array indicating anomalies
    """
    if method == 'zscore':
        z_scores = np.abs(stats.zscore(data, nan_policy='omit'))
        return z_scores > threshold
    
    elif method == 'iqr':
        q1 = np.percentile(data, 25)
        q3 = np.percentile(data, 75)
        iqr = q3 - q1
        lower_bound = q1 - (threshold * iqr)
        upper_bound = q3 + (threshold * iqr)
        return (data < lower_bound) | (data > upper_bound)
    
    elif method == 'modified_zscore':
        median = np.median(data)
        mad = np.median(np.abs(data - median))
        if mad == 0:
            mad = 1.4826  # Constant for normal distribution
        modified_z_scores = 0.6745 * (data - median) / mad
        return np.abs(modified_z_scores) > threshold
    
    else:
        raise ValueError(f"Unsupported anomaly detection method: {method}")


def normalize_data(
    data: np.ndarray,
    method: str = 'minmax',
    feature_range: tuple = (0, 1)
) -> np.ndarray:
    """
    Normalize data using specified method.
    
    Args:
        data: Input data array
        method: Normalization method ('minmax', 'standard', 'robust')
        feature_range: Desired range for minmax scaling
        
    Returns:
        Normalized data
    """
    if method == 'minmax':
        data_min = np.nanmin(data)
        data_max = np.nanmax(data)
        
        if data_max - data_min == 0:
            return np.zeros_like(data)
        
        normalized = (data - data_min) / (data_max - data_min)
        min_val, max_val = feature_range
        return normalized * (max_val - min_val) + min_val
    
    elif method == 'standard':
        mean = np.nanmean(data)
        std = np.nanstd(data)
        
        if std == 0:
            return np.zeros_like(data)
        
        return (data - mean) / std
    
    elif method == 'robust':
        median = np.median(data)
        q1 = np.percentile(data, 25)
        q3 = np.percentile(data, 75)
        iqr = q3 - q1
        
        if iqr == 0:
            return np.zeros_like(data)
        
        return (data - median) / iqr
    
    else:
        raise ValueError(f"Unsupported normalization method: {method}")


class MemoryProfiler:
    """Memory usage profiler."""
    
    def __init__(self):
        self.start_memory = None
        self.max_memory = None
    
    def __enter__(self):
        self.start_memory = psutil.Process().memory_info().rss
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        current_memory = psutil.Process().memory_info().rss
        self.max_memory = current_memory - self.start_memory
        
        logger.info(f"Memory usage: {format_bytes(self.max_memory)}")
    
    def get_memory_usage(self) -> int:
        """Get current memory usage in bytes."""
        return psutil.Process().memory_info().rss
    
    def get_gpu_memory_usage(self) -> Optional[Dict]:
        """Get GPU memory usage if available."""
        try:
            gpus = GPUtil.getGPUs()
            if not gpus:
                return None
            
            gpu_info = {}
            for i, gpu in enumerate(gpus):
                gpu_info[f'gpu_{i}'] = {
                    'name': gpu.name,
                    'memory_total': gpu.memoryTotal,
                    'memory_used': gpu.memoryUsed,
                    'memory_free': gpu.memoryFree,
                    'load': gpu.load
                }
            
            return gpu_info
        except Exception as e:
            logger.debug(f"Failed to get GPU memory info: {e}")
            return None


class PerformanceMonitor:
    """Performance monitoring utility."""
    
    def __init__(self):
        self.metrics = {}
        self.start_time = None
    
    def start(self, operation_name: str = None):
        """Start performance monitoring."""
        self.start_time = time.perf_counter()
        if operation_name:
            self.metrics[operation_name] = {
                'start_time': self.start_time,
                'end_time': None,
                'duration': None
            }
    
    def stop(self, operation_name: str = None):
        """Stop performance monitoring."""
        end_time = time.perf_counter()
        
        if operation_name and operation_name in self.metrics:
            self.metrics[operation_name]['end_time'] = end_time
            self.metrics[operation_name]['duration'] = (
                end_time - self.metrics[operation_name]['start_time']
            )
        
        return end_time - self.start_time if self.start_time else 0
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get all performance metrics."""
        metrics = {}
        
        for op_name, op_metrics in self.metrics.items():
            if op_metrics['duration']:
                metrics[op_name] = {
                    'duration_seconds': op_metrics['duration'],
                    'duration_formatted': format_timedelta(op_metrics['duration'])
                }
        
        # Add system metrics
        metrics['system'] = self.get_system_metrics()
        
        return metrics
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get system performance metrics."""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                'cpu_percent': cpu_percent,
                'memory_total_gb': memory.total / (1024**3),
                'memory_used_gb': memory.used / (1024**3),
                'memory_percent': memory.percent,
                'disk_total_gb': disk.total / (1024**3),
                'disk_used_gb': disk.used / (1024**3),
                'disk_percent': disk.percent,
                'process_count': len(psutil.pids())
            }
        except Exception as e:
            logger.error(f"Failed to get system metrics: {e}")
            return {}
    
    def generate_report(self) -> str:
        """Generate performance report."""
        metrics = self.get_metrics()
        
        report_lines = ["Performance Report", "=" * 50]
        
        for section, section_metrics in metrics.items():
            if section == 'system':
                continue
            
            report_lines.append(f"\n{section}:")
            for key, value in section_metrics.items():
                report_lines.append(f"  {key}: {value}")
        
        report_lines.append("\nSystem Metrics:")
        system_metrics = metrics.get('system', {})
        for key, value in system_metrics.items():
            if 'gb' in key:
                report_lines.append(f"  {key}: {value:.2f}")
            elif 'percent' in key:
                report_lines.append(f"  {key}: {value:.1f}%")
            else:
                report_lines.append(f"  {key}: {value}")
        
        return "\n".join(report_lines)


# Thread-safe singleton decorator
def singleton(cls):
    """Decorator to make a class a thread-safe singleton."""
    instances = {}
    lock = threading.Lock()
    
    @functools.wraps(cls)
    def get_instance(*args, **kwargs):
        with lock:
            if cls not in instances:
                instances[cls] = cls(*args, **kwargs)
            return instances[cls]
    
    return get_instance


# Async utilities
async def run_in_threadpool(func: Callable, *args, **kwargs) -> Any:
    """Run synchronous function in thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))


async def run_in_processpool(func: Callable, *args, **kwargs) -> Any:
    """Run function in process pool for CPU-bound operations."""
    loop = asyncio.get_event_loop()
    with ProcessPoolExecutor() as executor:
        return await loop.run_in_executor(
            executor, functools.partial(func, *args, **kwargs)
        )


# Data validation utilities
def validate_dataframe(df: pd.DataFrame, required_columns: List[str] = None) -> List[str]:
    """
    Validate DataFrame structure and data.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        List of validation errors
    """
    errors = []
    
    if df.empty:
        errors.append("DataFrame is empty")
        return errors
    
    # Check required columns
    if required_columns:
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
    
    # Check for NaN values in critical columns
    critical_columns = ['timestamp', 'vehicle_id'] if 'timestamp' in df.columns and 'vehicle_id' in df.columns else []
    for col in critical_columns:
        if col in df.columns and df[col].isna().any():
            errors.append(f"Column '{col}' contains NaN values")
    
    # Check data types
    if 'timestamp' in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            errors.append("Column 'timestamp' must be datetime type")
    
    return errors


# Batch processing utilities
def chunk_iterator(iterable, chunk_size: int = 1000):
    """Split iterable into chunks of specified size."""
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, chunk_size))
        if not chunk:
            break
        yield chunk


def process_in_batches(
    data: List[Any],
    process_func: Callable,
    batch_size: int = 1000,
    max_workers: int = None,
    use_multiprocessing: bool = False
) -> List[Any]:
    """
    Process data in batches, optionally using parallel processing.
    
    Args:
        data: Data to process
        process_func: Processing function
        batch_size: Size of each batch
        max_workers: Maximum number of workers
        use_multiprocessing: Whether to use multiprocessing
        
    Returns:
        Processed results
    """
    if max_workers is None:
        max_workers = multiprocessing.cpu_count()
    
    results = []
    
    # Split data into batches
    batches = [data[i:i + batch_size] for i in range(0, len(data), batch_size)]
    
    if use_multiprocessing and len(batches) > 1:
        # Use multiprocessing for CPU-bound tasks
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_func, batch) for batch in batches]
            for future in futures:
                results.extend(future.result())
    else:
        # Use threading for I/O-bound tasks
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_func, batch) for batch in batches]
            for future in futures:
                results.extend(future.result())
    
    return results
"""
Configuration management for Vehicle Telemetry Analytics Platform.
Supports YAML, JSON, environment variables, and secrets management.
"""
import os
import json
import yaml
import logging
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field, asdict
from pathlib import Path
from enum import Enum
import hvac
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import boto3
from google.cloud import secretmanager
import azure.keyvault.secrets

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


class ConfigSource(Enum):
    """Configuration source types."""
    ENV = "environment"
    YAML = "yaml"
    JSON = "json"
    VAULT = "vault"
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"


@dataclass
class DatabaseConfig:
    """Database configuration."""
    enabled: bool = True
    host: str = "localhost"
    port: int = 5432
    database: str = "vehicle_analytics"
    user: str = "postgres"
    password: str = "password"
    min_connections: int = 2
    max_connections: int = 20
    pool_recycle: int = 3600
    echo_sql: bool = False
    ssl_mode: str = "prefer"
    timeout: int = 30
    
    def get_connection_string(self) -> str:
        """Get SQLAlchemy connection string."""
        return (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
            f"?sslmode={self.ssl_mode}"
        )


@dataclass
class RedisConfig:
    """Redis configuration."""
    enabled: bool = True
    host: str = "localhost"
    port: int = 6379
    database: int = 0
    password: Optional[str] = None
    max_connections: int = 10
    socket_timeout: int = 5
    socket_connect_timeout: int = 5
    retry_on_timeout: bool = True
    health_check_interval: int = 30
    
    def get_connection_url(self) -> str:
        """Get Redis connection URL."""
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.database}"
        return f"redis://{self.host}:{self.port}/{self.database}"


@dataclass
class KafkaConfig:
    """Kafka configuration."""
    enabled: bool = True
    bootstrap_servers: List[str] = field(default_factory=lambda: ["localhost:9092"])
    topic_prefix: str = "vehicle-telemetry"
    consumer_group: str = "vehicle-analytics-group"
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    session_timeout_ms: int = 10000
    max_poll_records: int = 500
    fetch_max_bytes: int = 52428800
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    
    def get_consumer_config(self) -> Dict[str, Any]:
        """Get Kafka consumer configuration."""
        config = {
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': self.consumer_group,
            'auto_offset_reset': self.auto_offset_reset,
            'enable_auto_commit': self.enable_auto_commit,
            'auto_commit_interval_ms': self.auto_commit_interval_ms,
            'session_timeout_ms': self.session_timeout_ms,
            'max_poll_records': self.max_poll_records,
            'fetch_max_bytes': self.fetch_max_bytes
        }
        
        # Add security configuration if provided
        if self.security_protocol != "PLAINTEXT":
            config['security_protocol'] = self.security_protocol
            
            if self.sasl_mechanism:
                config['sasl_mechanism'] = self.sasl_mechanism
                config['sasl_plain_username'] = self.sasl_username
                config['sasl_plain_password'] = self.sasl_password
            
            if self.ssl_cafile:
                config['ssl_cafile'] = self.ssl_cafile
                config['ssl_certfile'] = self.ssl_certfile
                config['ssl_keyfile'] = self.ssl_keyfile
        
        return config


@dataclass
class SparkConfig:
    """Spark configuration."""
    enabled: bool = True
    master: str = "local[*]"
    app_name: str = "VehicleTelemetryAnalytics"
    executor_memory: str = "2g"
    driver_memory: str = "1g"
    executor_cores: int = 2
    driver_cores: int = 1
    shuffle_partitions: int = 200
    sql_adaptive_enabled: bool = True
    sql_adaptive_coalesce_partitions_enabled: bool = True
    sql_auto_broadcast_threshold: int = -1
    sql_shuffle_partitions: int = 200
    streaming_batch_duration: int = 10
    checkpoint_location: str = "/tmp/spark-checkpoints"
    
    def get_spark_config(self) -> Dict[str, str]:
        """Get Spark configuration dictionary."""
        return {
            'spark.master': self.master,
            'spark.app.name': self.app_name,
            'spark.executor.memory': self.executor_memory,
            'spark.driver.memory': self.driver_memory,
            'spark.executor.cores': str(self.executor_cores),
            'spark.driver.cores': str(self.driver_cores),
            'spark.sql.shuffle.partitions': str(self.shuffle_partitions),
            'spark.sql.adaptive.enabled': str(self.sql_adaptive_enabled).lower(),
            'spark.sql.adaptive.coalescePartitions.enabled': 
                str(self.sql_adaptive_coalesce_partitions_enabled).lower(),
            'spark.sql.autoBroadcastJoinThreshold': str(self.sql_auto_broadcast_threshold),
            'spark.sql.streaming.checkpointLocation': self.checkpoint_location
        }


@dataclass
class MonitoringConfig:
    """Monitoring configuration."""
    enabled: bool = True
    prometheus_enabled: bool = True
    grafana_enabled: bool = True
    metrics_port: int = 8000
    health_check_interval: int = 30
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_file: Optional[str] = "/var/log/vehicle-analytics/app.log"
    max_log_size_mb: int = 100
    backup_count: int = 5
    enable_structured_logging: bool = True
    enable_metrics_export: bool = True
    metrics_prefix: str = "vehicle_analytics"


@dataclass
class StorageConfig:
    """Storage configuration."""
    enabled: bool = True
    storage_type: str = "local"  # local, s3, gcs, azure
    local_path: str = "./data"
    s3_bucket: Optional[str] = None
    s3_prefix: Optional[str] = "vehicle-telemetry/"
    gcs_bucket: Optional[str] = None
    azure_container: Optional[str] = None
    retention_days: int = 90
    compression: bool = True
    encryption: bool = False
    encryption_key: Optional[str] = None
    
    def get_storage_path(self, file_type: str, timestamp: Optional[str] = None) -> str:
        """Get storage path for file type."""
        import datetime
        
        if timestamp is None:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        date_str = datetime.datetime.now().strftime("%Y/%m/%d")
        
        if self.storage_type == "local":
            path = f"{self.local_path}/{file_type}/{date_str}/{timestamp}"
            Path(path).mkdir(parents=True, exist_ok=True)
            return path
        elif self.storage_type == "s3":
            return f"s3://{self.s3_bucket}/{self.s3_prefix}/{file_type}/{date_str}/{timestamp}"
        elif self.storage_type == "gcs":
            return f"gs://{self.gcs_bucket}/{file_type}/{date_str}/{timestamp}"
        elif self.storage_type == "azure":
            return f"https://{self.azure_container}.blob.core.windows.net/{file_type}/{date_str}/{timestamp}"
        else:
            raise ValueError(f"Unsupported storage type: {self.storage_type}")


@dataclass
class AppConfig:
    """Main application configuration."""
    app_name: str = "Vehicle Telemetry Analytics"
    version: str = "1.0.0"
    environment: str = "development"
    debug: bool = False
    host: str = "0.0.0.0"
    port: int = 8000
    workers: int = 4
    api_prefix: str = "/api/v1"
    cors_origins: List[str] = field(default_factory=lambda: ["*"])
    rate_limit_enabled: bool = True
    rate_limit_requests: int = 100
    rate_limit_period: int = 60
    auth_enabled: bool = False
    jwt_secret: Optional[str] = None
    jwt_expiry_hours: int = 24
    api_keys: List[str] = field(default_factory=list)
    enable_docs: bool = True
    enable_metrics: bool = True
    enable_health: bool = True
    
    # Component configurations
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    
    # Feature flags
    features: Dict[str, bool] = field(default_factory=lambda: {
        'real_time_processing': True,
        'batch_processing': True,
        'predictive_maintenance': True,
        'driver_behavior_analysis': True,
        'fuel_efficiency_analysis': True,
        'fleet_optimization': True,
        'anomaly_detection': True,
        'geospatial_analysis': True
    })
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return asdict(self)
    
    def validate(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors = []
        
        # Validate environment
        if self.environment not in ['development', 'staging', 'production']:
            errors.append(f"Invalid environment: {self.environment}")
        
        # Validate port
        if not (1 <= self.port <= 65535):
            errors.append(f"Invalid port: {self.port}")
        
        # Validate JWT secret if auth enabled
        if self.auth_enabled and not self.jwt_secret:
            errors.append("JWT secret is required when authentication is enabled")
        
        # Validate feature flags
        for feature, enabled in self.features.items():
            if not isinstance(enabled, bool):
                errors.append(f"Feature flag '{feature}' must be boolean")
        
        return errors


class ConfigManager:
    """
    Configuration manager with support for multiple sources and secrets management.
    """
    
    def __init__(self, config_source: ConfigSource = ConfigSource.ENV, 
                 config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_source: Source of configuration
            config_path: Path to configuration file (if using file source)
        """
        self.config_source = config_source
        self.config_path = config_path
        self.config = AppConfig()
        self.secrets_manager = None
        self._load_configuration()
        self._setup_secrets_manager()
    
    def _load_configuration(self):
        """Load configuration from specified source."""
        try:
            if self.config_source == ConfigSource.ENV:
                self._load_from_env()
            elif self.config_source == ConfigSource.YAML:
                self._load_from_yaml()
            elif self.config_source == ConfigSource.JSON:
                self._load_from_json()
            elif self.config_source == ConfigSource.VAULT:
                self._load_from_vault()
            elif self.config_source == ConfigSource.AWS:
                self._load_from_aws()
            elif self.config_source == ConfigSource.GCP:
                self._load_from_gcp()
            elif self.config_source == ConfigSource.AZURE:
                self._load_from_azure()
            else:
                raise ValueError(f"Unsupported config source: {self.config_source}")
            
            # Validate configuration
            errors = self.config.validate()
            if errors:
                logger.warning(f"Configuration validation errors: {errors}")
            
            logger.info(f"Configuration loaded from {self.config_source.value}")
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _load_from_env(self):
        """Load configuration from environment variables."""
        # Application settings
        self.config.app_name = os.getenv('APP_NAME', self.config.app_name)
        self.config.version = os.getenv('APP_VERSION', self.config.version)
        self.config.environment = os.getenv('ENVIRONMENT', self.config.environment)
        self.config.debug = os.getenv('DEBUG', 'False').lower() == 'true'
        self.config.host = os.getenv('HOST', self.config.host)
        self.config.port = int(os.getenv('PORT', self.config.port))
        
        # Database settings
        self.config.database.host = os.getenv('DB_HOST', self.config.database.host)
        self.config.database.port = int(os.getenv('DB_PORT', self.config.database.port))
        self.config.database.database = os.getenv('DB_NAME', self.config.database.database)
        self.config.database.user = os.getenv('DB_USER', self.config.database.user)
        self.config.database.password = os.getenv('DB_PASSWORD', self.config.database.password)
        
        # Redis settings
        self.config.redis.host = os.getenv('REDIS_HOST', self.config.redis.host)
        self.config.redis.port = int(os.getenv('REDIS_PORT', self.config.redis.port))
        self.config.redis.password = os.getenv('REDIS_PASSWORD')
        
        # Kafka settings
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        if bootstrap_servers:
            self.config.kafka.bootstrap_servers = bootstrap_servers.split(',')
        
        # Feature flags
        for feature in self.config.features.keys():
            env_var = f"FEATURE_{feature.upper()}"
            value = os.getenv(env_var)
            if value is not None:
                self.config.features[feature] = value.lower() == 'true'
    
    def _load_from_yaml(self):
        """Load configuration from YAML file."""
        if not self.config_path:
            raise ValueError("Config path is required for YAML source")
        
        with open(self.config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        self._update_config_from_dict(config_data)
    
    def _load_from_json(self):
        """Load configuration from JSON file."""
        if not self.config_path:
            raise ValueError("Config path is required for JSON source")
        
        with open(self.config_path, 'r') as f:
            config_data = json.load(f)
        
        self._update_config_from_dict(config_data)
    
    def _load_from_vault(self):
        """Load configuration from HashiCorp Vault."""
        vault_addr = os.getenv('VAULT_ADDR')
        vault_token = os.getenv('VAULT_TOKEN')
        
        if not vault_addr or not vault_token:
            raise ValueError("VAULT_ADDR and VAULT_TOKEN must be set for Vault config source")
        
        client = hvac.Client(url=vault_addr, token=vault_token)
        
        # Read configuration from Vault
        secret_path = os.getenv('VAULT_SECRET_PATH', 'secret/vehicle-analytics/config')
        secret_response = client.secrets.kv.v2.read_secret_version(
            path=secret_path
        )
        
        config_data = secret_response['data']['data']
        self._update_config_from_dict(config_data)
    
    def _load_from_aws(self):
        """Load configuration from AWS Secrets Manager."""
        secret_id = os.getenv('AWS_SECRET_ID')
        region = os.getenv('AWS_REGION', 'us-east-1')
        
        if not secret_id:
            raise ValueError("AWS_SECRET_ID must be set for AWS config source")
        
        client = boto3.client('secretsmanager', region_name=region)
        response = client.get_secret_value(SecretId=secret_id)
        
        if 'SecretString' in response:
            config_data = json.loads(response['SecretString'])
        else:
            config_data = json.loads(response['SecretBinary'])
        
        self._update_config_from_dict(config_data)
    
    def _load_from_gcp(self):
        """Load configuration from Google Cloud Secret Manager."""
        project_id = os.getenv('GCP_PROJECT_ID')
        secret_id = os.getenv('GCP_SECRET_ID')
        
        if not project_id or not secret_id:
            raise ValueError("GCP_PROJECT_ID and GCP_SECRET_ID must be set for GCP config source")
        
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        
        response = client.access_secret_version(request={"name": name})
        config_data = json.loads(response.payload.data.decode("UTF-8"))
        
        self._update_config_from_dict(config_data)
    
    def _load_from_azure(self):
        """Load configuration from Azure Key Vault."""
        vault_url = os.getenv('AZURE_VAULT_URL')
        secret_name = os.getenv('AZURE_SECRET_NAME')
        
        if not vault_url or not secret_name:
            raise ValueError("AZURE_VAULT_URL and AZURE_SECRET_NAME must be set for Azure config source")
        
        credential = azure.identity.DefaultAzureCredential()
        client = azure.keyvault.secrets.SecretClient(
            vault_url=vault_url,
            credential=credential
        )
        
        secret = client.get_secret(secret_name)
        config_data = json.loads(secret.value)
        
        self._update_config_from_dict(config_data)
    
    def _update_config_from_dict(self, config_dict: Dict[str, Any]):
        """Update configuration from dictionary."""
        # Update top-level configuration
        for key, value in config_dict.items():
            if hasattr(self.config, key):
                if isinstance(getattr(self.config, key), dict) and isinstance(value, dict):
                    # Merge dictionaries
                    getattr(self.config, key).update(value)
                else:
                    setattr(self.config, key, value)
        
        # Update nested configurations
        if 'database' in config_dict and isinstance(config_dict['database'], dict):
            for key, value in config_dict['database'].items():
                if hasattr(self.config.database, key):
                    setattr(self.config.database, key, value)
        
        if 'redis' in config_dict and isinstance(config_dict['redis'], dict):
            for key, value in config_dict['redis'].items():
                if hasattr(self.config.redis, key):
                    setattr(self.config.redis, key, value)
        
        if 'kafka' in config_dict and isinstance(config_dict['kafka'], dict):
            for key, value in config_dict['kafka'].items():
                if hasattr(self.config.kafka, key):
                    setattr(self.config.kafka, key, value)
    
    def _setup_secrets_manager(self):
        """Setup secrets manager for encrypted values."""
        encryption_key = os.getenv('CONFIG_ENCRYPTION_KEY')
        if encryption_key:
            try:
                self.secrets_manager = Fernet(encryption_key.encode())
            except Exception as e:
                logger.warning(f"Failed to setup encryption: {e}")
    
    def get_secret(self, key: str, encrypted: bool = False) -> Optional[str]:
        """
        Get secret value, optionally decrypting it.
        
        Args:
            key: Secret key
            encrypted: Whether the value is encrypted
            
        Returns:
            Decrypted secret value or None
        """
        value = os.getenv(key)
        
        if value and encrypted and self.secrets_manager:
            try:
                return self.secrets_manager.decrypt(value.encode()).decode()
            except Exception as e:
                logger.error(f"Failed to decrypt secret {key}: {e}")
                return None
        
        return value
    
    def save_config(self, output_path: str, format: str = 'yaml'):
        """
        Save current configuration to file.
        
        Args:
            output_path: Output file path
            format: Output format ('yaml' or 'json')
        """
        config_dict = self.config.to_dict()
        
        if format.lower() == 'yaml':
            with open(output_path, 'w') as f:
                yaml.dump(config_dict, f, default_flow_style=False)
        elif format.lower() == 'json':
            with open(output_path, 'w') as f:
                json.dump(config_dict, f, indent=2)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"Configuration saved to {output_path}")
    
    def reload(self):
        """Reload configuration from source."""
        self._load_configuration()


# Global configuration instance
_config_instance: Optional[ConfigManager] = None


def get_config() -> AppConfig:
    """
    Get the global application configuration.
    
    Returns:
        AppConfig instance
    """
    global _config_instance
    if _config_instance is None:
        # Default to environment variables
        _config_instance = ConfigManager(ConfigSource.ENV)
    return _config_instance.config


def load_config(config_source: ConfigSource = ConfigSource.ENV, 
                config_path: Optional[str] = None) -> AppConfig:
    """
    Load configuration and return AppConfig.
    
    Args:
        config_source: Configuration source
        config_path: Path to configuration file
        
    Returns:
        AppConfig instance
    """
    global _config_instance
    _config_instance = ConfigManager(config_source, config_path)
    return _config_instance.config


def validate_config(config: AppConfig) -> bool:
    """
    Validate configuration.
    
    Args:
        config: Configuration to validate
        
    Returns:
        True if configuration is valid
    """
    errors = config.validate()
    if errors:
        logger.error(f"Configuration validation failed: {errors}")
        return False
    return True
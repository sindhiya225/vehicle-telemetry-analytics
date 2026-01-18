-- Vehicle Database Schema

-- Main vehicle information table
CREATE TABLE IF NOT EXISTS vehicles (
    vehid INT PRIMARY KEY,
    vehicle_type VARCHAR(10) NOT NULL,
    vehicle_class VARCHAR(50),
    engine_configuration TEXT,
    transmission VARCHAR(100),
    drive_wheels VARCHAR(50),
    generalized_weight INT,
    engine_cylinders INT,
    engine_type VARCHAR(20),
    displacement_l DECIMAL(3,1),
    is_turbo BOOLEAN DEFAULT FALSE,
    is_hybrid BOOLEAN DEFAULT FALSE,
    power_index DECIMAL(5,2),
    transmission_type VARCHAR(20),
    transmission_speeds INT,
    weight_category VARCHAR(20),
    efficiency_score DECIMAL(5,2),
    efficiency_category VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Telemetry data table (partitioned by date for performance)
CREATE TABLE IF NOT EXISTS telemetry_data (
    telemetry_id SERIAL,
    timestamp TIMESTAMP NOT NULL,
    vehid INT NOT NULL,
    vehicle_type VARCHAR(10),
    vehicle_class VARCHAR(50),
    trip_id VARCHAR(100),
    trip_distance_km DECIMAL(6,2),
    trip_duration_min DECIMAL(6,2),
    fuel_consumed_l DECIMAL(6,2),
    fuel_efficiency_kmpl DECIMAL(6,2),
    avg_speed_kmph DECIMAL(5,2),
    avg_rpm INT,
    avg_engine_temp_c DECIMAL(4,1),
    idle_time_min DECIMAL(6,2),
    fault_codes TEXT,
    has_fault BOOLEAN DEFAULT FALSE,
    maintenance_flag BOOLEAN DEFAULT FALSE,
    weather_condition VARCHAR(20),
    road_type VARCHAR(20),
    driver_behavior_score DECIMAL(3,2),
    PRIMARY KEY (telemetry_id, timestamp),
    FOREIGN KEY (vehid) REFERENCES vehicles(vehid)
) PARTITION BY RANGE (timestamp);

-- Create monthly partitions (example)
CREATE TABLE telemetry_data_2024_01 PARTITION OF telemetry_data
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- Daily aggregations for fast reporting
CREATE TABLE IF NOT EXISTS daily_aggregations (
    aggregation_date DATE NOT NULL,
    vehid INT NOT NULL,
    total_distance_km DECIMAL(8,2),
    total_fuel_l DECIMAL(8,2),
    avg_efficiency_kmpl DECIMAL(6,2),
    total_idle_time_min DECIMAL(8,2),
    fault_count INT DEFAULT 0,
    maintenance_alerts INT DEFAULT 0,
    avg_driver_score DECIMAL(3,2),
    PRIMARY KEY (aggregation_date, vehid),
    FOREIGN KEY (vehid) REFERENCES vehicles(vehid)
);

-- Maintenance history table
CREATE TABLE IF NOT EXISTS maintenance_history (
    maintenance_id SERIAL PRIMARY KEY,
    vehid INT NOT NULL,
    maintenance_date DATE NOT NULL,
    maintenance_type VARCHAR(50) NOT NULL,
    description TEXT,
    cost DECIMAL(8,2),
    mileage_km INT,
    next_due_km INT,
    performed_by VARCHAR(100),
    FOREIGN KEY (vehid) REFERENCES vehicles(vehid)
);

-- Driver behavior analytics
CREATE TABLE IF NOT EXISTS driver_behavior (
    behavior_id SERIAL PRIMARY KEY,
    vehid INT NOT NULL,
    analysis_date DATE NOT NULL,
    harsh_acceleration_count INT DEFAULT 0,
    harsh_braking_count INT DEFAULT 0,
    speeding_count INT DEFAULT 0,
    night_driving_hours DECIMAL(5,2),
    idle_time_percentage DECIMAL(5,2),
    overall_score DECIMAL(3,2),
    recommendations TEXT,
    FOREIGN KEY (vehid) REFERENCES vehicles(vehid)
);

-- Fuel efficiency benchmarks
CREATE TABLE IF NOT EXISTS efficiency_benchmarks (
    benchmark_id SERIAL PRIMARY KEY,
    vehicle_class VARCHAR(50) NOT NULL,
    engine_type VARCHAR(20),
    displacement_range VARCHAR(20),
    urban_efficiency_kmpl DECIMAL(5,2),
    highway_efficiency_kmpl DECIMAL(5,2),
    mixed_efficiency_kmpl DECIMAL(5,2),
    last_updated DATE
);

-- Create indexes for performance
CREATE INDEX idx_telemetry_vehid_date ON telemetry_data(vehid, timestamp);
CREATE INDEX idx_telemetry_faults ON telemetry_data(has_fault, timestamp);
CREATE INDEX idx_daily_agg_date ON daily_aggregations(aggregation_date);
CREATE INDEX idx_maintenance_vehid ON maintenance_history(vehid, maintenance_date);
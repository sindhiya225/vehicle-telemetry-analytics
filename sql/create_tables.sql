-- Vehicle Telemetry Analytics Database Schema
-- PostgreSQL compatible

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "timescaledb" CASCADE;

-- Main vehicle information table
CREATE TABLE IF NOT EXISTS vehicles (
    vehid SERIAL PRIMARY KEY,
    vehicle_type VARCHAR(10) NOT NULL CHECK (vehicle_type IN ('ICE', 'HEV', 'PHEV', 'BEV')),
    vehicle_class VARCHAR(50) NOT NULL,
    engine_configuration TEXT,
    transmission VARCHAR(100),
    drive_wheels VARCHAR(50),
    generalized_weight INTEGER,
    engine_cylinders INTEGER,
    engine_type VARCHAR(20),
    displacement_l NUMERIC(3,1),
    is_turbo BOOLEAN DEFAULT FALSE,
    is_hybrid BOOLEAN DEFAULT FALSE,
    power_index NUMERIC(5,2),
    transmission_type VARCHAR(20),
    transmission_speeds INTEGER,
    weight_category VARCHAR(20),
    efficiency_score NUMERIC(5,2),
    efficiency_category VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT check_displacement CHECK (displacement_l >= 0 AND displacement_l <= 10),
    CONSTRAINT check_weight CHECK (generalized_weight >= 1000 AND generalized_weight <= 10000),
    CONSTRAINT check_cylinders CHECK (engine_cylinders >= 0 AND engine_cylinders <= 16)
);

-- Create hypertable for telemetry data (for TimescaleDB)
CREATE TABLE IF NOT EXISTS telemetry_data (
    telemetry_id BIGSERIAL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    vehid INTEGER NOT NULL,
    vehicle_type VARCHAR(10),
    vehicle_class VARCHAR(50),
    trip_id VARCHAR(100),
    trip_distance_km NUMERIC(6,2) NOT NULL,
    trip_duration_min NUMERIC(6,2) NOT NULL,
    fuel_consumed_l NUMERIC(6,2),
    fuel_efficiency_kmpl NUMERIC(6,2),
    avg_speed_kmph NUMERIC(5,2),
    avg_rpm INTEGER,
    avg_engine_temp_c NUMERIC(4,1),
    idle_time_min NUMERIC(6,2),
    fault_codes TEXT[],
    has_fault BOOLEAN DEFAULT FALSE,
    maintenance_flag BOOLEAN DEFAULT FALSE,
    weather_condition VARCHAR(20),
    road_type VARCHAR(20),
    driver_behavior_score NUMERIC(3,2),
    location_lat NUMERIC(9,6),
    location_lon NUMERIC(9,6),
    altitude NUMERIC(6,1),
    harsh_acceleration_count INTEGER DEFAULT 0,
    harsh_braking_count INTEGER DEFAULT 0,
    speeding_count INTEGER DEFAULT 0,
    tire_pressure_front_left NUMERIC(3,1),
    tire_pressure_front_right NUMERIC(3,1),
    tire_pressure_rear_left NUMERIC(3,1),
    tire_pressure_rear_right NUMERIC(3,1),
    battery_voltage NUMERIC(4,1),
    battery_soc_percent NUMERIC(5,2),
    
    -- Constraints
    CONSTRAINT check_fuel_efficiency CHECK (fuel_efficiency_kmpl >= 0 AND fuel_efficiency_kmpl <= 100),
    CONSTRAINT check_speed CHECK (avg_speed_kmph >= 0 AND avg_speed_kmph <= 300),
    CONSTRAINT check_temperature CHECK (avg_engine_temp_c >= -20 AND avg_engine_temp_c <= 150),
    CONSTRAINT check_driver_score CHECK (driver_behavior_score >= 0 AND driver_behavior_score <= 1),
    
    -- Foreign key
    CONSTRAINT fk_vehicle FOREIGN KEY (vehid) REFERENCES vehicles(vehid) ON DELETE CASCADE,
    
    -- Composite primary key
    PRIMARY KEY (telemetry_id, timestamp)
);

-- Convert to hypertable if TimescaleDB is available
SELECT create_hypertable('telemetry_data', 'timestamp', if_not_exists => TRUE);

-- Daily aggregations for fast reporting
CREATE TABLE IF NOT EXISTS daily_aggregations (
    aggregation_date DATE NOT NULL,
    vehid INTEGER NOT NULL,
    vehicle_type VARCHAR(10),
    vehicle_class VARCHAR(50),
    total_distance_km NUMERIC(8,2) DEFAULT 0,
    total_fuel_l NUMERIC(8,2) DEFAULT 0,
    avg_efficiency_kmpl NUMERIC(6,2) DEFAULT 0,
    total_idle_time_min NUMERIC(8,2) DEFAULT 0,
    fault_count INTEGER DEFAULT 0,
    maintenance_alerts INTEGER DEFAULT 0,
    avg_driver_score NUMERIC(3,2) DEFAULT 0,
    harsh_acceleration_total INTEGER DEFAULT 0,
    harsh_braking_total INTEGER DEFAULT 0,
    speeding_total INTEGER DEFAULT 0,
    max_speed_kmph NUMERIC(5,2) DEFAULT 0,
    min_engine_temp_c NUMERIC(4,1) DEFAULT 0,
    max_engine_temp_c NUMERIC(4,1) DEFAULT 0,
    
    -- Primary key
    PRIMARY KEY (aggregation_date, vehid),
    
    -- Foreign key
    CONSTRAINT fk_vehicle_agg FOREIGN KEY (vehid) REFERENCES vehicles(vehid) ON DELETE CASCADE
);

-- Maintenance history table
CREATE TABLE IF NOT EXISTS maintenance_history (
    maintenance_id SERIAL PRIMARY KEY,
    vehid INTEGER NOT NULL,
    maintenance_date DATE NOT NULL,
    maintenance_type VARCHAR(50) NOT NULL,
    description TEXT,
    cost NUMERIC(8,2),
    mileage_km INTEGER,
    next_due_km INTEGER,
    performed_by VARCHAR(100),
    parts_replaced TEXT[],
    warranty_claimed BOOLEAN DEFAULT FALSE,
    
    -- Constraints
    CONSTRAINT check_cost CHECK (cost >= 0),
    CONSTRAINT check_mileage CHECK (mileage_km >= 0),
    
    -- Foreign key
    CONSTRAINT fk_vehicle_maintenance FOREIGN KEY (vehid) REFERENCES vehicles(vehid) ON DELETE CASCADE
);

-- Driver behavior analytics
CREATE TABLE IF NOT EXISTS driver_behavior (
    behavior_id SERIAL PRIMARY KEY,
    vehid INTEGER NOT NULL,
    driver_id VARCHAR(20),
    analysis_date DATE NOT NULL,
    total_trips_analyzed INTEGER DEFAULT 0,
    total_distance_km NUMERIC(8,2) DEFAULT 0,
    avg_driver_score NUMERIC(3,2) DEFAULT 0,
    safety_score NUMERIC(5,2) DEFAULT 0,
    efficiency_score NUMERIC(5,2) DEFAULT 0,
    overall_score NUMERIC(5,2) DEFAULT 0,
    driver_category VARCHAR(20),
    harsh_acceleration_count INTEGER DEFAULT 0,
    harsh_braking_count INTEGER DEFAULT 0,
    speeding_count INTEGER DEFAULT 0,
    night_driving_hours NUMERIC(5,2) DEFAULT 0,
    idle_time_percentage NUMERIC(5,2) DEFAULT 0,
    recommendations TEXT,
    co2_emissions_kg NUMERIC(8,2) DEFAULT 0,
    fuel_cost_usd NUMERIC(8,2) DEFAULT 0,
    insurance_premium_factor NUMERIC(4,3) DEFAULT 1.0,
    
    -- Constraints
    CONSTRAINT check_scores CHECK (overall_score >= 0 AND overall_score <= 100),
    
    -- Foreign key
    CONSTRAINT fk_vehicle_driver FOREIGN KEY (vehid) REFERENCES vehicles(vehid) ON DELETE CASCADE,
    
    -- Unique constraint
    UNIQUE (vehid, analysis_date)
);

-- Fuel efficiency benchmarks
CREATE TABLE IF NOT EXISTS efficiency_benchmarks (
    benchmark_id SERIAL PRIMARY KEY,
    vehicle_class VARCHAR(50) NOT NULL,
    engine_type VARCHAR(20),
    displacement_range VARCHAR(20),
    urban_efficiency_kmpl NUMERIC(5,2),
    highway_efficiency_kmpl NUMERIC(5,2),
    mixed_efficiency_kmpl NUMERIC(5,2),
    year INTEGER,
    last_updated DATE DEFAULT CURRENT_DATE,
    
    -- Unique constraint
    UNIQUE (vehicle_class, engine_type, displacement_range, year)
);

-- Cost analysis table
CREATE TABLE IF NOT EXISTS cost_analysis (
    analysis_id SERIAL PRIMARY KEY,
    vehid INTEGER NOT NULL,
    analysis_date DATE NOT NULL,
    fuel_cost NUMERIC(8,2) DEFAULT 0,
    maintenance_cost NUMERIC(8,2) DEFAULT 0,
    insurance_cost NUMERIC(8,2) DEFAULT 0,
    depreciation_cost NUMERIC(8,2) DEFAULT 0,
    total_cost NUMERIC(8,2) DEFAULT 0,
    cost_per_km NUMERIC(6,4) DEFAULT 0,
    potential_savings NUMERIC(8,2) DEFAULT 0,
    optimization_opportunities JSONB,
    
    -- Foreign key
    CONSTRAINT fk_vehicle_cost FOREIGN KEY (vehid) REFERENCES vehicles(vehid) ON DELETE CASCADE,
    
    -- Unique constraint
    UNIQUE (vehid, analysis_date)
);

-- Real-time alerts table
CREATE TABLE IF NOT EXISTS alerts (
    alert_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    vehid INTEGER NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL')),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    description TEXT NOT NULL,
    recommended_action TEXT,
    status VARCHAR(20) DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'ACKNOWLEDGED', 'RESOLVED')),
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMP WITH TIME ZONE,
    resolved_at TIMESTAMP WITH TIME ZONE,
    
    -- Foreign key
    CONSTRAINT fk_vehicle_alert FOREIGN KEY (vehid) REFERENCES vehicles(vehid) ON DELETE CASCADE
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_telemetry_vehid_timestamp ON telemetry_data(vehid, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp ON telemetry_data(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_faults ON telemetry_data(has_fault) WHERE has_fault = TRUE;
CREATE INDEX IF NOT EXISTS idx_telemetry_maintenance ON telemetry_data(maintenance_flag) WHERE maintenance_flag = TRUE;
CREATE INDEX IF NOT EXISTS idx_daily_agg_date ON daily_aggregations(aggregation_date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_agg_vehid ON daily_aggregations(vehid);
CREATE INDEX IF NOT EXISTS idx_maintenance_vehid ON maintenance_history(vehid, maintenance_date DESC);
CREATE INDEX IF NOT EXISTS idx_driver_behavior_date ON driver_behavior(analysis_date DESC);
CREATE INDEX IF NOT EXISTS idx_driver_behavior_score ON driver_behavior(overall_score DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status) WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity) WHERE severity IN ('WARNING', 'CRITICAL');

-- Create materialized views for performance

-- Materialized view for vehicle performance summary
CREATE MATERIALIZED VIEW IF NOT EXISTS vehicle_performance_summary AS
SELECT 
    v.vehid,
    v.vehicle_type,
    v.vehicle_class,
    v.engine_type,
    COUNT(t.telemetry_id) as total_trips,
    SUM(t.trip_distance_km) as total_distance_km,
    AVG(t.fuel_efficiency_kmpl) as avg_efficiency,
    SUM(CASE WHEN t.has_fault THEN 1 ELSE 0 END) as total_faults,
    SUM(CASE WHEN t.maintenance_flag THEN 1 ELSE 0 END) as maintenance_alerts,
    AVG(t.driver_behavior_score) as avg_driver_score,
    MAX(t.timestamp) as last_trip_date
FROM vehicles v
LEFT JOIN telemetry_data t ON v.vehid = t.vehid
WHERE t.timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY v.vehid, v.vehicle_type, v.vehicle_class, v.engine_type
WITH DATA;

-- Materialized view for fleet efficiency
CREATE MATERIALIZED VIEW IF NOT EXISTS fleet_efficiency_daily AS
SELECT 
    DATE(t.timestamp) as date,
    v.vehicle_type,
    COUNT(DISTINCT t.vehid) as active_vehicles,
    SUM(t.trip_distance_km) as total_distance_km,
    SUM(t.fuel_consumed_l) as total_fuel_l,
    AVG(t.fuel_efficiency_kmpl) as avg_efficiency_kmpl,
    SUM(CASE WHEN t.has_fault THEN 1 ELSE 0 END) as total_faults,
    AVG(t.driver_behavior_score) as avg_driver_score
FROM telemetry_data t
JOIN vehicles v ON t.vehid = v.vehid
WHERE t.timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(t.timestamp), v.vehicle_type
WITH DATA;

-- Create refresh functions for materialized views
CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY vehicle_performance_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY fleet_efficiency_daily;
END;
$$ LANGUAGE plpgsql;

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for updated_at
CREATE TRIGGER update_vehicles_updated_at
    BEFORE UPDATE ON vehicles
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create function for daily aggregation
CREATE OR REPLACE FUNCTION aggregate_daily_telemetry()
RETURNS void AS $$
BEGIN
    INSERT INTO daily_aggregations (
        aggregation_date,
        vehid,
        vehicle_type,
        vehicle_class,
        total_distance_km,
        total_fuel_l,
        avg_efficiency_kmpl,
        total_idle_time_min,
        fault_count,
        maintenance_alerts,
        avg_driver_score,
        harsh_acceleration_total,
        harsh_braking_total,
        speeding_total,
        max_speed_kmph,
        min_engine_temp_c,
        max_engine_temp_c
    )
    SELECT 
        DATE(t.timestamp) as aggregation_date,
        t.vehid,
        v.vehicle_type,
        v.vehicle_class,
        SUM(t.trip_distance_km) as total_distance_km,
        SUM(t.fuel_consumed_l) as total_fuel_l,
        AVG(t.fuel_efficiency_kmpl) as avg_efficiency_kmpl,
        SUM(t.idle_time_min) as total_idle_time_min,
        SUM(CASE WHEN t.has_fault THEN 1 ELSE 0 END) as fault_count,
        SUM(CASE WHEN t.maintenance_flag THEN 1 ELSE 0 END) as maintenance_alerts,
        AVG(t.driver_behavior_score) as avg_driver_score,
        SUM(t.harsh_acceleration_count) as harsh_acceleration_total,
        SUM(t.harsh_braking_count) as harsh_braking_total,
        SUM(t.speeding_count) as speeding_total,
        MAX(t.avg_speed_kmph) as max_speed_kmph,
        MIN(t.avg_engine_temp_c) as min_engine_temp_c,
        MAX(t.avg_engine_temp_c) as max_engine_temp_c
    FROM telemetry_data t
    JOIN vehicles v ON t.vehid = v.vehid
    WHERE DATE(t.timestamp) = CURRENT_DATE - INTERVAL '1 day'
    GROUP BY DATE(t.timestamp), t.vehid, v.vehicle_type, v.vehicle_class
    ON CONFLICT (aggregation_date, vehid) 
    DO UPDATE SET
        total_distance_km = EXCLUDED.total_distance_km,
        total_fuel_l = EXCLUDED.total_fuel_l,
        avg_efficiency_kmpl = EXCLUDED.avg_efficiency_kmpl,
        total_idle_time_min = EXCLUDED.total_idle_time_min,
        fault_count = EXCLUDED.fault_count,
        maintenance_alerts = EXCLUDED.maintenance_alerts,
        avg_driver_score = EXCLUDED.avg_driver_score,
        harsh_acceleration_total = EXCLUDED.harsh_acceleration_total,
        harsh_braking_total = EXCLUDED.harsh_braking_total,
        speeding_total = EXCLUDED.speeding_total,
        max_speed_kmph = EXCLUDED.max_speed_kmph,
        min_engine_temp_c = EXCLUDED.min_engine_temp_c,
        max_engine_temp_c = EXCLUDED.max_engine_temp_c;
END;
$$ LANGUAGE plpgsql;

-- Create function to detect anomalies
CREATE OR REPLACE FUNCTION detect_telemetry_anomalies()
RETURNS TABLE (
    vehid INTEGER,
    anomaly_type VARCHAR(50),
    severity VARCHAR(20),
    timestamp TIMESTAMP WITH TIME ZONE,
    description TEXT
) AS $$
BEGIN
    RETURN QUERY
    -- Detect overheating
    SELECT 
        t.vehid,
        'OVERHEATING'::VARCHAR as anomaly_type,
        CASE 
            WHEN t.avg_engine_temp_c > 115 THEN 'CRITICAL'
            WHEN t.avg_engine_temp_c > 110 THEN 'WARNING'
            ELSE 'INFO'
        END as severity,
        t.timestamp,
        CONCAT('Engine temperature: ', t.avg_engine_temp_c, '°C') as description
    FROM telemetry_data t
    WHERE t.avg_engine_temp_c > 110
    AND t.timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
    
    UNION ALL
    
    -- Detect low fuel efficiency
    SELECT 
        t.vehid,
        'LOW_EFFICIENCY'::VARCHAR as anomaly_type,
        CASE 
            WHEN t.fuel_efficiency_kmpl < 5 THEN 'CRITICAL'
            WHEN t.fuel_efficiency_kmpl < 8 THEN 'WARNING'
            ELSE 'INFO'
        END as severity,
        t.timestamp,
        CONCAT('Fuel efficiency: ', t.fuel_efficiency_kmpl, ' km/L') as description
    FROM telemetry_data t
    WHERE t.fuel_efficiency_kmpl < 8
    AND t.timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
    
    UNION ALL
    
    -- Detect excessive idling
    SELECT 
        t.vehid,
        'EXCESSIVE_IDLING'::VARCHAR as anomaly_type,
        CASE 
            WHEN t.idle_time_min > 30 THEN 'CRITICAL'
            WHEN t.idle_time_min > 15 THEN 'WARNING'
            ELSE 'INFO'
        END as severity,
        t.timestamp,
        CONCAT('Idle time: ', t.idle_time_min, ' minutes') as description
    FROM telemetry_data t
    WHERE t.idle_time_min > 15
    AND t.timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour';
END;
$$ LANGUAGE plpgsql;

-- Create view for maintenance prediction
CREATE OR REPLACE VIEW maintenance_prediction AS
SELECT 
    v.vehid,
    v.vehicle_type,
    v.vehicle_class,
    v.engine_type,
    v.odometer_km,
    v.last_service_km,
    v.service_interval_km,
    v.odometer_km - v.last_service_km as km_since_last_service,
    CASE 
        WHEN v.odometer_km - v.last_service_km >= v.service_interval_km THEN 'IMMEDIATE'
        WHEN v.odometer_km - v.last_service_km >= v.service_interval_km * 0.8 THEN 'SOON'
        ELSE 'OK'
    END as maintenance_urgency,
    COUNT(DISTINCT m.maintenance_id) as past_maintenance_count,
    MAX(m.maintenance_date) as last_maintenance_date,
    COUNT(DISTINCT t.telemetry_id) FILTER (WHERE t.has_fault = TRUE) as recent_faults
FROM vehicles v
LEFT JOIN maintenance_history m ON v.vehid = m.vehid
LEFT JOIN telemetry_data t ON v.vehid = t.vehid AND t.timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY v.vehid, v.vehicle_type, v.vehicle_class, v.engine_type, 
         v.odometer_km, v.last_service_km, v.service_interval_km;

-- Insert sample benchmark data
INSERT INTO efficiency_benchmarks (
    vehicle_class, engine_type, displacement_range, 
    urban_efficiency_kmpl, highway_efficiency_kmpl, mixed_efficiency_kmpl, year
) VALUES
    ('Compact', 'Gasoline', '1.0-1.5L', 14.5, 18.0, 16.0, 2023),
    ('Compact', 'Gasoline', '1.5-2.0L', 13.0, 17.0, 14.5, 2023),
    ('Compact', 'Hybrid', '1.5-2.0L', 20.0, 22.0, 21.0, 2023),
    ('Midsize', 'Gasoline', '2.0-2.5L', 11.5, 16.0, 13.5, 2023),
    ('Midsize', 'Gasoline', '2.5-3.0L', 10.5, 15.0, 12.5, 2023),
    ('Midsize', 'Hybrid', '2.0-2.5L', 18.0, 20.0, 19.0, 2023),
    ('SUV', 'Gasoline', '2.5-3.0L', 9.5, 13.0, 11.0, 2023),
    ('SUV', 'Gasoline', '3.0-3.5L', 8.5, 12.0, 10.0, 2023),
    ('SUV', 'Hybrid', '2.5-3.0L', 16.0, 18.0, 17.0, 2023),
    ('Truck', 'Gasoline', '3.5-4.0L', 7.5, 10.0, 8.5, 2023),
    ('Truck', 'Diesel', '3.0-3.5L', 9.0, 12.0, 10.5, 2023)
ON CONFLICT (vehicle_class, engine_type, displacement_range, year) 
DO NOTHING;

-- Create user and grant permissions (for production)
-- Note: Run these commands separately with admin privileges
/*
CREATE USER vehicle_app WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE vehicle_telemetry TO vehicle_app;
GRANT USAGE ON SCHEMA public TO vehicle_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO vehicle_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO vehicle_app;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO vehicle_app;
*/

COMMENT ON DATABASE vehicle_telemetry IS 'Vehicle Telemetry Analytics Database';
COMMENT ON TABLE vehicles IS 'Master vehicle information table';
COMMENT ON TABLE telemetry_data IS 'Time-series telemetry data from vehicles';
COMMENT ON TABLE daily_aggregations IS 'Daily aggregated telemetry data for fast reporting';
COMMENT ON TABLE maintenance_history IS 'Maintenance and service history';
COMMENT ON TABLE driver_behavior IS 'Driver behavior analytics and scoring';
COMMENT ON TABLE efficiency_benchmarks IS 'Fuel efficiency benchmarks by vehicle type';
COMMENT ON TABLE cost_analysis IS 'Cost analysis and optimization opportunities';
COMMENT ON TABLE alerts IS 'Real-time alerts and notifications';

-- Print success message
DO $$
BEGIN
    RAISE NOTICE 'Database schema created successfully!';
    RAISE NOTICE 'Tables created: vehicles, telemetry_data, daily_aggregations, maintenance_history, driver_behavior, efficiency_benchmarks, cost_analysis, alerts';
    RAISE NOTICE 'Views created: vehicle_performance_summary, fleet_efficiency_daily, maintenance_prediction';
    RAISE NOTICE 'Functions created: refresh_materialized_views, update_updated_at_column, aggregate_daily_telemetry, detect_telemetry_anomalies';
END $$;
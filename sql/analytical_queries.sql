-- Vehicle Telemetry Analytics - Analytical Queries
-- Advanced SQL queries for business intelligence and reporting

-- ============================================================================
-- 1. USAGE PATTERNS ANALYSIS
-- ============================================================================

-- Q1.1: Daily usage patterns with trends
WITH daily_usage AS (
    SELECT 
        DATE(timestamp) as usage_date,
        vehicle_type,
        COUNT(DISTINCT vehid) as active_vehicles,
        COUNT(*) as total_trips,
        SUM(trip_distance_km) as total_distance_km,
        AVG(trip_distance_km) as avg_trip_distance,
        SUM(trip_duration_min) / 60.0 as total_hours_driven,
        AVG(avg_speed_kmph) as avg_speed_kmph
    FROM telemetry_data
    WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE(timestamp), vehicle_type
)
SELECT 
    usage_date,
    vehicle_type,
    active_vehicles,
    total_trips,
    total_distance_km,
    avg_trip_distance,
    total_hours_driven,
    avg_speed_kmph,
    -- 7-day moving averages
    AVG(active_vehicles) OVER (
        PARTITION BY vehicle_type 
        ORDER BY usage_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_active_vehicles,
    AVG(total_distance_km) OVER (
        PARTITION BY vehicle_type 
        ORDER BY usage_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_distance
FROM daily_usage
ORDER BY usage_date DESC, vehicle_type;

-- Q1.2: Hourly usage patterns (peak hour analysis)
SELECT 
    EXTRACT(HOUR FROM timestamp) as hour_of_day,
    vehicle_type,
    COUNT(*) as trip_count,
    SUM(trip_distance_km) as total_distance_km,
    AVG(trip_distance_km) as avg_trip_distance,
    AVG(avg_speed_kmph) as avg_speed_kmph,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trip_distance_km) as median_trip_distance
FROM telemetry_data
WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY EXTRACT(HOUR FROM timestamp), vehicle_type
ORDER BY hour_of_day, vehicle_type;

-- Q1.3: Trip distance distribution analysis
SELECT 
    vehicle_type,
    vehicle_class,
    -- Distance buckets
    COUNT(CASE WHEN trip_distance_km < 5 THEN 1 END) as under_5km,
    COUNT(CASE WHEN trip_distance_km BETWEEN 5 AND 10 THEN 1 END) as "5_10_km",
    COUNT(CASE WHEN trip_distance_km BETWEEN 10 AND 20 THEN 1 END) as "10_20_km",
    COUNT(CASE WHEN trip_distance_km BETWEEN 20 AND 50 THEN 1 END) as "20_50_km",
    COUNT(CASE WHEN trip_distance_km > 50 THEN 1 END) as over_50km,
    -- Statistics
    COUNT(*) as total_trips,
    AVG(trip_distance_km) as avg_distance,
    STDDEV(trip_distance_km) as stddev_distance,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance_km) as p25_distance,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trip_distance_km) as median_distance,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance_km) as p75_distance
FROM telemetry_data
WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY vehicle_type, vehicle_class
ORDER BY vehicle_type, vehicle_class;

-- ============================================================================
-- 2. FUEL EFFICIENCY ANALYSIS
-- ============================================================================

-- Q2.1: Comparative fuel efficiency analysis
SELECT 
    v.vehicle_type,
    v.vehicle_class,
    v.engine_type,
    v.is_turbo,
    v.weight_category,
    COUNT(DISTINCT t.vehid) as vehicle_count,
    COUNT(t.telemetry_id) as trip_count,
    AVG(t.fuel_efficiency_kmpl) as avg_efficiency_kmpl,
    MIN(t.fuel_efficiency_kmpl) as min_efficiency_kmpl,
    MAX(t.fuel_efficiency_kmpl) as max_efficiency_kmpl,
    STDDEV(t.fuel_efficiency_kmpl) as efficiency_stddev,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY t.fuel_efficiency_kmpl) as p25_efficiency,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.fuel_efficiency_kmpl) as median_efficiency,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY t.fuel_efficiency_kmpl) as p75_efficiency,
    -- Compare with benchmarks
    AVG(t.fuel_efficiency_kmpl) - 
        COALESCE(eb.mixed_efficiency_kmpl, 
            (SELECT AVG(mixed_efficiency_kmpl) 
             FROM efficiency_benchmarks 
             WHERE vehicle_class = v.vehicle_class 
             AND engine_type = v.engine_type)) as deviation_from_benchmark
FROM telemetry_data t
JOIN vehicles v ON t.vehid = v.vehid
LEFT JOIN efficiency_benchmarks eb ON 
    v.vehicle_class = eb.vehicle_class 
    AND v.engine_type = eb.engine_type
    AND v.displacement_l BETWEEN 
        SPLIT_PART(eb.displacement_range, '-', 1)::NUMERIC 
        AND SPLIT_PART(eb.displacement_range, '-', 2)::NUMERIC
WHERE t.fuel_efficiency_kmpl > 0
AND t.timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY v.vehicle_type, v.vehicle_class, v.engine_type, v.is_turbo, 
         v.weight_category, eb.mixed_efficiency_kmpl
ORDER BY avg_efficiency_kmpl DESC;

-- Q2.2: Fuel efficiency trends over time
WITH efficiency_trends AS (
    SELECT 
        DATE_TRUNC('week', t.timestamp) as week_start,
        v.vehicle_type,
        COUNT(DISTINCT t.vehid) as vehicle_count,
        AVG(t.fuel_efficiency_kmpl) as avg_efficiency,
        -- Week-over-week change
        LAG(AVG(t.fuel_efficiency_kmpl), 1) OVER (
            PARTITION BY v.vehicle_type 
            ORDER BY DATE_TRUNC('week', t.timestamp)
        ) as prev_week_efficiency
    FROM telemetry_data t
    JOIN vehicles v ON t.vehid = v.vehid
    WHERE t.timestamp >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('week', t.timestamp), v.vehicle_type
)
SELECT 
    week_start,
    vehicle_type,
    vehicle_count,
    avg_efficiency,
    prev_week_efficiency,
    avg_efficiency - prev_week_efficiency as week_over_week_change,
    CASE 
        WHEN prev_week_efficiency > 0 
        THEN ((avg_efficiency - prev_week_efficiency) / prev_week_efficiency) * 100
        ELSE NULL
    END as percentage_change
FROM efficiency_trends
ORDER BY week_start DESC, vehicle_type;

-- Q2.3: Factors affecting fuel efficiency (multivariate analysis)
SELECT 
    t.avg_speed_kmph,
    CASE 
        WHEN t.avg_speed_kmph < 30 THEN 'Urban'
        WHEN t.avg_speed_kmph BETWEEN 30 AND 80 THEN 'Suburban'
        WHEN t.avg_speed_kmph > 80 THEN 'Highway'
    END as speed_category,
    t.weather_condition,
    t.road_type,
    v.vehicle_type,
    v.vehicle_class,
    COUNT(*) as trip_count,
    AVG(t.fuel_efficiency_kmpl) as avg_efficiency,
    CORR(t.avg_speed_kmph, t.fuel_efficiency_kmpl) as speed_efficiency_correlation,
    CORR(t.idle_time_min, t.fuel_efficiency_kmpl) as idle_efficiency_correlation
FROM telemetry_data t
JOIN vehicles v ON t.vehid = v.vehid
WHERE t.fuel_efficiency_kmpl > 0
AND t.timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY 
    t.avg_speed_kmph,
    CASE 
        WHEN t.avg_speed_kmph < 30 THEN 'Urban'
        WHEN t.avg_speed_kmph BETWEEN 30 AND 80 THEN 'Suburban'
        WHEN t.avg_speed_kmph > 80 THEN 'Highway'
    END,
    t.weather_condition,
    t.road_type,
    v.vehicle_type,
    v.vehicle_class
ORDER BY avg_efficiency DESC;

-- ============================================================================
-- 3. MAINTENANCE INDICATORS ANALYSIS
-- ============================================================================

-- Q3.1: Predictive maintenance indicators
WITH vehicle_metrics AS (
    SELECT 
        t.vehid,
        v.vehicle_type,
        v.vehicle_class,
        COUNT(*) as total_trips,
        SUM(CASE WHEN t.has_fault THEN 1 ELSE 0 END) as fault_trips,
        SUM(CASE WHEN t.maintenance_flag THEN 1 ELSE 0 END) as maintenance_flag_trips,
        AVG(t.avg_engine_temp_c) as avg_engine_temp,
        AVG(t.fuel_efficiency_kmpl) as avg_efficiency,
        STDDEV(t.fuel_efficiency_kmpl) as efficiency_stddev,
        SUM(t.idle_time_min) as total_idle_minutes,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY t.avg_engine_temp_c) as p95_engine_temp
    FROM telemetry_data t
    JOIN vehicles v ON t.vehid = v.vehid
    WHERE t.timestamp >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY t.vehid, v.vehicle_type, v.vehicle_class
),
maintenance_history_summary AS (
    SELECT 
        vehid,
        COUNT(*) as maintenance_count,
        MAX(maintenance_date) as last_maintenance_date,
        AVG(cost) as avg_maintenance_cost
    FROM maintenance_history
    GROUP BY vehid
)
SELECT 
    vm.vehid,
    vm.vehicle_type,
    vm.vehicle_class,
    vm.total_trips,
    vm.fault_trips,
    vm.fault_trips::FLOAT / NULLIF(vm.total_trips, 0) * 100 as fault_rate_percent,
    vm.maintenance_flag_trips,
    vm.avg_engine_temp,
    vm.avg_efficiency,
    vm.efficiency_stddev,
    vm.total_idle_minutes,
    vm.p95_engine_temp,
    COALESCE(mh.maintenance_count, 0) as past_maintenance_count,
    mh.last_maintenance_date,
    mh.avg_maintenance_cost,
    -- Maintenance urgency score
    CASE 
        WHEN vm.fault_trips > 5 THEN 'CRITICAL'
        WHEN vm.fault_trips > 2 OR vm.p95_engine_temp > 110 THEN 'HIGH'
        WHEN vm.fault_trips > 0 OR vm.efficiency_stddev > 3 THEN 'MEDIUM'
        ELSE 'LOW'
    END as maintenance_urgency,
    -- Estimated time to next maintenance (days)
    CASE 
        WHEN vm.fault_trips > 0 THEN 7
        WHEN vm.p95_engine_temp > 110 THEN 14
        ELSE 30
    END as estimated_days_to_maintenance
FROM vehicle_metrics vm
LEFT JOIN maintenance_history_summary mh ON vm.vehid = mh.vehid
ORDER BY maintenance_urgency DESC, fault_rate_percent DESC;

-- Q3.2: Fault pattern analysis
WITH fault_analysis AS (
    SELECT 
        UNNEST(fault_codes) as fault_code,
        vehid,
        vehicle_type,
        COUNT(*) as occurrence_count
    FROM telemetry_data
    WHERE has_fault = TRUE
    AND fault_codes IS NOT NULL
    AND timestamp >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY UNNEST(fault_codes), vehid, vehicle_type
),
fault_categories AS (
    SELECT 
        fault_code,
        CASE 
            WHEN fault_code LIKE 'P03%' THEN 'Misfire'
            WHEN fault_code LIKE 'P04%' THEN 'Emissions'
            WHEN fault_code LIKE 'P01%' THEN 'Fuel/Air'
            WHEN fault_code LIKE 'P05%' THEN 'Speed/Idle'
            WHEN fault_code LIKE 'P06%' THEN 'Computer'
            ELSE 'Other'
        END as fault_category
    FROM (SELECT DISTINCT fault_code FROM fault_analysis) fa
)
SELECT 
    fc.fault_category,
    fa.fault_code,
    fa.vehicle_type,
    COUNT(DISTINCT fa.vehid) as affected_vehicles,
    SUM(fa.occurrence_count) as total_occurrences,
    AVG(fa.occurrence_count) as avg_occurrences_per_vehicle,
    MIN(fa.occurrence_count) as min_occurrences,
    MAX(fa.occurrence_count) as max_occurrences,
    -- Time between occurrences (if we had timestamps per occurrence)
    -- This would require a more complex query with LAG()
    'Requires detailed analysis' as recurrence_pattern
FROM fault_analysis fa
JOIN fault_categories fc ON fa.fault_code = fc.fault_code
GROUP BY fc.fault_category, fa.fault_code, fa.vehicle_type
ORDER BY total_occurrences DESC, affected_vehicles DESC;

-- ============================================================================
-- 4. IDLE TIME ANALYSIS
-- ============================================================================

-- Q4.1: Idle time analysis by vehicle and time of day
SELECT 
    v.vehicle_type,
    v.vehicle_class,
    EXTRACT(HOUR FROM t.timestamp) as hour_of_day,
    COUNT(*) as trip_count,
    SUM(t.idle_time_min) as total_idle_minutes,
    AVG(t.idle_time_min) as avg_idle_per_trip,
    SUM(t.idle_time_min) / SUM(t.trip_duration_min) * 100 as idle_percentage,
    SUM(t.trip_duration_min) as total_trip_minutes,
    -- Cost of idle time (assuming 0.5L fuel per hour idle)
    SUM(t.idle_time_min) / 60 * 0.5 * 1.2 as idle_fuel_cost_usd
FROM telemetry_data t
JOIN vehicles v ON t.vehid = v.vehid
WHERE t.timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY v.vehicle_type, v.vehicle_class, EXTRACT(HOUR FROM t.timestamp)
ORDER BY idle_percentage DESC, hour_of_day;

-- Q4.2: Excessive idling detection
WITH idle_analysis AS (
    SELECT 
        t.vehid,
        v.vehicle_type,
        DATE(t.timestamp) as trip_date,
        SUM(t.idle_time_min) as daily_idle_minutes,
        COUNT(*) as daily_trips,
        SUM(t.trip_duration_min) as daily_trip_minutes,
        SUM(t.idle_time_min) / SUM(t.trip_duration_min) * 100 as daily_idle_percentage
    FROM telemetry_data t
    JOIN vehicles v ON t.vehid = v.vehid
    WHERE t.timestamp >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY t.vehid, v.vehicle_type, DATE(t.timestamp)
)
SELECT 
    vehid,
    vehicle_type,
    COUNT(*) as days_analyzed,
    AVG(daily_idle_minutes) as avg_daily_idle_minutes,
    AVG(daily_idle_percentage) as avg_daily_idle_percentage,
    SUM(CASE WHEN daily_idle_minutes > 60 THEN 1 ELSE 0 END) as days_with_excessive_idling,
    SUM(CASE WHEN daily_idle_percentage > 20 THEN 1 ELSE 0 END) as days_with_high_idle_percentage,
    -- Overall classification
    CASE 
        WHEN AVG(daily_idle_percentage) > 25 THEN 'Very High Idling'
        WHEN AVG(daily_idle_percentage) > 15 THEN 'High Idling'
        WHEN AVG(daily_idle_percentage) > 10 THEN 'Moderate Idling'
        ELSE 'Normal Idling'
    END as idling_classification,
    -- Estimated annual cost of idling
    AVG(daily_idle_minutes) * 365 / 60 * 0.5 * 1.2 as estimated_annual_idle_cost_usd
FROM idle_analysis
GROUP BY vehid, vehicle_type
HAVING AVG(daily_idle_percentage) > 10
ORDER BY avg_daily_idle_percentage DESC;

-- ============================================================================
-- 5. DRIVER BEHAVIOR ANALYSIS
-- ============================================================================

-- Q5.1: Driver safety score analysis
SELECT 
    db.vehid,
    v.vehicle_type,
    db.driver_category,
    db.overall_score,
    db.safety_score,
    db.efficiency_score,
    db.harsh_acceleration_count,
    db.harsh_braking_count,
    db.speeding_count,
    db.night_driving_hours,
    db.idle_time_percentage,
    db.recommendations,
    db.insurance_premium_factor,
    -- Insurance premium calculation (example)
    1000 * db.insurance_premium_factor as estimated_annual_premium_usd,
    -- Potential savings with improved driving
    1000 * (1.0 - db.insurance_premium_factor) as potential_premium_savings_usd,
    -- Comparison with fleet average
    db.overall_score - AVG(db.overall_score) OVER () as score_vs_fleet_avg,
    -- Percentile rank
    PERCENT_RANK() OVER (ORDER BY db.overall_score DESC) as percentile_rank
FROM driver_behavior db
JOIN vehicles v ON db.vehid = v.vehid
WHERE db.analysis_date = CURRENT_DATE - INTERVAL '1 day'
ORDER BY db.overall_score DESC;

-- Q5.2: Correlation between driver behavior and vehicle performance
SELECT 
    db.driver_category,
    COUNT(DISTINCT db.vehid) as driver_count,
    AVG(db.overall_score) as avg_driver_score,
    AVG(t.fuel_efficiency_kmpl) as avg_fuel_efficiency,
    AVG(CASE WHEN t.has_fault THEN 1 ELSE 0 END) * 100 as fault_rate_percent,
    AVG(t.maintenance_flag::INT) * 100 as maintenance_flag_rate,
    AVG(t.avg_speed_kmph) as avg_speed,
    AVG(t.idle_time_min / NULLIF(t.trip_duration_min, 0)) * 100 as avg_idle_percentage,
    -- Correlations (would need statistical functions)
    'See statistical analysis' as efficiency_correlation,
    'See statistical analysis' as safety_correlation
FROM driver_behavior db
JOIN telemetry_data t ON db.vehid = t.vehid 
    AND DATE(t.timestamp) = db.analysis_date
WHERE db.analysis_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY db.driver_category
ORDER BY avg_driver_score DESC;

-- ============================================================================
-- 6. COST ANALYSIS & OPTIMIZATION
-- ============================================================================

-- Q6.1: Total cost of ownership analysis
WITH cost_summary AS (
    SELECT 
        v.vehid,
        v.vehicle_type,
        v.vehicle_class,
        -- Fuel cost
        SUM(t.fuel_consumed_l) * 1.2 as total_fuel_cost_usd,
        -- Maintenance cost (estimated)
        COALESCE(SUM(mh.cost), 0) as total_maintenance_cost_usd,
        -- Idle time cost (estimated)
        SUM(t.idle_time_min) / 60 * 0.5 * 1.2 as total_idle_cost_usd,
        -- Total distance
        SUM(t.trip_distance_km) as total_distance_km,
        -- Trip count
        COUNT(*) as total_trips
    FROM telemetry_data t
    JOIN vehicles v ON t.vehid = v.vehid
    LEFT JOIN maintenance_history mh ON t.vehid = mh.vehid 
        AND mh.maintenance_date >= CURRENT_DATE - INTERVAL '365 days'
    WHERE t.timestamp >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY v.vehid, v.vehicle_type, v.vehicle_class
)
SELECT 
    vehicle_type,
    vehicle_class,
    COUNT(DISTINCT vehid) as vehicle_count,
    SUM(total_fuel_cost_usd) as total_fuel_cost,
    SUM(total_maintenance_cost_usd) as total_maintenance_cost,
    SUM(total_idle_cost_usd) as total_idle_cost,
    SUM(total_fuel_cost_usd + total_maintenance_cost_usd + total_idle_cost_usd) as total_operating_cost,
    SUM(total_distance_km) as total_distance,
    AVG((total_fuel_cost_usd + total_maintenance_cost_usd + total_idle_cost_usd) / NULLIF(total_distance_km, 0)) as avg_cost_per_km,
    -- Benchmark comparison
    AVG((total_fuel_cost_usd + total_maintenance_cost_usd + total_idle_cost_usd) / NULLIF(total_distance_km, 0)) * 0.85 as target_cost_per_km,
    -- Potential savings
    SUM(total_fuel_cost_usd + total_maintenance_cost_usd + total_idle_cost_usd) * 0.15 as potential_savings_usd
FROM cost_summary
GROUP BY vehicle_type, vehicle_class
ORDER BY avg_cost_per_km DESC;

-- Q6.2: ROI analysis for optimization opportunities
SELECT 
    'Idle Time Reduction' as opportunity,
    'Install idle shutdown technology' as implementation,
    COUNT(DISTINCT t.vehid) as affected_vehicles,
    SUM(t.idle_time_min) / 60 * 0.5 * 1.2 as current_annual_cost_usd,
    SUM(t.idle_time_min) / 60 * 0.5 * 1.2 * 0.5 as potential_savings_usd,
    5000 as implementation_cost_usd,
    (SUM(t.idle_time_min) / 60 * 0.5 * 1.2 * 0.5) / 5000 * 100 as roi_percent,
    CASE 
        WHEN (SUM(t.idle_time_min) / 60 * 0.5 * 1.2 * 0.5) / 5000 > 1 THEN 'HIGH'
        ELSE 'MEDIUM'
    END as priority
FROM telemetry_data t
WHERE t.timestamp >= CURRENT_DATE - INTERVAL '30 days'
AND t.idle_time_min > 10

UNION ALL

SELECT 
    'Route Optimization' as opportunity,
    'Implement GPS routing software' as implementation,
    COUNT(DISTINCT t.vehid) as affected_vehicles,
    SUM(t.fuel_consumed_l) * 1.2 as current_annual_cost_usd,
    SUM(t.fuel_consumed_l) * 1.2 * 0.1 as potential_savings_usd,
    3000 as implementation_cost_usd,
    (SUM(t.fuel_consumed_l) * 1.2 * 0.1) / 3000 * 100 as roi_percent,
    CASE 
        WHEN (SUM(t.fuel_consumed_l) * 1.2 * 0.1) / 3000 > 1 THEN 'HIGH'
        ELSE 'MEDIUM'
    END as priority
FROM telemetry_data t
WHERE t.timestamp >= CURRENT_DATE - INTERVAL '30 days'

UNION ALL

SELECT 
    'Driver Training' as opportunity,
    'Implement defensive driving course' as implementation,
    COUNT(DISTINCT db.vehid) as affected_vehicles,
    1000 * AVG(db.insurance_premium_factor) as current_annual_cost_usd,
    1000 * (AVG(db.insurance_premium_factor) - 0.9) as potential_savings_usd,
    2000 as implementation_cost_usd,
    (1000 * (AVG(db.insurance_premium_factor) - 0.9)) / 2000 * 100 as roi_percent,
    CASE 
        WHEN AVG(db.insurance_premium_factor) > 1.0 THEN 'HIGH'
        ELSE 'MEDIUM'
    END as priority
FROM driver_behavior db
WHERE db.analysis_date = CURRENT_DATE - INTERVAL '1 day'
AND db.overall_score < 80;

-- ============================================================================
-- 7. REAL-TIME MONITORING QUERIES
-- ============================================================================

-- Q7.1: Active vehicles with current status
SELECT 
    t.vehid,
    v.vehicle_type,
    v.vehicle_class,
    MAX(t.timestamp) as last_telemetry_time,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(t.timestamp))) / 60 as minutes_since_last_update,
    MAX(t.fuel_efficiency_kmpl) as last_efficiency,
    MAX(t.avg_engine_temp_c) as last_engine_temp,
    MAX(CASE WHEN t.has_fault THEN 1 ELSE 0 END) as has_active_fault,
    MAX(t.driver_behavior_score) as last_driver_score,
    -- Current status
    CASE 
        WHEN MAX(CASE WHEN t.has_fault THEN 1 ELSE 0 END) = 1 THEN 'FAULT'
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(t.timestamp))) / 60 > 60 THEN 'INACTIVE'
        WHEN MAX(t.avg_engine_temp_c) > 110 THEN 'OVERHEATING'
        WHEN MAX(t.fuel_efficiency_kmpl) < 8 THEN 'LOW_EFFICIENCY'
        ELSE 'NORMAL'
    END as current_status
FROM telemetry_data t
JOIN vehicles v ON t.vehid = v.vehid
WHERE t.timestamp >= CURRENT_TIMESTAMP - INTERVAL '2 hours'
GROUP BY t.vehid, v.vehicle_type, v.vehicle_class
ORDER BY minutes_since_last_update DESC;

-- Q7.2: Real-time alerts dashboard
SELECT 
    a.alert_id,
    a.vehid,
    v.vehicle_type,
    a.alert_type,
    a.severity,
    a.timestamp,
    a.description,
    a.recommended_action,
    a.status,
    a.acknowledged,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - a.timestamp)) / 60 as minutes_since_alert,
    -- Escalation level
    CASE 
        WHEN a.severity = 'CRITICAL' AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - a.timestamp)) / 60 > 30 THEN 'ESCALATE'
        WHEN a.severity = 'WARNING' AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - a.timestamp)) / 60 > 60 THEN 'ESCALATE'
        ELSE 'MONITOR'
    END as escalation_status
FROM alerts a
JOIN vehicles v ON a.vehid = v.vehid
WHERE a.status = 'ACTIVE'
ORDER BY 
    CASE a.severity 
        WHEN 'CRITICAL' THEN 1
        WHEN 'WARNING' THEN 2
        WHEN 'INFO' THEN 3
    END,
    a.timestamp DESC;

-- ============================================================================
-- 8. PREDICTIVE ANALYTICS QUERIES
-- ============================================================================

-- Q8.1: Predictive maintenance schedule
WITH vehicle_stats AS (
    SELECT 
        v.vehid,
        v.vehicle_type,
        v.vehicle_class,
        v.odometer_km,
        v.last_service_km,
        v.service_interval_km,
        COUNT(DISTINCT t.telemetry_id) FILTER (WHERE t.has_fault = TRUE) as recent_faults,
        AVG(t.fuel_efficiency_kmpl) as avg_efficiency,
        AVG(t.avg_engine_temp_c) as avg_engine_temp,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY t.avg_engine_temp_c) as p95_engine_temp
    FROM vehicles v
    LEFT JOIN telemetry_data t ON v.vehid = t.vehid 
        AND t.timestamp >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY v.vehid, v.vehicle_type, v.vehicle_class, v.odometer_km, 
             v.last_service_km, v.service_interval_km
)
SELECT 
    vehid,
    vehicle_type,
    vehicle_class,
    odometer_km,
    last_service_km,
    service_interval_km,
    odometer_km - last_service_km as km_since_last_service,
    recent_faults,
    avg_efficiency,
    avg_engine_temp,
    p95_engine_temp,
    -- Maintenance prediction
    CASE 
        WHEN recent_faults > 3 OR p95_engine_temp > 115 THEN 0
        WHEN recent_faults > 0 OR p95_engine_temp > 110 THEN 7
        WHEN odometer_km - last_service_km >= service_interval_km * 0.9 THEN 14
        WHEN odometer_km - last_service_km >= service_interval_km * 0.8 THEN 30
        ELSE service_interval_km - (odometer_km - last_service_km)
    END as predicted_days_to_maintenance,
    -- Recommended maintenance type
    CASE 
        WHEN recent_faults > 3 THEN 'COMPREHENSIVE DIAGNOSTIC'
        WHEN p95_engine_temp > 115 THEN 'COOLING SYSTEM'
        WHEN recent_faults > 0 THEN 'DIAGNOSTIC CHECK'
        WHEN odometer_km - last_service_km >= service_interval_km THEN 'SCHEDULED MAINTENANCE'
        ELSE 'MONITOR'
    END as recommended_maintenance_type
FROM vehicle_stats
ORDER BY predicted_days_to_maintenance ASC;

-- Q8.2: Fuel efficiency degradation prediction
WITH efficiency_trend AS (
    SELECT 
        vehid,
        DATE_TRUNC('week', timestamp) as week_start,
        AVG(fuel_efficiency_kmpl) as weekly_avg_efficiency,
        COUNT(*) as weekly_trips
    FROM telemetry_data
    WHERE timestamp >= CURRENT_DATE - INTERVAL '90 days'
    AND fuel_efficiency_kmpl > 0
    GROUP BY vehid, DATE_TRUNC('week', timestamp)
),
efficiency_changes AS (
    SELECT 
        vehid,
        week_start,
        weekly_avg_efficiency,
        weekly_trips,
        LAG(weekly_avg_efficiency, 1) OVER (PARTITION BY vehid ORDER BY week_start) as prev_week_efficiency,
        LAG(weekly_avg_efficiency, 4) OVER (PARTITION BY vehid ORDER BY week_start) as month_ago_efficiency
    FROM efficiency_trend
)
SELECT 
    ec.vehid,
    v.vehicle_type,
    ec.week_start,
    ec.weekly_avg_efficiency,
    ec.prev_week_efficiency,
    ec.month_ago_efficiency,
    ec.weekly_avg_efficiency - ec.prev_week_efficiency as week_over_week_change,
    ec.weekly_avg_efficiency - ec.month_ago_efficiency as month_over_month_change,
    -- Degradation detection
    CASE 
        WHEN ec.weekly_avg_efficiency < ec.month_ago_efficiency * 0.9 THEN 'SIGNIFICANT DEGRADATION'
        WHEN ec.weekly_avg_efficiency < ec.month_ago_efficiency * 0.95 THEN 'MODERATE DEGRADATION'
        WHEN ec.weekly_avg_efficiency < ec.prev_week_efficiency * 0.98 THEN 'SLIGHT DEGRADATION'
        ELSE 'STABLE'
    END as degradation_status,
    -- Predicted efficiency in 30 days (simple linear projection)
    ec.weekly_avg_efficiency + (ec.weekly_avg_efficiency - ec.prev_week_efficiency) * 4 as predicted_efficiency_30_days
FROM efficiency_changes ec
JOIN vehicles v ON ec.vehid = v.vehid
WHERE ec.week_start = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '7 days')
AND ec.prev_week_efficiency IS NOT NULL
AND ec.month_ago_efficiency IS NOT NULL
ORDER BY month_over_month_change ASC;

-- ============================================================================
-- 9. ENVIRONMENTAL IMPACT ANALYSIS
-- ============================================================================

-- Q9.1: CO2 emissions analysis
SELECT 
    v.vehicle_type,
    v.vehicle_class,
    v.engine_type,
    COUNT(DISTINCT t.vehid) as vehicle_count,
    SUM(t.trip_distance_km) as total_distance_km,
    SUM(t.fuel_consumed_l) as total_fuel_consumed_l,
    -- CO2 emissions (kg CO2 per liter)
    SUM(t.fuel_consumed_l) * 
        CASE v.engine_type
            WHEN 'Gasoline' THEN 2.31
            WHEN 'Diesel' THEN 2.68
            WHEN 'Hybrid' THEN 1.85
            ELSE 0
        END as total_co2_emissions_kg,
    -- CO2 per km
    AVG(t.fuel_consumed_l * 
        CASE v.engine_type
            WHEN 'Gasoline' THEN 2.31
            WHEN 'Diesel' THEN 2.68
            WHEN 'Hybrid' THEN 1.85
            ELSE 0
        END / NULLIF(t.trip_distance_km, 0)) as avg_co2_per_km,
    -- Comparison with electric vehicles (0.2 kg CO2 per km for grid electricity)
    SUM(t.trip_distance_km) * 0.2 as ev_equivalent_co2_kg,
    -- CO2 savings potential
    SUM(t.fuel_consumed_l * 
        CASE v.engine_type
            WHEN 'Gasoline' THEN 2.31
            WHEN 'Diesel' THEN 2.68
            WHEN 'Hybrid' THEN 1.85
            ELSE 0
        END) - (SUM(t.trip_distance_km) * 0.2) as co2_savings_potential_kg
FROM telemetry_data t
JOIN vehicles v ON t.vehid = v.vehid
WHERE t.timestamp >= CURRENT_DATE - INTERVAL '30 days'
AND v.engine_type != 'Electric'
GROUP BY v.vehicle_type, v.vehicle_class, v.engine_type
ORDER BY total_co2_emissions_kg DESC;

-- ============================================================================
-- 10. EXECUTIVE SUMMARY QUERIES
-- ============================================================================

-- Q10.1: Executive dashboard summary
WITH fleet_summary AS (
    SELECT 
        COUNT(DISTINCT v.vehid) as total_vehicles,
        COUNT(DISTINCT CASE WHEN v.vehicle_type = 'HEV' THEN v.vehid END) as hybrid_vehicles,
        COUNT(DISTINCT CASE WHEN v.vehicle_type = 'BEV' THEN v.vehid END) as electric_vehicles,
        AVG(v.efficiency_score) as avg_efficiency_score
    FROM vehicles v
),
telemetry_summary AS (
    SELECT 
        COUNT(DISTINCT t.vehid) as active_vehicles_today,
        SUM(t.trip_distance_km) as total_distance_today,
        AVG(t.fuel_efficiency_kmpl) as avg_efficiency_today,
        SUM(CASE WHEN t.has_fault THEN 1 ELSE 0 END) as total_faults_today,
        AVG(t.driver_behavior_score) as avg_driver_score_today
    FROM telemetry_data t
    WHERE DATE(t.timestamp) = CURRENT_DATE
),
cost_summary AS (
    SELECT 
        SUM(t.fuel_consumed_l) * 1.2 as daily_fuel_cost,
        COALESCE(SUM(mh.cost), 0) as daily_maintenance_cost,
        SUM(t.idle_time_min) / 60 * 0.5 * 1.2 as daily_idle_cost
    FROM telemetry_data t
    LEFT JOIN maintenance_history mh ON t.vehid = mh.vehid 
        AND mh.maintenance_date = CURRENT_DATE
    WHERE DATE(t.timestamp) = CURRENT_DATE
)
SELECT 
    -- Fleet metrics
    fs.total_vehicles,
    fs.hybrid_vehicles,
    fs.electric_vehicles,
    ROUND((fs.hybrid_vehicles + fs.electric_vehicles)::NUMERIC / fs.total_vehicles * 100, 1) as green_fleet_percentage,
    fs.avg_efficiency_score,
    
    -- Daily performance
    ts.active_vehicles_today,
    ts.total_distance_today,
    ts.avg_efficiency_today,
    ts.total_faults_today,
    ts.avg_driver_score_today,
    
    -- Cost metrics
    cs.daily_fuel_cost,
    cs.daily_maintenance_cost,
    cs.daily_idle_cost,
    cs.daily_fuel_cost + cs.daily_maintenance_cost + cs.daily_idle_cost as total_daily_cost,
    
    -- KPIs
    CASE 
        WHEN ts.total_faults_today > 5 THEN 'HIGH'
        WHEN ts.total_faults_today > 2 THEN 'MEDIUM'
        ELSE 'LOW'
    END as fault_severity,
    
    CASE 
        WHEN ts.avg_efficiency_today > 15 THEN 'EXCELLENT'
        WHEN ts.avg_efficiency_today > 12 THEN 'GOOD'
        WHEN ts.avg_efficiency_today > 9 THEN 'FAIR'
        ELSE 'POOR'
    END as efficiency_rating,
    
    CASE 
        WHEN ts.avg_driver_score_today > 0.9 THEN 'EXCELLENT'
        WHEN ts.avg_driver_score_today > 0.8 THEN 'GOOD'
        WHEN ts.avg_driver_score_today > 0.7 THEN 'FAIR'
        ELSE 'POOR'
    END as driver_rating
    
FROM fleet_summary fs, telemetry_summary ts, cost_summary cs;
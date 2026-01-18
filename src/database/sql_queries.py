class AnalyticalQueries:
    """Collection of analytical SQL queries for vehicle telemetry analysis"""
    
    # 1. Usage Patterns Analysis
    QUERY_DAILY_USAGE = """
        SELECT 
            DATE(timestamp) as date,
            vehicle_type,
            vehicle_class,
            COUNT(DISTINCT vehid) as active_vehicles,
            SUM(trip_distance_km) as total_distance,
            AVG(trip_distance_km) as avg_distance_per_vehicle,
            SUM(trip_duration_min) / 60.0 as total_hours,
            AVG(avg_speed_kmph) as avg_speed
        FROM telemetry_data
        WHERE trip_id NOT LIKE '%SUMMARY'
        GROUP BY DATE(timestamp), vehicle_type, vehicle_class
        ORDER BY date DESC, total_distance DESC
    """
    
    # 2. Fuel Efficiency Analysis
    QUERY_FUEL_EFFICIENCY = """
        SELECT 
            vehicle_type,
            vehicle_class,
            engine_type,
            is_turbo,
            weight_category,
            COUNT(*) as trip_count,
            AVG(fuel_efficiency_kmpl) as avg_efficiency,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fuel_efficiency_kmpl) as median_efficiency,
            MIN(fuel_efficiency_kmpl) as min_efficiency,
            MAX(fuel_efficiency_kmpl) as max_efficiency,
            STDDEV(fuel_efficiency_kmpl) as efficiency_stddev
        FROM telemetry_data
        WHERE fuel_efficiency_kmpl > 0 
            AND trip_id NOT LIKE '%SUMMARY'
        GROUP BY vehicle_type, vehicle_class, engine_type, is_turbo, weight_category
        ORDER BY avg_efficiency DESC
    """
    
    # 3. Maintenance Indicators
    QUERY_MAINTENANCE_INDICATORS = """
        WITH fault_analysis AS (
            SELECT 
                vehid,
                COUNT(*) as total_trips,
                SUM(CASE WHEN has_fault THEN 1 ELSE 0 END) as fault_trips,
                SUM(CASE WHEN maintenance_flag THEN 1 ELSE 0 END) as maintenance_trips,
                AVG(avg_engine_temp_c) as avg_engine_temp,
                AVG(avg_rpm) as avg_rpm,
                SUM(idle_time_min) / SUM(trip_duration_min) * 100 as idle_percentage
            FROM telemetry_data
            WHERE trip_id NOT LIKE '%SUMMARY'
            GROUP BY vehid
        ),
        vehicle_info AS (
            SELECT v.*, fa.*
            FROM vehicles v
            JOIN fault_analysis fa ON v.vehid = fa.vehid
        )
        SELECT 
            vehicle_type,
            vehicle_class,
            engine_type,
            COUNT(*) as vehicle_count,
            AVG(fault_trips * 100.0 / total_trips) as fault_rate_percent,
            AVG(maintenance_trips * 100.0 / total_trips) as maintenance_rate_percent,
            AVG(avg_engine_temp) as avg_engine_temp,
            AVG(idle_percentage) as avg_idle_percentage,
            CASE 
                WHEN fault_rate_percent > 5 THEN 'High Maintenance'
                WHEN fault_rate_percent > 2 THEN 'Medium Maintenance'
                ELSE 'Low Maintenance'
            END as maintenance_category
        FROM vehicle_info
        GROUP BY vehicle_type, vehicle_class, engine_type
        ORDER BY fault_rate_percent DESC
    """
    
    # 4. Idle Time Analysis
    QUERY_IDLE_TIME_ANALYSIS = """
        SELECT 
            v.vehicle_class,
            v.engine_type,
            DATE(t.timestamp) as date,
            AVG(t.idle_time_min / t.trip_duration_min * 100) as avg_idle_percentage,
            SUM(t.idle_time_min) as total_idle_minutes,
            COUNT(DISTINCT t.vehid) as vehicles_analyzed,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY t.idle_time_min) as idle_p25,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.idle_time_min) as idle_median,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY t.idle_time_min) as idle_p75
        FROM telemetry_data t
        JOIN vehicles v ON t.vehid = v.vehid
        WHERE t.trip_duration_min > 0 
            AND t.trip_id NOT LIKE '%SUMMARY'
        GROUP BY v.vehicle_class, v.engine_type, DATE(t.timestamp)
        ORDER BY avg_idle_percentage DESC
    """
    
    # 5. Fault Frequency Analysis
    QUERY_FAULT_FREQUENCY = """
        WITH fault_details AS (
            SELECT 
                t.vehid,
                v.vehicle_type,
                v.vehicle_class,
                t.fault_codes,
                DATE(t.timestamp) as fault_date,
                COUNT(*) OVER (PARTITION BY t.vehid) as total_faults,
                ROW_NUMBER() OVER (PARTITION BY t.vehid ORDER BY t.timestamp) as fault_sequence
            FROM telemetry_data t
            JOIN vehicles v ON t.vehid = v.vehid
            WHERE t.has_fault = TRUE
        ),
        fault_patterns AS (
            SELECT 
                vehicle_type,
                vehicle_class,
                fault_codes,
                COUNT(*) as fault_count,
                COUNT(DISTINCT vehid) as affected_vehicles,
                MIN(fault_date) as first_occurrence,
                MAX(fault_date) as last_occurrence
            FROM fault_details
            GROUP BY vehicle_type, vehicle_class, fault_codes
        )
        SELECT 
            fp.*,
            fp.fault_count * 100.0 / SUM(fp.fault_count) OVER () as fault_percentage,
            CASE 
                WHEN fp.fault_count > 10 THEN 'Critical'
                WHEN fp.fault_count > 5 THEN 'High'
                WHEN fp.fault_count > 2 THEN 'Medium'
                ELSE 'Low'
            END as severity_level
        FROM fault_patterns fp
        ORDER BY fault_count DESC
    """
    
    # 6. Comparative Analysis: HEV vs ICE
    QUERY_HEV_VS_ICE = """
        SELECT 
            v.vehicle_type,
            v.vehicle_class,
            v.engine_type,
            v.is_turbo,
            COUNT(DISTINCT t.vehid) as vehicle_count,
            AVG(t.fuel_efficiency_kmpl) as avg_fuel_efficiency,
            AVG(t.trip_distance_km) as avg_trip_distance,
            AVG(t.avg_speed_kmph) as avg_speed,
            AVG(t.idle_time_min / t.trip_duration_min * 100) as avg_idle_percentage,
            SUM(CASE WHEN t.has_fault THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as fault_rate_percent,
            AVG(t.driver_behavior_score) as avg_driver_score
        FROM telemetry_data t
        JOIN vehicles v ON t.vehid = v.vehid
        WHERE t.trip_id NOT LIKE '%SUMMARY'
        GROUP BY v.vehicle_type, v.vehicle_class, v.engine_type, v.is_turbo
        ORDER BY avg_fuel_efficiency DESC
    """
    
    # 7. Driver Behavior Analysis
    QUERY_DRIVER_BEHAVIOR = """
        WITH driver_stats AS (
            SELECT 
                vehid,
                AVG(driver_behavior_score) as avg_score,
                PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY driver_behavior_score) as score_p10,
                PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY driver_behavior_score) as score_p90,
                COUNT(*) as trip_count,
                AVG(fuel_efficiency_kmpl) as avg_efficiency,
                AVG(avg_speed_kmph) as avg_speed
            FROM telemetry_data
            WHERE trip_id NOT LIKE '%SUMMARY'
            GROUP BY vehid
        ),
        vehicle_driver AS (
            SELECT 
                v.vehid,
                v.vehicle_type,
                v.vehicle_class,
                ds.*,
                CASE 
                    WHEN ds.avg_score >= 0.9 THEN 'Excellent'
                    WHEN ds.avg_score >= 0.8 THEN 'Good'
                    WHEN ds.avg_score >= 0.7 THEN 'Average'
                    WHEN ds.avg_score >= 0.6 THEN 'Needs Improvement'
                    ELSE 'Poor'
                END as driver_category
            FROM vehicles v
            JOIN driver_stats ds ON v.vehid = ds.vehid
        )
        SELECT 
            vehicle_type,
            vehicle_class,
            driver_category,
            COUNT(*) as driver_count,
            AVG(avg_score) as avg_driver_score,
            AVG(avg_efficiency) as avg_fuel_efficiency,
            AVG(avg_speed) as avg_speed,
            AVG(trip_count) as avg_trips_per_driver
        FROM vehicle_driver
        GROUP BY vehicle_type, vehicle_class, driver_category
        ORDER BY vehicle_type, driver_category
    """
    
    # 8. Cost Analysis
    QUERY_COST_ANALYSIS = """
        WITH fuel_costs AS (
            SELECT 
                v.vehid,
                v.vehicle_type,
                v.vehicle_class,
                SUM(t.fuel_consumed_l) as total_fuel_l,
                SUM(t.fuel_consumed_l) * 1.2 as fuel_cost_usd, -- Assuming $1.2 per liter
                SUM(t.trip_distance_km) as total_distance_km,
                COUNT(*) as trip_count,
                SUM(CASE WHEN t.has_fault THEN 1 ELSE 0 END) as fault_count
            FROM telemetry_data t
            JOIN vehicles v ON t.vehid = v.vehid
            WHERE t.trip_id NOT LIKE '%SUMMARY'
            GROUP BY v.vehid, v.vehicle_type, v.vehicle_class
        ),
        maintenance_costs AS (
            SELECT 
                vehid,
                COUNT(*) * 150 as estimated_maintenance_cost -- $150 per maintenance event
            FROM telemetry_data
            WHERE maintenance_flag = TRUE
            GROUP BY vehid
        )
        SELECT 
            fc.vehicle_type,
            fc.vehicle_class,
            COUNT(DISTINCT fc.vehid) as vehicle_count,
            SUM(fc.total_distance_km) as total_distance,
            SUM(fc.fuel_cost_usd) as total_fuel_cost,
            COALESCE(SUM(mc.estimated_maintenance_cost), 0) as total_maintenance_cost,
            SUM(fc.fuel_cost_usd + COALESCE(mc.estimated_maintenance_cost, 0)) as total_operating_cost,
            AVG(fc.fuel_cost_usd / fc.total_distance_km * 100) as cost_per_100km,
            AVG(fc.fault_count * 100.0 / fc.trip_count) as fault_rate_percent
        FROM fuel_costs fc
        LEFT JOIN maintenance_costs mc ON fc.vehid = mc.vehid
        GROUP BY fc.vehicle_type, fc.vehicle_class
        ORDER BY cost_per_100km DESC
    """
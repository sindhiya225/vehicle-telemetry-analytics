-- Vehicle Telemetry Analytics - Business Insights Queries
-- Strategic queries for business decision making

-- ============================================================================
-- 1. STRATEGIC BUSINESS INSIGHTS
-- ============================================================================

-- Insight 1.1: Fleet Optimization Opportunities
-- Identifies which vehicles to replace/upgrade based on TCO
WITH vehicle_tco AS (
    SELECT 
        v.vehid,
        v.vehicle_type,
        v.vehicle_class,
        v.engine_type,
        v.displacement_l,
        v.efficiency_category,
        -- Total Cost of Ownership components
        SUM(t.fuel_consumed_l) * 1.2 as annual_fuel_cost,
        COALESCE(SUM(mh.cost), 0) as annual_maintenance_cost,
        SUM(t.idle_time_min) / 60 * 0.5 * 1.2 as annual_idle_cost,
        -- Depreciation (simplified: 15% of vehicle value annually)
        30000 * 0.15 as annual_depreciation,
        -- Insurance (estimated)
        1200 as annual_insurance,
        -- Total distance
        SUM(t.trip_distance_km) as annual_distance_km,
        -- Efficiency metrics
        AVG(t.fuel_efficiency_kmpl) as avg_efficiency,
        COUNT(DISTINCT CASE WHEN t.has_fault THEN t.telemetry_id END) as fault_count
    FROM telemetry_data t
    JOIN vehicles v ON t.vehid = v.vehid
    LEFT JOIN maintenance_history mh ON t.vehid = mh.vehid 
        AND mh.maintenance_date >= CURRENT_DATE - INTERVAL '365 days'
    WHERE t.timestamp >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY v.vehid, v.vehicle_type, v.vehicle_class, v.engine_type, 
             v.displacement_l, v.efficiency_category
),
replacement_analysis AS (
    SELECT 
        *,
        annual_fuel_cost + annual_maintenance_cost + annual_idle_cost + 
        annual_depreciation + annual_insurance as total_annual_cost,
        (annual_fuel_cost + annual_maintenance_cost + annual_idle_cost + 
         annual_depreciation + annual_insurance) / NULLIF(annual_distance_km, 0) as cost_per_km,
        -- Replacement candidate score (higher score = higher priority to replace)
        CASE 
            WHEN efficiency_category = 'Low' THEN 3
            WHEN efficiency_category = 'Medium' THEN 2
            ELSE 1
        END +
        CASE 
            WHEN fault_count > 10 THEN 3
            WHEN fault_count > 5 THEN 2
            WHEN fault_count > 0 THEN 1
            ELSE 0
        END +
        CASE 
            WHEN annual_maintenance_cost > 5000 THEN 3
            WHEN annual_maintenance_cost > 3000 THEN 2
            WHEN annual_maintenance_cost > 1000 THEN 1
            ELSE 0
        END as replacement_score
    FROM vehicle_tco
)
SELECT 
    vehicle_type,
    vehicle_class,
    engine_type,
    COUNT(*) as vehicle_count,
    AVG(total_annual_cost) as avg_annual_cost,
    AVG(cost_per_km) as avg_cost_per_km,
    SUM(total_annual_cost) as total_annual_cost,
    AVG(replacement_score) as avg_replacement_score,
    COUNT(CASE WHEN replacement_score >= 5 THEN 1 END) as high_priority_replacements,
    COUNT(CASE WHEN replacement_score BETWEEN 3 AND 4 THEN 1 END) as medium_priority_replacements,
    -- Estimated savings from replacement with new HEV/EV
    SUM(total_annual_cost) * 0.3 as potential_annual_savings,
    -- Payback period for replacement (years)
    CASE 
        WHEN AVG(total_annual_cost) > 0 
        THEN 30000 / (AVG(total_annual_cost) * 0.3)
        ELSE NULL
    END as estimated_payback_period_years
FROM replacement_analysis
GROUP BY vehicle_type, vehicle_class, engine_type
ORDER BY avg_replacement_score DESC, total_annual_cost DESC;

-- Insight 1.2: ROI Analysis for Fleet Electrification
WITH current_fleet_cost AS (
    SELECT 
        v.vehicle_type,
        COUNT(DISTINCT v.vehid) as current_count,
        SUM(t.fuel_consumed_l) * 1.2 as annual_fuel_cost,
        COALESCE(SUM(mh.cost), 0) as annual_maintenance_cost,
        SUM(t.trip_distance_km) as annual_distance_km
    FROM telemetry_data t
    JOIN vehicles v ON t.vehid = v.vehid
    LEFT JOIN maintenance_history mh ON t.vehid = mh.vehid 
        AND mh.maintenance_date >= CURRENT_DATE - INTERVAL '365 days'
    WHERE t.timestamp >= CURRENT_DATE - INTERVAL '365 days'
    AND v.vehicle_type IN ('ICE', 'HEV')
    GROUP BY v.vehicle_type
),
ev_replacement_cost AS (
    SELECT 
        cf.vehicle_type,
        cf.current_count,
        cf.annual_fuel_cost,
        cf.annual_maintenance_cost,
        cf.annual_distance_km,
        -- EV costs (estimated)
        cf.annual_distance_km * 0.2 * 0.12 as ev_annual_energy_cost, -- 0.2 kWh/km * $0.12/kWh
        cf.annual_maintenance_cost * 0.5 as ev_annual_maintenance_cost, -- 50% lower maintenance
        -- EV purchase cost premium
        CASE 
            WHEN cf.vehicle_type = 'ICE' THEN 10000
            WHEN cf.vehicle_type = 'HEV' THEN 5000
            ELSE 0
        END as ev_premium_cost
    FROM current_fleet_cost cf
)
SELECT 
    vehicle_type,
    current_count,
    annual_fuel_cost as current_annual_fuel_cost,
    annual_maintenance_cost as current_annual_maintenance_cost,
    annual_fuel_cost + annual_maintenance_cost as current_total_annual_cost,
    ev_annual_energy_cost as ev_annual_energy_cost,
    ev_annual_maintenance_cost as ev_annual_maintenance_cost,
    ev_annual_energy_cost + ev_annual_maintenance_cost as ev_total_annual_cost,
    (annual_fuel_cost + annual_maintenance_cost) - 
    (ev_annual_energy_cost + ev_annual_maintenance_cost) as annual_savings_per_vehicle,
    ev_premium_cost as ev_premium_cost,
    ev_premium_cost / NULLIF(
        (annual_fuel_cost + annual_maintenance_cost) - 
        (ev_annual_energy_cost + ev_annual_maintenance_cost), 0
    ) as payback_period_years,
    -- Total fleet impact
    current_count * ((annual_fuel_cost + annual_maintenance_cost) - 
    (ev_annual_energy_cost + ev_annual_maintenance_cost)) as total_annual_savings,
    current_count * ev_premium_cost as total_ev_premium_cost,
    -- CO2 reduction (kg)
    current_count * annual_distance_km / current_count * 
        CASE vehicle_type
            WHEN 'ICE' THEN 2.31 / 12 * 1000 -- Convert to g/km then kg
            WHEN 'HEV' THEN 1.85 / 12 * 1000
            ELSE 0
        END as annual_co2_reduction_kg
FROM ev_replacement_cost
ORDER BY payback_period_years ASC;

-- ============================================================================
-- 2. OPERATIONAL EFFICIENCY INSIGHTS
-- ============================================================================

-- Insight 2.1: Optimal Vehicle Allocation by Usage Pattern
WITH vehicle_usage_patterns AS (
    SELECT 
        v.vehid,
        v.vehicle_type,
        v.vehicle_class,
        v.efficiency_category,
        -- Usage patterns
        AVG(t.trip_distance_km) as avg_trip_distance,
        STDDEV(t.trip_distance_km) as trip_distance_stddev,
        AVG(t.avg_speed_kmph) as avg_speed,
        COUNT(CASE WHEN EXTRACT(HOUR FROM t.timestamp) BETWEEN 7 AND 9 THEN 1 END) as morning_rush_trips,
        COUNT(CASE WHEN EXTRACT(HOUR FROM t.timestamp) BETWEEN 16 AND 18 THEN 1 END) as evening_rush_trips,
        COUNT(CASE WHEN t.road_type = 'Urban' THEN 1 END) as urban_trips,
        COUNT(CASE WHEN t.road_type = 'Highway' THEN 1 END) as highway_trips,
        COUNT(*) as total_trips,
        -- Performance metrics
        AVG(t.fuel_efficiency_kmpl) as avg_efficiency,
        AVG(t.driver_behavior_score) as avg_driver_score,
        COUNT(CASE WHEN t.has_fault THEN 1 END) as fault_count
    FROM telemetry_data t
    JOIN vehicles v ON t.vehid = v.vehid
    WHERE t.timestamp >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY v.vehid, v.vehicle_type, v.vehicle_class, v.efficiency_category
),
usage_clusters AS (
    SELECT 
        *,
        -- Cluster vehicles by usage pattern
        CASE 
            WHEN avg_trip_distance < 10 AND urban_trips::FLOAT / total_trips > 0.7 THEN 'Urban Short Haul'
            WHEN avg_trip_distance BETWEEN 10 AND 50 AND urban_trips::FLOAT / total_trips > 0.5 THEN 'Urban Medium Haul'
            WHEN avg_trip_distance > 50 AND highway_trips::FLOAT / total_trips > 0.7 THEN 'Highway Long Haul'
            WHEN morning_rush_trips + evening_rush_trips > total_trips * 0.5 THEN 'Rush Hour Specialist'
            ELSE 'Mixed Usage'
        END as usage_pattern,
        -- Suitability score for current usage
        CASE 
            WHEN vehicle_type = 'HEV' AND urban_trips::FLOAT / total_trips > 0.6 THEN 10
            WHEN vehicle_type = 'ICE' AND highway_trips::FLOAT / total_trips > 0.6 THEN 8
            WHEN vehicle_type = 'BEV' AND avg_trip_distance < 50 THEN 9
            ELSE 5
        END as current_suitability_score,
        -- Potential suitability score for optimal vehicle type
        CASE 
            WHEN usage_pattern = 'Urban Short Haul' THEN 
                CASE vehicle_type
                    WHEN 'BEV' THEN 10
                    WHEN 'HEV' THEN 8
                    ELSE 3
                END
            WHEN usage_pattern = 'Highway Long Haul' THEN 
                CASE vehicle_type
                    WHEN 'ICE' THEN 9
                    WHEN 'HEV' THEN 7
                    ELSE 2
                END
            ELSE 5
        END as optimal_suitability_score
    FROM vehicle_usage_patterns
)
SELECT 
    usage_pattern,
    vehicle_type,
    vehicle_class,
    COUNT(*) as vehicle_count,
    AVG(avg_trip_distance) as avg_trip_distance,
    AVG(avg_efficiency) as avg_efficiency,
    AVG(current_suitability_score) as avg_current_score,
    AVG(optimal_suitability_score) as avg_optimal_score,
    AVG(optimal_suitability_score - current_suitability_score) as improvement_potential,
    COUNT(CASE WHEN optimal_suitability_score - current_suitability_score > 2 THEN 1 END) as poor_fit_vehicles,
    -- Recommendation
    CASE 
        WHEN usage_pattern = 'Urban Short Haul' AND vehicle_type != 'BEV' THEN 'Consider EV replacement'
        WHEN usage_pattern = 'Highway Long Haul' AND vehicle_type = 'BEV' THEN 'Reassign to urban routes'
        WHEN current_suitability_score < 5 THEN 'Review vehicle allocation'
        ELSE 'Optimal allocation'
    END as allocation_recommendation
FROM usage_clusters
GROUP BY usage_pattern, vehicle_type, vehicle_class
ORDER BY improvement_potential DESC, poor_fit_vehicles DESC;

-- Insight 2.2: Driver-Vehicle Matching Optimization
WITH driver_vehicle_performance AS (
    SELECT 
        t.vehid,
        v.vehicle_type,
        v.vehicle_class,
        db.driver_id,
        db.driver_category,
        db.overall_score as driver_score,
        -- Vehicle performance with this driver
        AVG(t.fuel_efficiency_kmpl) as efficiency_with_driver,
        AVG(t.fuel_efficiency_kmpl) OVER (PARTITION BY t.vehid) as avg_vehicle_efficiency,
        COUNT(CASE WHEN t.has_fault THEN 1 END) as faults_with_driver,
        COUNT(CASE WHEN t.has_fault THEN 1 END) OVER (PARTITION BY t.vehid) as total_vehicle_faults,
        COUNT(*) as trips_with_driver,
        COUNT(*) OVER (PARTITION BY t.vehid) as total_vehicle_trips
    FROM telemetry_data t
    JOIN vehicles v ON t.vehid = v.vehid
    JOIN driver_behavior db ON t.vehid = db.vehid 
        AND DATE(t.timestamp) = db.analysis_date
    WHERE t.timestamp >= CURRENT_DATE - INTERVAL '90 days'
    AND db.analysis_date >= CURRENT_DATE - INTERVAL '90 days'
),
driver_vehicle_match AS (
    SELECT 
        *,
        efficiency_with_driver - avg_vehicle_efficiency as efficiency_delta,
        (faults_with_driver::FLOAT / NULLIF(trips_with_driver, 0)) - 
        (total_vehicle_faults::FLOAT / NULLIF(total_vehicle_trips, 0)) as fault_rate_delta,
        -- Match score (higher = better match)
        CASE 
            WHEN efficiency_delta > 2 THEN 3
            WHEN efficiency_delta > 1 THEN 2
            WHEN efficiency_delta > 0 THEN 1
            WHEN efficiency_delta > -1 THEN 0
            WHEN efficiency_delta > -2 THEN -1
            ELSE -2
        END +
        CASE 
            WHEN fault_rate_delta < -0.1 THEN 2
            WHEN fault_rate_delta < 0 THEN 1
            WHEN fault_rate_delta < 0.1 THEN 0
            ELSE -1
        END +
        CASE 
            WHEN driver_score > 90 THEN 2
            WHEN driver_score > 80 THEN 1
            ELSE 0
        END as match_score
    FROM driver_vehicle_performance
    WHERE trips_with_driver >= 10
)
SELECT 
    vehicle_type,
    vehicle_class,
    driver_category,
    COUNT(DISTINCT vehid) as vehicles_used,
    COUNT(DISTINCT driver_id) as drivers_assigned,
    AVG(match_score) as avg_match_score,
    AVG(efficiency_delta) as avg_efficiency_impact,
    AVG(fault_rate_delta) as avg_fault_rate_impact,
    COUNT(CASE WHEN match_score >= 3 THEN 1 END) as excellent_matches,
    COUNT(CASE WHEN match_score BETWEEN 1 AND 2 THEN 1 END) as good_matches,
    COUNT(CASE WHEN match_score <= 0 THEN 1 END) as poor_matches,
    -- Improvement recommendations
    CASE 
        WHEN AVG(match_score) < 0 AND vehicle_class = 'SUV' AND driver_category = 'Needs Improvement' 
        THEN 'Avoid assigning inexperienced drivers to SUVs'
        WHEN AVG(efficiency_impact) < -1 AND vehicle_type = 'HEV' 
        THEN 'Provide HEV-specific training to drivers'
        WHEN AVG(match_score) > 2 AND driver_category = 'Excellent' 
        THEN 'Use as benchmark for driver training'
        ELSE 'No specific action needed'
    END as recommendation
FROM driver_vehicle_match
GROUP BY vehicle_type, vehicle_class, driver_category
ORDER BY avg_match_score DESC;

-- ============================================================================
-- 3. FINANCIAL OPTIMIZATION INSIGHTS
-- ============================================================================

-- Insight 3.1: Cost-Benefit Analysis of Preventive Maintenance
WITH maintenance_impact AS (
    SELECT 
        mh.vehid,
        v.vehicle_type,
        v.vehicle_class,
        mh.maintenance_type,
        mh.cost,
        mh.maintenance_date,
        -- Performance before maintenance
        AVG(CASE 
            WHEN t.timestamp < mh.maintenance_date 
            AND t.timestamp >= mh.maintenance_date - INTERVAL '30 days'
            THEN t.fuel_efficiency_kmpl 
        END) as efficiency_before,
        -- Performance after maintenance
        AVG(CASE 
            WHEN t.timestamp > mh.maintenance_date 
            AND t.timestamp <= mh.maintenance_date + INTERVAL '30 days'
            THEN t.fuel_efficiency_kmpl 
        END) as efficiency_after,
        -- Faults before maintenance
        COUNT(CASE 
            WHEN t.timestamp < mh.maintenance_date 
            AND t.timestamp >= mh.maintenance_date - INTERVAL '30 days'
            AND t.has_fault THEN 1 
        END) as faults_before,
        -- Faults after maintenance
        COUNT(CASE 
            WHEN t.timestamp > mh.maintenance_date 
            AND t.timestamp <= mh.maintenance_date + INTERVAL '30 days'
            AND t.has_fault THEN 1 
        END) as faults_after,
        -- Distance driven after maintenance
        SUM(CASE 
            WHEN t.timestamp > mh.maintenance_date 
            AND t.timestamp <= mh.maintenance_date + INTERVAL '30 days'
            THEN t.trip_distance_km 
        END) as distance_after_30_days
    FROM maintenance_history mh
    JOIN vehicles v ON mh.vehid = v.vehid
    LEFT JOIN telemetry_data t ON mh.vehid = t.vehid
        AND t.timestamp BETWEEN mh.maintenance_date - INTERVAL '30 days' 
        AND mh.maintenance_date + INTERVAL '30 days'
    WHERE mh.maintenance_date >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY mh.vehid, v.vehicle_type, v.vehicle_class, mh.maintenance_type, 
             mh.cost, mh.maintenance_date
)
SELECT 
    maintenance_type,
    vehicle_type,
    vehicle_class,
    COUNT(*) as maintenance_count,
    AVG(cost) as avg_cost,
    AVG(efficiency_after - efficiency_before) as avg_efficiency_improvement,
    AVG(faults_before - faults_after) as avg_fault_reduction,
    AVG(distance_after_30_days) as avg_distance_30_days_post,
    -- ROI calculation
    AVG(
        (efficiency_after - efficiency_before) * 
        distance_after_30_days / efficiency_after * 1.2  -- Fuel cost savings
        + (faults_before - faults_after) * 500  -- Estimated repair cost avoidance
        - cost
    ) as avg_net_benefit_30_days,
    AVG(
        ((efficiency_after - efficiency_before) * 
        distance_after_30_days / efficiency_after * 1.2
        + (faults_before - faults_after) * 500) / cost * 100
    ) as avg_roi_percent_30_days,
    -- Maintenance effectiveness rating
    CASE 
        WHEN AVG(efficiency_after - efficiency_before) > 2 
        AND AVG(faults_before - faults_after) > 1 THEN 'HIGHLY EFFECTIVE'
        WHEN AVG(efficiency_after - efficiency_before) > 1 
        OR AVG(faults_before - faults_after) > 0 THEN 'EFFECTIVE'
        ELSE 'INEFFECTIVE'
    END as effectiveness_rating,
    -- Recommendation
    CASE 
        WHEN AVG(roi_percent_30_days) > 100 THEN 'Increase frequency'
        WHEN AVG(roi_percent_30_days) BETWEEN 50 AND 100 THEN 'Maintain current schedule'
        WHEN AVG(roi_percent_30_days) < 50 THEN 'Review necessity or negotiate cost'
        ELSE 'Insufficient data'
    END as recommendation
FROM maintenance_impact
WHERE efficiency_before IS NOT NULL 
AND efficiency_after IS NOT NULL
GROUP BY maintenance_type, vehicle_type, vehicle_class
ORDER BY avg_roi_percent_30_days DESC;

-- Insight 3.2: Insurance Premium Optimization Analysis
WITH driver_insurance_analysis AS (
    SELECT 
        db.vehid,
        v.vehicle_type,
        v.vehicle_class,
        db.driver_id,
        db.overall_score,
        db.safety_score,
        db.harsh_acceleration_count,
        db.harsh_braking_count,
        db.speeding_count,
        -- Current insurance premium factor
        db.insurance_premium_factor,
        -- Base premium for vehicle type
        CASE v.vehicle_type
            WHEN 'SUV' THEN 1200
            WHEN 'Truck' THEN 1400
            WHEN 'ICE' THEN 1000
            WHEN 'HEV' THEN 900
            WHEN 'BEV' THEN 800
            ELSE 1000
        END as base_annual_premium,
        -- Risk factors
        CASE 
            WHEN db.safety_score < 70 THEN 1.5
            WHEN db.safety_score < 80 THEN 1.2
            WHEN db.safety_score < 90 THEN 1.0
            ELSE 0.8
        END as safety_factor,
        CASE 
            WHEN db.harsh_braking_count > 5 THEN 1.3
            WHEN db.harsh_braking_count > 2 THEN 1.1
            ELSE 1.0
        END as braking_factor,
        CASE 
            WHEN db.speeding_count > 10 THEN 1.4
            WHEN db.speeding_count > 5 THEN 1.2
            ELSE 1.0
        END as speeding_factor
    FROM driver_behavior db
    JOIN vehicles v ON db.vehid = v.vehid
    WHERE db.analysis_date = CURRENT_DATE - INTERVAL '1 day'
),
premium_calculation AS (
    SELECT 
        *,
        base_annual_premium * safety_factor * braking_factor * speeding_factor as calculated_premium,
        base_annual_premium * insurance_premium_factor as current_premium,
        -- Potential savings with improved driving
        base_annual_premium * insurance_premium_factor - 
        base_annual_premium * 0.8 as max_potential_savings,
        -- Improvement needed for discount
        CASE 
            WHEN safety_score < 90 THEN 90 - safety_score
            ELSE 0
        END as safety_score_improvement_needed,
        CASE 
            WHEN harsh_braking_count > 2 THEN harsh_braking_count - 2
            ELSE 0
        END as harsh_braking_improvement_needed
    FROM driver_insurance_analysis
)
SELECT 
    vehicle_type,
    vehicle_class,
    COUNT(*) as driver_count,
    AVG(overall_score) as avg_driver_score,
    AVG(safety_score) as avg_safety_score,
    SUM(current_premium) as total_current_premium,
    SUM(calculated_premium) as total_calculated_premium,
    SUM(current_premium - calculated_premium) as total_premium_overpayment,
    SUM(max_potential_savings) as total_potential_savings,
    AVG(safety_score_improvement_needed) as avg_safety_improvement_needed,
    AVG(harsh_braking_improvement_needed) as avg_braking_improvement_needed,
    -- Insurance optimization recommendation
    CASE 
        WHEN AVG(safety_score) < 80 AND AVG(harsh_braking_count) > 3 
        THEN 'High priority: Implement defensive driving training'
        WHEN AVG(safety_score) BETWEEN 80 AND 90 
        THEN 'Medium priority: Targeted coaching for high-risk drivers'
        WHEN AVG(safety_score) >= 90 
        THEN 'Negotiate with insurers for group discount'
        ELSE 'Maintain current program'
    END as insurance_optimization_strategy,
    -- Expected ROI from driver training
    CASE 
        WHEN AVG(safety_score) < 80 
        THEN SUM(max_potential_savings) * 0.5  -- 50% of potential savings achievable
        WHEN AVG(safety_score) BETWEEN 80 AND 90 
        THEN SUM(max_potential_savings) * 0.3  -- 30% of potential savings achievable
        ELSE 0
    END as expected_annual_savings
FROM premium_calculation
GROUP BY vehicle_type, vehicle_class
ORDER BY total_premium_overpayment DESC;

-- ============================================================================
-- 4. SUSTAINABILITY & ENVIRONMENTAL INSIGHTS
-- ============================================================================

-- Insight 4.1: Carbon Footprint Reduction Roadmap
WITH fleet_emissions AS (
    SELECT 
        v.vehicle_type,
        v.vehicle_class,
        COUNT(DISTINCT v.vehid) as vehicle_count,
        SUM(t.trip_distance_km) as annual_distance_km,
        SUM(t.fuel_consumed_l) as annual_fuel_consumed_l,
        -- CO2 emissions
        SUM(t.fuel_consumed_l) * 
            CASE v.engine_type
                WHEN 'Gasoline' THEN 2.31
                WHEN 'Diesel' THEN 2.68
                WHEN 'Hybrid' THEN 1.85
                ELSE 0
            END as annual_co2_emissions_kg,
        -- Efficiency metrics
        AVG(t.fuel_efficiency_kmpl) as avg_efficiency
    FROM telemetry_data t
    JOIN vehicles v ON t.vehid = v.vehid
    WHERE t.timestamp >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY v.vehicle_type, v.vehicle_class
),
replacement_scenarios AS (
    SELECT 
        *,
        -- Scenario 1: Replace oldest 20% with HEVs
        vehicle_count * 0.2 as hev_replacements_year1,
        -- Scenario 2: Replace next 30% with HEVs in year 2
        vehicle_count * 0.3 as hev_replacements_year2,
        -- Scenario 3: Replace remaining with EVs in year 3
        vehicle_count * 0.5 as ev_replacements_year3,
        -- CO2 reduction from HEV replacement (40% reduction)
        annual_co2_emissions_kg * 0.2 * 0.4 as year1_co2_reduction,
        -- Additional reduction in year 2
        annual_co2_emissions_kg * 0.3 * 0.4 as year2_co2_reduction,
        -- EV replacement (80% reduction from ICE baseline)
        annual_co2_emissions_kg * 0.5 * 0.8 as year3_co2_reduction
    FROM fleet_emissions
    WHERE vehicle_type IN ('ICE', 'HEV')
)
SELECT 
    vehicle_class,
    vehicle_count,
    annual_co2_emissions_kg,
    ROUND(annual_co2_emissions_kg / 1000, 1) as annual_co2_emissions_tonnes,
    -- 3-year transition plan
    hev_replacements_year1 as year1_hev_replacements,
    hev_replacements_year2 as year2_hev_replacements,
    ev_replacements_year3 as year3_ev_replacements,
    -- Cumulative CO2 reduction
    year1_co2_reduction as year1_co2_reduction_kg,
    year1_co2_reduction + year2_co2_reduction as year2_cumulative_co2_reduction_kg,
    year1_co2_reduction + year2_co2_reduction + year3_co2_reduction as year3_cumulative_co2_reduction_kg,
    -- Percentage reduction
    ROUND((year1_co2_reduction + year2_co2_reduction + year3_co2_reduction) / 
          annual_co2_emissions_kg * 100, 1) as total_co2_reduction_percent,
    -- Cost implications (simplified)
    hev_replacements_year1 * 5000 as year1_investment,
    hev_replacements_year2 * 5000 as year2_investment,
    ev_replacements_year3 * 10000 as year3_investment,
    -- Fuel cost savings (estimated 30% with HEV, 70% with EV)
    annual_fuel_consumed_l * 1.2 * 0.2 * 0.3 as year1_fuel_savings,
    annual_fuel_consumed_l * 1.2 * 0.3 * 0.3 as year2_fuel_savings,
    annual_fuel_consumed_l * 1.2 * 0.5 * 0.7 as year3_fuel_savings,
    -- Payback period
    CASE 
        WHEN (annual_fuel_consumed_l * 1.2 * 0.2 * 0.3) > 0 
        THEN (hev_replacements_year1 * 5000) / (annual_fuel_consumed_l * 1.2 * 0.2 * 0.3)
        ELSE NULL
    END as year1_payback_years
FROM replacement_scenarios
ORDER BY annual_co2_emissions_kg DESC;

-- ============================================================================
-- 5. RISK MANAGEMENT INSIGHTS
-- ============================================================================

-- Insight 5.1: Safety Risk Assessment and Mitigation
WITH safety_risk_analysis AS (
    SELECT 
        t.vehid,
        v.vehicle_type,
        v.vehicle_class,
        -- Risk indicators
        COUNT(*) as trip_count,
        AVG(t.avg_speed_kmph) as avg_speed,
        MAX(t.avg_speed_kmph) as max_speed,
        SUM(t.harsh_acceleration_count) as total_harsh_acceleration,
        SUM(t.harsh_braking_count) as total_harsh_braking,
        SUM(t.speeding_count) as total_speeding_events,
        COUNT(CASE WHEN t.avg_speed_kmph > 120 THEN 1 END) as excessive_speed_trips,
        COUNT(CASE WHEN t.weather_condition IN ('Rain', 'Snow', 'Fog') THEN 1 END) as adverse_weather_trips,
        COUNT(CASE WHEN EXTRACT(HOUR FROM t.timestamp) BETWEEN 22 AND 5 THEN 1 END) as night_trips,
        -- Driver behavior
        AVG(t.driver_behavior_score) as avg_driver_score,
        -- Vehicle condition indicators
        AVG(CASE WHEN t.avg_engine_temp_c > 110 THEN 1.0 ELSE 0.0 END) as overheating_frequency,
        COUNT(CASE WHEN t.has_fault THEN 1 END) as fault_count
    FROM telemetry_data t
    JOIN vehicles v ON t.vehid = v.vehid
    WHERE t.timestamp >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY t.vehid, v.vehicle_type, v.vehicle_class
),
risk_scoring AS (
    SELECT 
        *,
        -- Risk score calculation (higher = higher risk)
        CASE 
            WHEN total_speeding_events > 20 THEN 3
            WHEN total_speeding_events > 10 THEN 2
            WHEN total_speeding_events > 5 THEN 1
            ELSE 0
        END +
        CASE 
            WHEN total_harsh_braking > 15 THEN 3
            WHEN total_harsh_braking > 8 THEN 2
            WHEN total_harsh_braking > 3 THEN 1
            ELSE 0
        END +
        CASE 
            WHEN night_trips > trip_count * 0.3 THEN 2
            WHEN night_trips > trip_count * 0.1 THEN 1
            ELSE 0
        END +
        CASE 
            WHEN adverse_weather_trips > trip_count * 0.2 THEN 2
            WHEN adverse_weather_trips > trip_count * 0.1 THEN 1
            ELSE 0
        END +
        CASE 
            WHEN fault_count > 5 THEN 3
            WHEN fault_count > 2 THEN 2
            WHEN fault_count > 0 THEN 1
            ELSE 0
        END as risk_score,
        -- Risk category
        CASE 
            WHEN avg_driver_score < 0.7 THEN 'HIGH RISK'
            WHEN avg_driver_score < 0.8 THEN 'MEDIUM RISK'
            ELSE 'LOW RISK'
        END as driver_risk_category
    FROM safety_risk_analysis
)
SELECT 
    vehicle_type,
    vehicle_class,
    COUNT(*) as vehicle_count,
    AVG(risk_score) as avg_risk_score,
    COUNT(CASE WHEN risk_score >= 8 THEN 1 END) as high_risk_vehicles,
    COUNT(CASE WHEN risk_score BETWEEN 5 AND 7 THEN 1 END) as medium_risk_vehicles,
    COUNT(CASE WHEN risk_score <= 4 THEN 1 END) as low_risk_vehicles,
    AVG(avg_driver_score) as avg_driver_score,
    AVG(total_speeding_events) as avg_speeding_events,
    AVG(total_harsh_braking) as avg_harsh_braking,
    -- Risk mitigation recommendations
    CASE 
        WHEN AVG(risk_score) >= 7 THEN 'Immediate intervention required: Review driver assignments, schedule safety training'
        WHEN AVG(risk_score) >= 5 THEN 'Priority intervention: Install telematics alerts, implement coaching program'
        WHEN AVG(risk_score) >= 3 THEN 'Standard monitoring: Regular safety reviews, maintain current protocols'
        ELSE 'Low risk: Continue current practices'
    END as risk_mitigation_strategy,
    -- Expected insurance impact
    CASE 
        WHEN AVG(risk_score) >= 7 THEN '30-50% premium increase likely'
        WHEN AVG(risk_score) >= 5 THEN '10-30% premium increase possible'
        WHEN AVG(risk_score) >= 3 THEN 'Minimal impact'
        ELSE 'Potential for premium reduction'
    END as insurance_impact
FROM risk_scoring
GROUP BY vehicle_type, vehicle_class
ORDER BY avg_risk_score DESC;

-- ============================================================================
-- 6. STRATEGIC DECISION SUPPORT QUERIES
-- ============================================================================

-- Insight 6.1: Vehicle Replacement Priority Matrix
WITH vehicle_health_index AS (
    SELECT 
        v.vehid,
        v.vehicle_type,
        v.vehicle_class,
        v.manufacture_year,
        EXTRACT(YEAR FROM CURRENT_DATE) - v.manufacture_year as vehicle_age,
        v.odometer_km,
        -- Health indicators
        COUNT(DISTINCT mh.maintenance_id) as total_maintenance_events,
        COALESCE(SUM(mh.cost), 0) as total_maintenance_cost,
        AVG(t.fuel_efficiency_kmpl) as current_efficiency,
        AVG(eb.mixed_efficiency_kmpl) as benchmark_efficiency,
        COUNT(CASE WHEN t.has_fault THEN 1 END) as fault_count_last_90_days,
        -- Cost indicators
        AVG(t.fuel_consumed_l / NULLIF(t.trip_distance_km, 0)) * 1.2 * 100 as cost_per_100km,
        -- Utilization
        COUNT(DISTINCT DATE(t.timestamp)) as active_days_last_90,
        SUM(t.trip_distance_km) as distance_last_90_days
    FROM vehicles v
    LEFT JOIN telemetry_data t ON v.vehid = t.vehid 
        AND t.timestamp >= CURRENT_DATE - INTERVAL '90 days'
    LEFT JOIN maintenance_history mh ON v.vehid = mh.vehid 
        AND mh.maintenance_date >= CURRENT_DATE - INTERVAL '365 days'
    LEFT JOIN efficiency_benchmarks eb ON v.vehicle_class = eb.vehicle_class 
        AND v.engine_type = eb.engine_type
        AND v.displacement_l BETWEEN 
            SPLIT_PART(eb.displacement_range, '-', 1)::NUMERIC 
            AND SPLIT_PART(eb.displacement_range, '-', 2)::NUMERIC
    GROUP BY v.vehid, v.vehicle_type, v.vehicle_class, v.manufacture_year, 
             v.odometer_km, eb.mixed_efficiency_kmpl
),
replacement_scoring AS (
    SELECT 
        *,
        -- Age score (higher = older)
        CASE 
            WHEN vehicle_age > 10 THEN 3
            WHEN vehicle_age > 7 THEN 2
            WHEN vehicle_age > 5 THEN 1
            ELSE 0
        END as age_score,
        -- Maintenance cost score
        CASE 
            WHEN total_maintenance_cost > 5000 THEN 3
            WHEN total_maintenance_cost > 3000 THEN 2
            WHEN total_maintenance_cost > 1000 THEN 1
            ELSE 0
        END as maintenance_cost_score,
        -- Efficiency score
        CASE 
            WHEN current_efficiency < benchmark_efficiency * 0.8 THEN 3
            WHEN current_efficiency < benchmark_efficiency * 0.9 THEN 2
            WHEN current_efficiency < benchmark_efficiency THEN 1
            ELSE 0
        END as efficiency_score,
        -- Reliability score
        CASE 
            WHEN fault_count_last_90_days > 5 THEN 3
            WHEN fault_count_last_90_days > 2 THEN 2
            WHEN fault_count_last_90_days > 0 THEN 1
            ELSE 0
        END as reliability_score,
        -- Utilization score (higher utilization = higher priority to keep/replace with better)
        CASE 
            WHEN distance_last_90_days > 10000 THEN 3
            WHEN distance_last_90_days > 5000 THEN 2
            WHEN distance_last_90_days > 1000 THEN 1
            ELSE 0
        END as utilization_score
    FROM vehicle_health_index
)
SELECT 
    vehicle_type,
    vehicle_class,
    COUNT(*) as vehicle_count,
    AVG(vehicle_age) as avg_vehicle_age,
    AVG(odometer_km) as avg_odometer_km,
    AVG(total_maintenance_cost) as avg_annual_maintenance_cost,
    AVG(current_efficiency) as avg_current_efficiency,
    AVG(benchmark_efficiency) as avg_benchmark_efficiency,
    -- Replacement priority scores
    AVG(age_score + maintenance_cost_score + efficiency_score + reliability_score) as avg_replacement_score,
    COUNT(CASE WHEN (age_score + maintenance_cost_score + efficiency_score + reliability_score) >= 8 THEN 1 END) as high_priority_replacements,
    COUNT(CASE WHEN (age_score + maintenance_cost_score + efficiency_score + reliability_score) BETWEEN 5 AND 7 THEN 1 END) as medium_priority_replacements,
    COUNT(CASE WHEN (age_score + maintenance_cost_score + efficiency_score + reliability_score) <= 4 THEN 1 END) as low_priority_replacements,
    -- Strategic recommendations
    CASE 
        WHEN AVG(utilization_score) >= 2 AND AVG(replacement_score) >= 6 THEN 'High utilization, high maintenance: Replace with new efficient model'
        WHEN AVG(utilization_score) >= 2 AND AVG(replacement_score) < 6 THEN 'High utilization, good condition: Maintain and monitor'
        WHEN AVG(utilization_score) < 2 AND AVG(replacement_score) >= 6 THEN 'Low utilization, poor condition: Consider disposal'
        ELSE 'Low utilization, good condition: Retain as backup'
    END as strategic_recommendation,
    -- Budget impact
    COUNT(CASE WHEN (age_score + maintenance_cost_score + efficiency_score + reliability_score) >= 8 THEN 1 END) * 30000 as estimated_replacement_cost_high_priority,
    COUNT(CASE WHEN (age_score + maintenance_cost_score + efficiency_score + reliability_score) BETWEEN 5 AND 7 THEN 1 END) * 30000 as estimated_replacement_cost_medium_priority
FROM replacement_scoring
GROUP BY vehicle_type, vehicle_class
ORDER BY avg_replacement_score DESC;
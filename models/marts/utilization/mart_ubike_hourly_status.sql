{{
    config(
        materialized='table',
        partition_by={
            'field':'date',
            'data_type':'date'
        },
        cluster_by=['station_id_key','hour_of_day']
    )
}}


WITH clean_data AS(
    SELECT
        *
    FROM{{ref('hourly_status')}}
),

station_metrics AS(
    SELECT
        date,
        hour_of_day,
        station_id_key,

        --KPI: 平均利用率
        AVG(available_bikes/total_docks) AS avg_utilization_rate,
        SUM(CASE WHEN available_bikes<=2 THEN 1 ELSE 0 END) AS count_low_bikes,
        SUM(CASE WHEN empty_docks<=2 THEN 1 ELSE 0 END) AS count_low_docks,
        
        COUNT(1) AS total_observations_count
    FROM clean_data
    GROUP BY 
        1,2,3
    HAVING 
        total_observations_count>0
)

SELECT
    T1.station_id_key,
    T1.date,
    T1.hour_of_day,

    T2.district,
    T2.station_name,

    T1.avg_utilization_rate,
    SAFE_DIVIDE(T1.count_low_bikes, T1.total_observations_count) AS low_bikes_ratio,
    SAFE_DIVIDE(T1.count_low_docks, T1.total_observations_count) AS low_docks_ratio,
    
    T1.total_observations_count

FROM
    station_metrics AS T1
LEFT JOIN
    {{ref('dim_ubike_stations')}} AS T2
    ON T1.station_id_key= T2.station_id

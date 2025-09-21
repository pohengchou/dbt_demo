{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('ubike_staging', 'raw_weather_data') }}
),

flattened AS (
    SELECT
        StationId AS station_id,
        StationName AS station_name,
        
        GeoInfo.TownName AS town_name,
        GeoInfo.CountyName AS county_name,
        CAST(GeoInfo.TownCode AS INT64) AS town_code,   
        CAST(GeoInfo.CountyCode AS INT64) AS county_code, 
        CAST(GeoInfo.StationAltitude AS FLOAT64) AS station_altitude,
        
        coordinate.CoordinateName AS coordinate_name,
        CAST(coordinate.StationLatitude AS FLOAT64) AS latitude,
        CAST(coordinate.StationLongitude AS FLOAT64) AS longitude,
        
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%z', ObsTime.DateTime) AS observation_timestamp,
        
        WeatherElement.VisibilityDescription AS visibility_description,
        CAST(WeatherElement.AirPressure AS FLOAT64) AS air_pressure,
        CAST(WeatherElement.AirTemperature AS FLOAT64) AS air_temperature,
        CAST(WeatherElement.RelativeHumidity AS FLOAT64) AS relative_humidity,
        CAST(WeatherElement.WindSpeed AS FLOAT64) AS wind_speed,
        CAST(WeatherElement.WindDirection AS FLOAT64) AS wind_direction,
        CAST(WeatherElement.UVIndex AS INT64) AS uv_index,
        CAST(WeatherElement.SunshineDuration AS FLOAT64) AS sunshine_duration,
        CAST(WeatherElement.Precipitation AS FLOAT64) AS precipitation,
        WeatherElement.Weather AS weather_description,
        
        CAST(WeatherElement.DailyExtreme.DailyLow.TemperatureInfo.AirTemperature AS FLOAT64) AS daily_low_temp,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%z', WeatherElement.DailyExtreme.DailyLow.TemperatureInfo.Occurred_at.DateTime) AS daily_low_temp_at,
        
        CAST(WeatherElement.DailyExtreme.DailyHigh.TemperatureInfo.AirTemperature AS FLOAT64) AS daily_high_temp,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%z', WeatherElement.DailyExtreme.DailyHigh.TemperatureInfo.Occurred_at.DateTime) AS daily_high_temp_at,
        
        CAST(WeatherElement.GustInfo.PeakGustSpeed AS FLOAT64) AS peak_gust_speed,
        CAST(WeatherElement.GustInfo.Occurred_at.WindDirection AS FLOAT64) AS gust_direction,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%z', WeatherElement.GustInfo.Occurred_at.DateTime) AS gust_occurred_at,
        
        CAST(WeatherElement.Max10MinAverage.WindSpeed AS FLOAT64) AS max_10min_avg_wind_speed,
        CAST(WeatherElement.Max10MinAverage.Occurred_at.WindDirection AS FLOAT64) AS max_10min_avg_wind_direction,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%z', WeatherElement.Max10MinAverage.Occurred_at.DateTime) AS max_10min_avg_wind_at,
        
    FROM
        source,
        UNNEST(source.GeoInfo.Coordinates) AS coordinate
)

SELECT * FROM flattened
WHERE coordinate_name = 'WGS84'
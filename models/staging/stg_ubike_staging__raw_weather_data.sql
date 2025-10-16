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
        
        NULLIF(GeoInfo.TownCode, -99) AS town_code,   
        NULLIF(GeoInfo.CountyCode, -99) AS county_code, 
        NULLIF(GeoInfo.StationAltitude, -99) AS station_altitude,
        
        coordinate.CoordinateName AS coordinate_name,
        NULLIF(coordinate.StationLatitude, -99) AS latitude,
        NULLIF(coordinate.StationLongitude, -99) AS longitude,
        
        ObsTime.DateTime AS observation_timestamp,
        
        NULLIF(WeatherElement.VisibilityDescription, '-99') AS visibility_description,
        
        NULLIF(WeatherElement.AirTemperature, -99) AS air_temperature,
        NULLIF(WeatherElement.AirPressure, -99) AS air_pressure,
        NULLIF(WeatherElement.RelativeHumidity, -99) AS relative_humidity,
        NULLIF(WeatherElement.WindSpeed, -99) AS wind_speed,
        NULLIF(WeatherElement.WindDirection, -99) AS wind_direction,
        NULLIF(WeatherElement.UVIndex, -99) AS uv_index,
        NULLIF(WeatherElement.SunshineDuration, -99) AS sunshine_duration,
        NULLIF(WeatherElement.NOW.Precipitation, -99) AS precipitation,
        NULLIF(WeatherElement.Weather, '-99') AS weather_description,
        
        NULLIF(WeatherElement.DailyExtreme.DailyLow.TemperatureInfo.AirTemperature, -99) AS daily_low_temp,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',
          NULLIF(CAST(WeatherElement.DailyExtreme.DailyLow.TemperatureInfo.Occurred_at.DateTime AS STRING), '-99')
        ) AS daily_low_temp_at,
        
        NULLIF(WeatherElement.DailyExtreme.DailyHigh.TemperatureInfo.AirTemperature, -99) AS daily_high_temp,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',
          NULLIF(CAST(WeatherElement.DailyExtreme.DailyHigh.TemperatureInfo.Occurred_at.DateTime AS STRING), '-99')
        ) AS daily_high_temp_at,
        
        NULLIF(WeatherElement.GustInfo.PeakGustSpeed, -99) AS peak_gust_speed,
        NULLIF(WeatherElement.GustInfo.Occurred_at.WindDirection, -99) AS gust_direction,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',
          NULLIF(CAST(WeatherElement.GustInfo.Occurred_at.DateTime AS STRING), '-99')
        ) AS gust_occurred_at,
        
        NULLIF(WeatherElement.Max10MinAverage.WindSpeed, -99) AS max_10min_avg_wind_speed,
        NULLIF(WeatherElement.Max10MinAverage.Occurred_at.WindDirection, -99) AS max_10min_avg_wind_direction,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',
          NULLIF(CAST(WeatherElement.Max10MinAverage.Occurred_at.DateTime AS STRING), '-99')
        ) AS max_10min_avg_wind_at
        
    FROM
        source,
        UNNEST(source.GeoInfo.Coordinates) AS coordinate
)

SELECT * FROM flattened
WHERE coordinate_name = 'WGS84'
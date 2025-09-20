{{ config(materialized='view') }}

WITH raw AS (
  SELECT *
  FROM {{ source('ubike_staging', 'raw_weather_data') }}
)

SELECT
  -- 時間處理
  DATETIME(TIMESTAMP(raw.ObsTime.DateTime), "Asia/Taipei") AS obs_time_local,

  -- 基本地理資訊
  raw.GeoInfo.CountyName AS county_name,
  raw.GeoInfo.TownName AS town_name,
  CAST(raw.GeoInfo.Coordinates[OFFSET(0)].StationLatitude AS FLOAT64) AS lat,
  CAST(raw.GeoInfo.Coordinates[OFFSET(0)].StationLongitude AS FLOAT64) AS lng,

  -- 天氣指標（-99 → NULL）
  NULLIF(raw.WeatherElement.AirTemperature, -99) AS air_temperature,
  NULLIF(raw.WeatherElement.RelativeHumidity, -99) AS humidity,
  NULLIF(raw.WeatherElement.WindSpeed, -99) AS wind_speed,
  raw.WeatherElement.Weather AS weather,
  NULLIF(raw.WeatherElement.Precipitation, -99) AS precipitation,
  NULLIF(raw.WeatherElement.AirPressure, -99) AS air_pressure
FROM raw
WHERE raw.ObsTime.DateTime IS NOT NULL
{{ config(materialized='view') }}

WITH source AS (
  SELECT *
  FROM {{ source('ubike_staging', 'raw_ubike_data') }}
),

unnested AS (
  SELECT
    source.updated_at,
    unnested_retVal.*
  FROM source
  , UNNEST(source.retVal) AS unnested_retVal
),

renamed AS (
  SELECT
    updated_at AS updated_at_local, -- updated_at 本身就是 DATETIME 格式，無需轉換
    PARSE_DATETIME('%Y%m%d%H%M%S', CAST(mday AS STRING)) AS last_updated_at, -- 使用新的格式字串
    sno AS station_id,
    sna AS station_name,
    sbi AS available_bikes,
    bemp AS empty_docks,
    tot AS total_docks,
    sarea AS district,
    ar AS address,
    lat AS latitude,
    lng AS longitude,
    act AS is_active
  FROM unnested
)

SELECT * FROM renamed
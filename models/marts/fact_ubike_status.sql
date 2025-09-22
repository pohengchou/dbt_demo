SELECT
    -- 使用 FARM_FINGERPRINT 將多個欄位組合成一個唯一的代理鍵
    FARM_FINGERPRINT(
        CONCAT(
            CAST(station_id AS STRING),
            CAST(api_request_at AS STRING)
        )
    ) AS Surrogate_Key,
    -- 外來鍵，用於連接維度表
    station_id AS station_id_key,
    api_request_at,
    data_updated_at,
    available_bikes,
    empty_docks,
    total_docks,
    electric_bikes,
    normal_bikes,
    is_active

FROM
    {{ ref('stg_ubike_staging__raw_ubike_data') }}
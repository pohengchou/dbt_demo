SELECT  
    --主鍵(Primary Key)
    station_id  AS station_id_key,
    
    -- 描述性維度：這些欄位用來分類和描述站點
    station_id,
    station_name,
    district,
    address,
    latitude,
    longitude,
    

FROM {{ref('stg_ubike_staging__raw_ubike_data')}}

GROUP BY
    station_id,
    station_name,
    district,
    address,
    latitude,
    longitude
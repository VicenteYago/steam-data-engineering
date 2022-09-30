{{ config(materialized='view') }}

select CAST(original.steam_appid as integer ) as appid,
        STRUCT <name ARRAY<STRING>,
                id   ARRAY<INTEGER>> (ARRAY_AGG(fixed.genres_name),
                                      ARRAY_AGG(fixed.genres_id)) as genres

FROM {{source('raw', 'steam_store_data')}} as original
       JOIN 
            (
            select 
                steam_appid,
                JSON_EXTRACT_SCALAR(genres, '$.description') as genres_name,
                CAST( REGEXP_EXTRACT(genres, r"(\d+)") as integer) as genres_id 

            from {{source('raw', 'steam_store_data')}},
                unnest(json_query_array(genres)) as genres
            ) fixed ON original.steam_appid = fixed.steam_appid

GROUP BY original.steam_appid
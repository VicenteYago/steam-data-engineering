{{ config(materialized='view') }}

select CAST(original.steam_appid as integer ) as appid,
       ARRAY_AGG(fixed.genres_name IGNORE NULLS) as genres_name ,
       ARRAY_AGG(fixed.genres_id IGNORE NULLS) as genres_id

FROM {{source('staging', 'steam_store_data')}} as original
       JOIN 
            (
            select 
                steam_appid,
                SAFE_CAST(JSON_QUERY(genres, '$.description') as string) as genres_name,
                CAST( REGEXP_EXTRACT(genres, r"(\d+)") as integer) as genres_id 

            from `steam-data-engineering-gcp.steam_stg.steam_store_data`,
                unnest(json_query_array(genres)) as genres
            ) fixed ON original.steam_appid = fixed.steam_appid

GROUP BY original.steam_appid
{{ config(materialized='view') }}

select CAST(original.steam_appid as integer ) as appid,
       ARRAY_AGG(fixed.categories_name) as categories_name ,
       ARRAY_AGG(fixed.categories_id) as categories_id

FROM {{source('staging', 'steam_store_data')}} as original
       JOIN 
            (
            select 
                steam_appid,
                CAST(JSON_QUERY(categories, '$.description') as string) as categories_name,
                CAST(JSON_QUERY(categories, '$.id') as integer) as categories_id

            from {{source('staging', 'steam_store_data')}},
                unnest(json_query_array(categories)) as categories
            ) fixed ON original.steam_appid = fixed.steam_appid

GROUP BY original.steam_appid
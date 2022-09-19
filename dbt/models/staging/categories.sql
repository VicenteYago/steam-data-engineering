{{ config(materialized='view') }}

select CAST(original.steam_appid as integer ) as appid,
        STRUCT <name ARRAY<STRING>,
                id   ARRAY<INTEGER>> (ARRAY_AGG(fixed.categories_name),
                                      ARRAY_AGG(fixed.categories_id)) as categories

FROM {{source('staging', 'steam_store_data')}} as original
       JOIN 
            (
            select 
                steam_appid,
                JSON_EXTRACT_SCALAR(categories, '$.description') as categories_name,
                CAST(JSON_QUERY(categories, '$.id') as integer) as categories_id

            from {{source('staging', 'steam_store_data')}},
                unnest(json_query_array(categories)) as categories
            ) fixed ON original.steam_appid = fixed.steam_appid

GROUP BY original.steam_appid
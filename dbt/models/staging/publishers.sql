{{ config(materialized='view') }}

select CAST(original.steam_appid as integer ) as appid,
            ARRAY_AGG( distinct {{ fix_strings('fixed.pubs') }}) as pubs

FROM {{source('raw', 'steam_store_data')}} as original
       JOIN 
            (
            select 
                steam_appid,
                JSON_EXTRACT_SCALAR(pubs) as pubs
            from {{source('raw', 'steam_store_data')}},
                unnest(json_query_array(publishers)) as pubs
            ) fixed ON original.steam_appid = fixed.steam_appid

GROUP BY original.steam_appid
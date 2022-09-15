{{ config(materialized='view') }}


SELECT
   type, 
   name, 
   SAFE_CAST(steam_appid as integer) as appid, 
   JSON_QUERY_ARRAY(dlc) as dlcs,
   SAFE_CAST(required_age as integer) as required_age,
   SAFE_CAST(is_free as BOOL) as is_free,
   JSON_QUERY_ARRAY(developers) as developers,
   JSON_QUERY_ARRAY(publishers) as publishers,
   JSON_EXTRACT({{ fix_bools('platforms') }}, '$.windows') as platform_windows,
   JSON_EXTRACT({{ fix_bools('platforms') }}, '$.mac') as platform_mac,
   JSON_EXTRACT({{ fix_bools('platforms') }}, '$.linux') as platform_linux,
   JSON_EXTRACT(metacritic, '$.score') as metacritic, 
   JSON_EXTRACT(recommendations, '$.total') as num_recommendations, 
   JSON_EXTRACT({{ fix_bools('release_date') }}, '$.coming_soon') as coming_soon,
   JSON_EXTRACT( {{ fix_bools('release_date') }}, '$.date') as release_date, --todo
   controller_support,
   drm_notice,
   JSON_EXTRACT(TRIM(demos, '[]'), '$.appid') as demos_appid


FROM {{source('staging', 'steam_store_data')}}
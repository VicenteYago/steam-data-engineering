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

   CAST(JSON_EXTRACT({{ fix_bools('platforms') }}, '$.windows') as boolean) as platform_windows,
   CAST(JSON_EXTRACT({{ fix_bools('platforms') }}, '$.mac') as boolean) as platform_mac,
   CAST(JSON_EXTRACT({{ fix_bools('platforms') }}, '$.linux') as boolean) as platform_linux,
   
   JSON_EXTRACT(metacritic, '$.score') as metacritic, 
   JSON_EXTRACT(recommendations, '$.total') as num_recommendations, 

   CAST(JSON_EXTRACT({{ fix_bools('release_date') }}, '$.coming_soon') as boolean) as coming_soon,
   CAST(REGEXP_EXTRACT(JSON_EXTRACT( {{ fix_bools('release_date') }}, '$.date'), r"(\d\d\d\d)") as integer) as release_year,
   CASE WHEN controller_support = 'full' THEN TRUE 
             ELSE FALSE END as controller_support,
   drm_notice,
   CAST(JSON_EXTRACT(TRIM(demos, '[]'), '$.appid') as integer) as demos_appid


FROM {{source('staging', 'steam_store_data')}}
{{ config(
   materialized='incremental', 
   unique_key='appid'
) }}


SELECT
   type, 
   name, 
   SAFE_CAST(ssd.steam_appid as integer) as appid, 
   JSON_QUERY_ARRAY(dlc) as dlcs,
   SAFE_CAST(required_age as integer) as required_age,
   SAFE_CAST(is_free as BOOL) as is_free,
   developers.devs as developers,

   CAST(JSON_EXTRACT({{ fix_bools('platforms') }}, '$.windows') as boolean) as platform_windows,
   CAST(JSON_EXTRACT({{ fix_bools('platforms') }}, '$.mac') as boolean) as platform_mac,
   CAST(JSON_EXTRACT({{ fix_bools('platforms') }}, '$.linux') as boolean) as platform_linux,
   
   CAST(JSON_EXTRACT(metacritic, '$.score') as integer) as metacritic , 
   CAST(JSON_EXTRACT(recommendations, '$.total') as integer ) as num_recommendations, 

   CAST(JSON_EXTRACT({{ fix_bools('release_date') }}, '$.coming_soon') as boolean) as coming_soon,
   CAST(REGEXP_EXTRACT(JSON_EXTRACT( {{ fix_bools('release_date') }}, '$.date'), r"(\d\d\d\d)") as integer) as release_year,
   CASE WHEN controller_support = 'full' THEN TRUE 
             ELSE FALSE END as controller_support,
   drm_notice,
   CAST(JSON_EXTRACT(recommendations, "$.total")as integer)  as recommendations,
   CAST(JSON_EXTRACT(TRIM(demos, '[]'), '$.appid') as integer) as demos_appid,
   categories.categories as categories,
   genres.genres as genres,
   publishers.pubs as publishers
   
FROM 

   (
      (
         (
            {{source('raw', 'steam_store_data')}} ssd LEFT JOIN
            {{source('staging', 'genres')}} genres ON CAST(ssd.steam_appid as integer) = genres.appid
         )  LEFT JOIN {{source('staging', 'developers')}} developers ON genres.appid = developers.appid  
      )  LEFT JOIN {{source('staging', 'categories')}} categories ON genres.appid = categories.appid
   ) LEFT JOIN {{source('staging', 'publishers')}} publishers ON categories.appid = publishers.appid





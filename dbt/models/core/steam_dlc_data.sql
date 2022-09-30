{{ config(materialized='table') }}

select
    SAFE_CAST(steam_appid as integer) as appid,
    type,
    name,
   JSON_EXTRACT_STRING_ARRAY(developers) as developers,
   JSON_EXTRACT_STRING_ARRAY(publishers) as publishers,

   STRUCT<currency STRING, price_final INTEGER,  price_discount INTEGER>(
    JSON_EXTRACT_SCALAR(price_overview, '$.currency'),
    CAST(JSON_EXTRACT(price_overview, '$.final') as integer),
    CAST(JSON_EXTRACT(price_overview, '$.discount_percent') as integer)
   ) as price, 

   CAST(JSON_EXTRACT(metacritic, '$.score') as integer) as score,

   CAST(JSON_EXTRACT({{ fix_bools('release_date') }}, '$.coming_soon') as boolean) as coming_soon,
   CAST(REGEXP_EXTRACT(JSON_EXTRACT( {{ fix_bools('release_date') }}, '$.date'), r"(\d\d\d\d)") as integer) as release_year,
   CAST(JSON_EXTRACT(recommendations, "$.total")as integer)  as recommendations,

from  {{source('raw', 'steam_dlc_data')}}

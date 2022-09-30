{{ config(materialized='view') }}


select
    SAFE_CAST(appid as integer) as appid,
    name,
    developer,
    publisher,
    score_rank,
    SAFE_CAST(positive as integer) as positive,
    SAFE_CAST(negative as integer) as negative,
    SAFE_CAST(userscore as integer) as userscore,
    SAFE_CAST( REPLACE( REGEXP_EXTRACT(owners, r'(.*)\.\.'), ",", "") AS INTEGER) AS owners_low,
    SAFE_CAST( REPLACE( REGEXP_EXTRACT(owners, r'\.\.(.*)'), ",", "") AS INTEGER) AS owners_high,
    SAFE_CAST(average_forever as integer) as average_forever,
    SAFE_CAST(average_2weeks as integer) as average_2weeks,
    SAFE_CAST(median_forever as integer) as median_forever,
    SAFE_CAST(median_2weeks as integer) as median_2weeks,
    SAFE_CAST(price as decimal)/100 as price,
    SAFE_CAST(initialprice as decimal)/100 as initialprice,
    SAFE_CAST( REPLACE(discount, '.0', '' ) as integer) as discount_percentage,
    SAFE_CAST(ccu as integer) as ccu
from  {{source('raw', 'steam_spy_scrap')}}
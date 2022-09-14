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
    owners,
    SAFE_CAST(average_forever as integer) as average_forever,
    SAFE_CAST(average_2weeks as integer) as average_2weeks,
    SAFE_CAST(median_forever as integer) as median_forever,
    SAFE_CAST(median_2weeks as integer) as median_2weeks,
    SAFE_CAST(price as decimal) as price,
    SAFE_CAST(initialprice as decimal) as initialprice,
    SAFE_CAST(discount as decimal) as discount,
    SAFE_CAST(ccu as integer) as ccu
from  {{source('staging', 'steam_spy_scrap')}}
limit 100


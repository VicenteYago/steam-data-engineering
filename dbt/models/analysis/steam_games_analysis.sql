SELECT appid,
       type,
       name,
       metacritic,
       required_age,
       is_free,
       release_year,
       coming_soon, 
       controller_support,
       demos_appid,
       revenue.low as revenue_low,
       revenue.high as revenue_high,
       drm_notice,
       recommendations,
       negative,
       positive,
       price,
       initialprice,
       discount_percentage,
       ccu
FROM {{ref('steam_games')}} as steam_games

WHERE metacritic is not null

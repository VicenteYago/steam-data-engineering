{{ config(
    materialized='table',
    partition_by={
      "field": "release_year",
      "data_type": "int64",
      "range": {
        "start": 1997,
        "end": 2023,
        "interval": 3
      }
    }
)}}


SELECT steam_store_data.appid,
       steam_store_data.type,
       steam_store_data.name,
       steam_store_data.developers,
       steam_store_data.publishers,
       steam_store_data.dlcs,
       steam_store_data.metacritic,
       steam_store_data.required_age,
       steam_store_data.is_free,
       steam_store_data.release_year,
       steam_store_data.coming_soon, 
       steam_store_data.controller_support,
       steam_store_data.demos_appid,
       steam_store_data.drm_notice,
       steam_store_data.recommendations,
       steam_store_data.categories as categories,
       steam_store_data.genres as genres,
       STRUCT<low integer, high integer>(steam_spy_scrap.owners_low, steam_spy_scrap.owners_high) as owners,
       STRUCT<low decimal, high decimal> (steam_spy_scrap.owners_low * steam_spy_scrap.price,
                                      steam_spy_scrap.owners_high*steam_spy_scrap.price) as revenue,
       STRUCT<windows boolean,
              mac     boolean,
              linux   boolean>(steam_store_data.platform_windows, steam_store_data.platform_mac, steam_store_data.platform_linux) as platforms,
       steam_spy_scrap.negative,
       steam_spy_scrap.positive,
       steam_spy_scrap.price,
       steam_spy_scrap.initialprice,
       steam_spy_scrap.discount_percentage,
       
       steam_spy_scrap.ccu

FROM {{ref('steam_store_data')}} as steam_store_data LEFT JOIN  
          {{ref('steam_spy_scrap')}}  as steam_spy_scrap ON steam_spy_scrap.appid = steam_store_data.appid
WHERE release_year <=2025

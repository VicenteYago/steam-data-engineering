{{ config(materialized='table') }}

SELECT steam_store_data.appid,
       steam_store_data.name,
       steam_store_data.developers,
       steam_store_data.publishers,
       steam_store_data.dlcs,
       steam_store_data.metacritic,
       steam_spy_scrap.owners_low,
       steam_spy_scrap.owners_high
  FROM {{ref('steam_store_data')}} as steam_store_data JOIN  
       {{ref('steam_spy_scrap')}}  as steam_spy_scrap ON steam_spy_scrap.appid = steam_store_data.appid

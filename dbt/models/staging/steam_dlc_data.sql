{{ config(materialized='view') }}

select
    SAFE_CAST(steam_appid as integer) as appid,
    name,
    developers,
    publishers
from  {{source('staging', 'steam_dlc_data')}}

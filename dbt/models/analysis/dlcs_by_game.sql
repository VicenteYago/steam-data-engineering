select appid, metacritic, name, ARRAY_LENGTH(dlcs) as n_dlcs
FROM {{ref('steam_games')}}
WHERE metacritic is not null
ORDER BY n_dlcs DESC
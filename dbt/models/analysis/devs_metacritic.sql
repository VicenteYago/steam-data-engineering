
SELECT 
  developers_unnest,
  appid,
  metacritic
FROM {{ref('steam_games')}},
  UNNEST(developers) as developers_unnest
WHERE metacritic is not null


SELECT 
  appid,
  genres_unnest as genre,
  name,
  metacritic 

FROM {{ref('steam_games')}},
  UNNEST(genres.name) as genres_unnest

WHERE metacritic is not null
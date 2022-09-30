SELECT 

release_year

FROM {{ ref('steam_games') }}

WHERE release_year > 2025
 OR   release_year < 1997
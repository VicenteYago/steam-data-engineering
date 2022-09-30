{{ config(materialized='table') }}


SELECT 

comment_count,			
language,			
received_for_free,			
SAFE_CAST(recommendationid as INTEGER) as recommendationid ,			
review,			
steam_purchase,		
timestamp_created,		
timestamp_updated,			
voted_up,			
votes_funny,			
votes_up,			
SAFE_CAST(weighted_vote_score as DECIMAL) as weighted_vote_score,			
written_during_early_access,		
SAFE_CAST(gameid as INTEGER) as gameid,			
author_last_played,		
author_num_games_owned,			
author_num_reviews,			
author_playtime_at_review,			
author_playtime_forever,			
author_playtime_last_two_weeks,			
CAST(author_steamid as INTEGER) as author_steamid 

from  {{source('raw', 'steam_reviews')}}

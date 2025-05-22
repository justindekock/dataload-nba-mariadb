from time import sleep
from nba_api.stats.endpoints import leaguegamefinder, playerindex, teaminfocommon, playbyplayv3

def game_logs(game_date, game_date_to=None, player_team = 'P', lg = 'NBA'):
    retries = 0
    while retries < 3:
        try:
            df = leaguegamefinder.LeagueGameFinder(
                player_or_team_abbreviation=player_team,
                league_id_nullable= '10' if lg == 'WNBA' else ('20' if lg == 'GNBA' else '00'), 
                date_from_nullable=game_date,
                date_to_nullable=game_date_to if game_date_to else game_date
            ).get_data_frames()[0]
            
            # CRUCIAL -- ADD LG TO THIS DF
            df['lg'] = lg
            
            return df
        except Exception as e:
            print(e)
            sleep(15)
    
    
def active_players():
    retries = 0
    while retries < 3:
        try:
            return playerindex.PlayerIndex(active_nullable=1).get_data_frames()[0]
        except Exception as e:
            print(e)
    
def teams():
    retries = 0
    while retries < 3:
        try:
            return teaminfocommon.TeamInfoCommon()
        except Exception as e:
            print(e)
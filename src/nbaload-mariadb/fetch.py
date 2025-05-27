from time import sleep
from nba_api.stats.endpoints import leaguegamefinder, commonallplayers, playbyplayv3
import pandas as pd
from datetime import datetime as dt

import logs

# TODO - play by play data with playbyplayv3

def get_players(league='all', current=1): # pass 0 to get all players
    lgs = ['NBA', 'WNBA', 'GNBA']
    if dt.now().month not in range(5, 11):
        lgs.remove('WNBA')
        
    if dt.now().month in range(7, 9):
        lgs.remove('NBA')
        
    if dt.now().month in range(5, 10):
        lgs.remove('GNBA')
        
    
    if league != 'all':
        lgs = [league]

    pls = []
    for lg in lgs:
        try:
            raw = commonallplayers.CommonAllPlayers(is_only_current_season=current, 
                                                    league_id='10' if lg == 'WNBA' \
                                                        else ('20' if lg == 'GNBA' else '00')
                                                        ).get_data_frames()[0]    
            
            raw['lg'] = lg
            pl = raw.copy()
            pl = pl.rename(columns={'PERSON_ID': 'player_id', 'DISPLAY_FIRST_LAST': 'player',
                                    'TEAM_ID': 'team_id', 'ROSTERSTATUS': 'active'})
            pl = pl[['player_id', 'player', 'team_id', 'lg', 'active']]
            pls.append(pl.copy())
        except Exception as e:
            logs.append_log(f'ERROR fetching current players: {e}')
            
        
        
    return pd.concat(pls)

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
            
            logs.append_log(f"{lg} {'team' if player_team == 'T' else 'player'} "
                f"game logs fetched for {game_date}: {df.shape[0]} rows")
            
            return df
        except Exception as e:
            print(e)
            logs.append_log(f'ERROR fetching {lg} game logs for {game_date}: {e}')
            sleep(5)
    
# convert start and end date in 01/01/2025 format to list of dates

            
def main():
    players = get_players()
    # print(players)
    
if __name__=='__main__':
        main()
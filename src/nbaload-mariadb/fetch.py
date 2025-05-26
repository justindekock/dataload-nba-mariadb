from time import sleep
from nba_api.stats.endpoints import leaguegamefinder, commonallplayers, commonplayerinfo,\
    playerindex, teaminfocommon, playbyplayv3, playerprofilev2
import pandas as pd
from datetime import datetime, timedelta



# NEED TO MAKE A DECISION - WHEN PLAYER HAS NEW TEAM, INSERT NEW ROW OR UPDATE CURRENT?
# TODO - IDENTIFY PLAYER ROWS THAT NEED TO BE UPDATED: 
# -- IF PLAYER EXISTS IN DB BUT TEAM, ACTIVE, OR LG IS DIFFERENT IN FETCH (NBA/GLEAGUE)
# -- -- UPDATE THE CURRENT ACTIVE FIELD TO 0. THEN INSERT THE NEW RECORD
# 
def get_players(league='all', current=1): # pass 0 to get all players
    lgs = ['NBA', 'WNBA', 'GNBA']
    if league != 'all':
        lgs = [league]

    pls = []
    for lg in lgs:
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
            
            return df
        except Exception as e:
            print(e)
            sleep(15)
    
    
def game_logs_batch(dates, player_team='P', lg='NBA'):
    start_date = datetime.strptime(dates[0], '%m/%d/%Y')
    end_date = datetime.strptime(dates[1], '%m/%d/%Y')
    days = abs(end_date-start_date).days
    
    game_dates = []
    
    for i in range(days):
        date = (start_date + timedelta(i)).strftime('%m/%d/%Y')
        game_dates.append(date)
        
    game_dates.append(dates[1])    
    print(game_dates)
    
def active_players():
    attempt = 0
    while attempt < 3:
        try:
            # return playerindex.PlayerIndex(active_nullable=1).get_data_frames()[0]
            # return playerprofilev2.PlayerProfileV2().get_data_frames()[0]
            return commonallplayers.CommonAllPlayers(is_only_current_season=1, league_id=10).get_data_frames()[0]
            # return commonplayerinfo.CommonPlayerInfo().get_data_frames()[0]
            
        
        except Exception as e:
            attempt+=1
            print(e)
    
def teams():
    retries = 0
    while retries < 3:
        try:
            return teaminfocommon.TeamInfoCommon()
        except Exception as e:
            print(e)
            
            
def main():
    players = current_players()
    # print(players)
    
    #test = teamplayerdashboard.TeamPlayerDashboard()#commonteamroster.CommonTeamRoster()
    #3print(test)
    
if __name__=='__main__':
        main()
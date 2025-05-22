from datetime import datetime, timedelta
from dbamdb import conn
import pandas as pd
import fetch
import clean

def check_for_games(game_date, lg):
    df = fetch.game_logs(game_date, 'T', lg)
    if not df:
        print(f'No {lg} games found for {game_date}')
        return pd.DataFrame()
    return df

def check_all_lgs(game_date):
    dfs = []
    lgs = ['NBA', 'WNBA', 'GNBA']
    for lg in lgs:
        df = fetch.game_logs(game_date, player_team='T', lg=lg)
        if not df.empty:
            dfs.append(df)
        
    return dfs

def main():
    game_date = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
    # TODO - overall first step should be to get current players and make sure their team is correct
    
    # get the raw team game logs for each league
    tm_dfs = check_all_lgs(game_date)
    if not tm_dfs:
        print(f'No games played in any league on {game_date}')
        print('Exiting...')
        return # exit the program    
    
    # combine the dataframes
    all_tm_logs = pd.concat(tm_dfs).reset_index(drop=True)
    
    print(all_tm_logs)
    print('Games found, continuing...')
    
    # if games exist for any of the leagues, pass it to TeamData class
    all_team_data = clean.TeamData(all_tm_logs)

if __name__=='__main__':
    main()
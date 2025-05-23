from datetime import datetime, timedelta
from time import sleep
from dbamdb import conn
import pandas as pd
import fetch
import clean

def df_to_insert_lists(df):
    in_flds = tuple(df.columns.values)
    in_vals = []
    for r in range(df.shape[0]):
        vals = tuple(df.values[r])
        in_vals.append(vals)
    
    return tuple([in_flds, in_vals])


def check_for_games(game_date, lg):
    df = fetch.game_logs(game_date, 'T', lg)
    if not df:
        print(f'No {lg} games found for {game_date}')
        return pd.DataFrame()
    return df

def check_all_lgs(game_date, pl_tm):
    dfs = []
    lgs = ['NBA', 'WNBA', 'GNBA']
    for lg in lgs:
        df = fetch.game_logs(game_date, player_team=pl_tm, lg=lg)
        if not df.empty:
            dfs.append(df)
        
    return dfs

def main():
    timeout = 2
    attempt = 0
    max_attempts = 3
    game_date = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
    # TODO - overall first step should be to get current players and make sure their team is correct
    
    # get the raw team game logs for each league
    tm_dfs = check_all_lgs(game_date, 'T')
    if not tm_dfs:
        print(f'No games played in any league on {game_date}')
        print('Exiting...')
        return # exit the program    
    
    # combine the dataframes
    all_tm_logs = pd.concat(tm_dfs.copy()).reset_index(drop=True)
    
    print(f'Cleaned DF of all game logs (NBA/WNBA/GLEAGUE) from {game_date}')
    # print(all_tm_logs)
    
    # if games exist for any of the leagues, pass it to TeamData class
    all_team_data = clean.TeamData(all_tm_logs)
    # print(all_team_data.game_df)
    
    while attempt < max_attempts:
        #pl_dfs = [0] # sample for testing
        pl_dfs = check_all_lgs(game_date, 'P')
        if len(pl_dfs) != len(tm_dfs): # TODO ALL OF THIS WILL GO IN A FUNCTION IN A LOGGING MODULE
            attempt+=1
            if attempt != (max_attempts): 
                print(f'Player log fetch inconsistent with team log fetch. Retry {attempt}/{max_attempts}')
                sleep(timeout)
            else: # last attempt, indicate failure
                print(f'Inconsistent player/team fetches after {attempt}/{max_attempts} attempts')
                attempt+=1
                 # increment once more, use outsite of loop to indicate failure
        else:    
            print('things work as they should, leaving loop')
            break
    
    if attempt == max_attempts + 1:
        exit_options = ('e', 'q', 'x', 'z')
        errin = input(f'Player fetch failed somewhere! What do you want to do?')
        if errin in exit_options:
            print('good job! bye!')
        else:
            print('wrong input exiting anyway!')
        return
    
    
    all_pl_logs = pd.concat(pl_dfs.copy()).reset_index(drop=True)
    # all_player_data = clean.PlayerData(all_pl_logs)
    # print(all_pl_logs)
    all_player_data = clean.PlayerDataLgcy(all_pl_logs, all_team_data.tgame_df)
    # print(all_player_data.pgame_df)
    # print(all_team_data.season_df)
    
    
    
    
    # INSERT TESTING
    
    # in_flds = tuple(all_team_data.season_df.columns.values)
    # in_vals = []
    # for r in range(all_team_data.season_df.shape[0]):
    #     vals = tuple(all_team_data.season_df.values[r])
    #     in_vals.append(vals)
    
    # print(in_flds)
    # print(in_vals)
    
    # players = fetch.current_players()
    
    
    # CONFIRMED SEASON AND TEAM INSERTS WORK 05/22
    insert_lists = df_to_insert_lists(all_team_data.team_df)    
    db_conn = conn.DBConn('dev')
    db_conn.insert('team', insert_lists[0], insert_lists[1])
    
    # insert_lists = df_to_insert_lists(all_team_data.team_df)    
    # db_conn = conn.DBConn('dev')
    # db_conn.insert('team', insert_lists[0], insert_lists[1])
    
    

    # only return for testing purposes
    return

if __name__=='__main__':
    main()
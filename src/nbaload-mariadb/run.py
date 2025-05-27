from time import sleep
import pandas as pd
from datetime import datetime, timedelta
from dbamdb import conn
import fetch
import logs

def fetch_insert_players(db = 'dev'):
    logs.append_log('Fetching current players, inserting into player_temp to trigger stored procedure sp_update_players...')
    players = fetch.get_players()
    if not players.empty:
        pl_ins_lists = df_to_insert_lists(players)
    
        db_conn = conn.DBConn(db)
        ins_res = db_conn.insert('player_temp', pl_ins_lists[0], pl_ins_lists[1])
        
        for res in ins_res:
            logs.append_log(res)
            
        logs.append_log('Completed player insert/update, delaying then deleting temporary player records...')
        
        sleep(1)
        
        del_res = db_conn.delete_temp_player()
        for res in del_res:
            logs.append_log(res)
    
def list_of_dates(dates):
    start_date = datetime.strptime(dates[0], '%m/%d/%Y')
    end_date = datetime.strptime(dates[1], '%m/%d/%Y')
    days = abs(end_date-start_date).days
    
    game_dates = []
    
    for i in range(days):
        date = (start_date + timedelta(i)).strftime('%m/%d/%Y')
        game_dates.append(date)
        
    game_dates.append(dates[1]) # append the end date
    return game_dates
    
def game_logs_batch(dates, player_team='P'):
    if len(dates) > 1:
        game_dates = list_of_dates(dates)
    else:
        game_dates = dates
        
    dfs = []
    for i, date in enumerate(game_dates):
        
        #df = get_game_logs(date, pl_tm=player_team)
        df = check_all_lgs(date, pl_tm=player_team)
        if df.empty:
            print(f'Empty dataframe {date}')
            continue
        
        dfs.append(df)
        # print(df)
        if (i+1) < len(game_dates) and (i+1) % 10 == 0:
            if (i+1) % 50 == 0:
                delay = 60
            delay = 30
            logs.append_log(f'Fetched date {i+1} of {len(game_dates)} - intentional {delay} second delay to respect rate limiting...')
        else:
            delay = 2
            
        print(f'Fetched date {i+1} of {len(game_dates)} - delaying for {delay} seconds...')
        sleep(delay)
    bigdf = pd.concat(dfs).reset_index(drop=True)
    return bigdf

# returns single df with all games for one date for all leagues
def check_all_lgs(game_date, pl_tm):
    lgs = ['NBA', 'WNBA', 'GNBA']
    dfs = []
    for lg in lgs:
        print(game_date)
        print(lg)
        df = fetch.game_logs(game_date, player_team=pl_tm, lg=lg)
        if df.empty:
            print(f'Empty dataframe {lg}: {game_date}')
            continue
        dfs.append(df)
        sleep(2) # 3 second timeout between each league fetch
    
    if dfs:
        bigdf = pd.concat(dfs).reset_index(drop=True)
        return bigdf
    return pd.DataFrame()

# convert df to lists for executemany
def df_to_insert_lists(df):
    in_flds = tuple(df.columns)
    in_vals = list(map(tuple, df.to_numpy()))
    return(in_flds, in_vals)

def get_game_logs(game_date, pl_tm='T'):
    dfs = check_all_lgs(game_date, pl_tm)
    if not dfs:
        print(f'No games played in any league on {game_date}')
        print('Exiting...')
        return # exit the program    
    
    # combine the dataframes
    return pd.concat(dfs.copy()).reset_index(drop=True)
    
def inserts(db, table_dfs):
    db = conn.DBConn(db)
    for dict in table_dfs:
        table = list(dict.keys())[0]
        df = list(dict.values())[0]
        in_list = df_to_insert_lists(df)
        logs.append_log(f'Attempting to insert into {table}...')
        ins_res = db.insert(table, in_list[0], in_list[1])
        for res in ins_res:
            logs.append_log(res)
    
def manual_insert(table, df):
    in_list = df_to_insert_lists(df)
    db = conn.DBConn('dev')
    ins_res = db.insert(table, in_list[0], in_list[1])
    for res in ins_res:
        logs.append_log(res)
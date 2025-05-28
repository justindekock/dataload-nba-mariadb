from time import sleep
import pandas as pd
from datetime import datetime, timedelta
from math import ceil
from dbamdb import conn
import fetch
import clean
import logs

def main():
    pass
    
def chunk_dates(dates, size):
    start_date = datetime.strptime(dates[0], '%m/%d/%Y')
    end_date = datetime.strptime(dates[1], '%m/%d/%Y')
    days = abs(end_date-start_date).days
    
    # split this into a list for every ten days
    game_dates = []
    chunks = int(ceil(days/size)) # number of days / 10 rounded up (27 days -> c=3)
    for c in range(chunks):
        chunk = [] # create one list to contain a list of each chunk of dates
        for i in range(size): # now for the num_dts, will create a list of this length with the dates
            date = ((start_date + timedelta(c * size)) + timedelta(i))
            if date > end_date:
                break
            chunk.append(date.strftime('%m/%d/%Y')) 
        game_dates.append(chunk)# multiplying the c (current chunk) by the chunk length ensures dates line up
    return game_dates
    
def run_chunk(dates_chunks, db, delay=10):
    for i, date_chunk in enumerate(dates_chunks):
        logs.append_log(f'Fetching data from {date_chunk[0]} - {date_chunk[len(date_chunk) - 1]}')
        print(date_chunk, '\n')
        tm_df = run_dates_chunk(date_chunk, 'T')
        pl_df = run_dates_chunk(date_chunk, 'P')
        team_data = clean.TeamData(tm_df)
        player_data = clean.PlayerData(pl_df, team_data.tgame_df)
        table_dfs = (list(team_data.table_dfs) + list(player_data.table_dfs))
        
        # count all the rows
        r = 0
        for df in table_dfs:
            r+=list(df.values())[0].shape[0]
        
        #.extend(player_data.table_dfs)
        logs.append_log(f'\n----\nPassing {r} rows from {date_chunk[0]} - {date_chunk[len(date_chunk) - 1]} to insert function...')
        inserts(db, table_dfs)
        logs.append_log(f'\nFinished with chunk {i+1} of {len(dates_chunks)} - intentional {delay} second delay...\n')
        sleep(delay)
    
def run_dates_chunk(game_dates, pl_tm, delay=3):
    dfs = []
    for i, date in enumerate(game_dates):
        df = check_all_lgs(date, pl_tm=pl_tm)
        if df.empty:
            print(f'Empty dataframe {date}')
            continue
        dfs.append(df)
        print(f'Fetched date {i+1} of {len(game_dates)} - delaying for {delay} seconds...')
        sleep(delay)
    bigdf = pd.concat(dfs).reset_index(drop=True)

    return bigdf  


# returns single df with all games for one date for all leagues
def check_all_lgs(game_date, game_date_to=None, pl_tm='T'):
    lgs = ['NBA', 'WNBA', 'GNBA']
    dfs = []
    for lg in lgs:
        # print(game_date)
        # print(lg)
        if game_date_to:
            logs.append_log(f'Fetching {pl_tm} data for {game_date} - {game_date_to}...')
            
        df = fetch.game_logs(game_date, game_date_to, player_team=pl_tm, lg=lg)
        if df.empty:
            # print(f'Empty dataframe {lg}: {game_date}')
            logs.append_log(f'No {lg} {pl_tm} data found: {game_date} - {game_date_to}...')
            continue
        dfs.append(df)
        sleep(1) # 3 second timeout between each league fetch
    
    if len(dfs) > 0:
        bigdf = pd.concat(dfs).reset_index(drop=True)
        return bigdf
    return pd.DataFrame()

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
    
    # split this into a list for every ten days
    game_dates = []
    
    for i in range(days):
        date = (start_date + timedelta(i)).strftime('%m/%d/%Y')
        game_dates.append(date)
        
    game_dates.append(dates[1]) # append the end date
    return game_dates
    
# TODO - possible to split this up so every say 10 days it cleans the data and isnerts then moves to the next 10 days
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
        logs.append_log(f'Attempting to insert {len(in_list[1])} rows into table {table}...')
        ins_res = db.insert(table, in_list[0], in_list[1])
        for res in ins_res:
            logs.append_log(res)
    
def manual_insert(table, df):
    in_list = df_to_insert_lists(df)
    db = conn.DBConn('dev')
    ins_res = db.insert(table, in_list[0], in_list[1])
    for res in ins_res:
        logs.append_log(res)
        
if __name__=='__main__':
    main()

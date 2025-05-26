import fetch
import clean
from time import sleep
import pandas as pd
from datetime import datetime, timedelta
from dbamdb import conn

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
    game_dates = list_of_dates(dates)
    dfs = []
    for date in game_dates:
        
        #df = get_game_logs(date, pl_tm=player_team)
        df = check_all_lgs(date, pl_tm=player_team)
        if df.empty:
            print(f'Empty dataframe {date}')
            continue
        
        dfs.append(df)
        print(df)    
        sleep(3)
    
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
        sleep(3) # 3 second timeout between each league fetch
    
    if dfs:
        bigdf = pd.concat(dfs).reset_index(drop=True)
        return bigdf
    return pd.DataFrame()

def get_game_logs(game_date, pl_tm='T'):
    dfs = check_all_lgs(game_date, pl_tm)
    if not dfs:
        print(f'No games played in any league on {game_date}')
        print('Exiting...')
        return # exit the program    
    
    # combine the dataframes
    return pd.concat(dfs.copy()).reset_index(drop=True)

def fetch_insert_players(db = 'dev'):
    players = fetch.get_players()
    pl_ins_lists = df_to_insert_lists(players)
    
    db_conn = conn.DBConn(db)
    db_conn.insert('player_temp', pl_ins_lists[0], pl_ins_lists[1])
    print('Completed player insert/update')
    
# should move this to clean
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
        return# pd.DataFrame()
    return df
    
def inserts(table_dfs):
    
    # database = conn.DBConn('dev')
    # db_conn = database.connect()
    # db_conn.begin()
    
    for i, dict in enumerate(table_dfs):
        table = list(dict.keys())[0]
        df = list(dict.values())[0]
        in_list = df_to_insert_lists(df)
        print(f'Table {i+1}: {table}\nColumns: {len(in_list[0])}\nRows: {len(in_list[1])}\n')
    
        
    # db_conn.commit()
    

import fetch
import clean
import pandas as pd
from dbamdb import conn

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

def check_all_lgs(game_date, pl_tm):
    dfs = []
    lgs = ['NBA', 'WNBA', 'GNBA']
    for lg in lgs:
        df = fetch.game_logs(game_date, player_team=pl_tm, lg=lg)
        if not df.empty:
            dfs.append(df)
        
    return dfs

def fetch_insert_players(db = 'dev'):
    players = fetch.get_players()
    pl_ins_lists = df_to_insert_lists(players)
    
    db_conn = conn.DBConn(db)
    db_conn.insert('player_temp', pl_ins_lists[0], pl_ins_lists[1])
    print('Completed player insert/update')
    
def get_game_logs(game_date, pl_tm='T'):
    dfs = check_all_lgs(game_date, pl_tm)
    if not dfs:
        print(f'No games played in any league on {game_date}')
        print('Exiting...')
        return # exit the program    
    
    # combine the dataframes
    return pd.concat(dfs.copy()).reset_index(drop=True)
    
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
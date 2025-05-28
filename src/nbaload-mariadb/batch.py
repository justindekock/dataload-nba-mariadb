from time import sleep
from datetime import datetime, timedelta
from math import ceil
from run import check_all_lgs, inserts
import clean
import logs

delay = 2
db = 'prod'

def main():
    dates = ['10/10/2006', '10/09/2008']#(datetime.today()).strftime('%m/%d/%Y')]
    
    start_msg = f'Beginning batch fetch for data from {dates[0]} - {dates[1]}....'
    logs.log_print(start_msg)
    
    gdates = chunk_dates(dates, size=20)
    
    for i, c in enumerate(gdates):
        msg = f'Chunk {i+1} of {len(gdates)}: {c[0]} - {c[len(c) - 1]}...'
        logs.log_print(msg, brk=True)
        
        tm_df = check_all_lgs(game_date=c[0], game_date_to=c[len(c) - 1], pl_tm='T')
        if tm_df.empty:
            continue # move to next chunk if no team logs fetched
        pl_df = check_all_lgs(game_date=c[0], game_date_to=c[len(c) - 1], pl_tm='P')
        team_data = clean.TeamData(tm_df)
        player_data = clean.PlayerData(pl_df, team_data.tgame_df)
        table_dfs = (list(team_data.table_dfs) + list(player_data.table_dfs))
        r = count_total_rows(table_dfs)
    
        logs.log_print(f'{r} total rows fetched from {c[0]} - {c[len(c) - 1]}...', brk=True)
        inserts(db, table_dfs)
        
        dt = (delay * 15) if (i+1) % (len(gdates) / 4) == 0 else delay
        logs.log_print(f'Finished with dates {c[0]} - {c[len(c) - 1]} - intentional {dt} second delay...\n', brk=True)
        sleep(dt)
        
    logs.log_print(f'Finished fetching data from {dates[0]} - {dates[1]} - script complete!', brk=True)
    
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
    
    logs.append_log(f'Dates split into {chunks} chunks with {size} dates each...', brk=True)
    return game_dates

def count_total_rows(dfs=[]):
    r = 0
    if dfs:
        for df in dfs:
            r+=list(df.values())[0].shape[0]
    return r
            
if __name__=='__main__':
    main()
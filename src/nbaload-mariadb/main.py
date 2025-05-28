from datetime import datetime, timedelta
import clean
import run
import logs

db = 'prod'

def main():
    
    game_date = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
    
    logmsg = f'Fetching and inserting NBA/WNBA/G-League game logs: {game_date}\n' 
    logs.log_print(logmsg)
    
    logs.append_log(f'Fetching current players first...')
    run.fetch_insert_players(db)
        
    tm_df = run.check_all_lgs(game_date, pl_tm='T')
    if tm_df.empty:
        logs.log_print(f'No logs found for {game_date}, exiting...', brk=True)
        return # no logs, exit
    pl_df = run.check_all_lgs(game_date, pl_tm='P')
    team_data = clean.TeamData(tm_df)
    player_data = clean.PlayerData(pl_df, team_data.tgame_df)
    table_dfs = (list(team_data.table_dfs) + list(player_data.table_dfs))
    
    logs.append_log('Team logs fetched and cleaned, starting DB insert...')
    run.inserts(db, table_dfs)
    logs.log_print('Script complete!', brk=True)

if __name__=='__main__':
    main()
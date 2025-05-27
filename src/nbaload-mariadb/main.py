from datetime import datetime, timedelta
import clean
import run
import logs

def main():
    db = 'prod'
    
    game_date = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
    dates = ['10/22/2024', game_date] # testing with entirity of 2425 season
    # dates = [game_date]
    
    logmsg = f"Fetching and inserting NBA/WNBA/G-League game logs: {dates[0]}" 
    if len(dates) > 1:
        logmsg += f' through {dates[1]}...'
    logmsg += '\n'
    
    logs.append_log(logmsg)
    
    run.fetch_insert_players(db)
    
    logs.append_log('\nStarting team fetch...')
    batch_tm_df = run.game_logs_batch(dates, player_team='T')
    batch_tm_data = clean.TeamData(batch_tm_df)
    logs.append_log('Team logs fetched and cleaned, starting DB insert...')
    run.inserts(db, batch_tm_data.table_dfs)
    
    logs.append_log('\nStarting player fetch...')
    batch_pl_df = run.game_logs_batch(dates, player_team='P')
    batch_pl_data = clean.PlayerData(batch_pl_df, batch_tm_data.tgame_df)
    logs.append_log('Player logs fetched and cleaned, starting DB insert...')
    run.inserts(db, batch_pl_data.table_dfs)

if __name__=='__main__':
    main()
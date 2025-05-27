from datetime import datetime, timedelta
import clean
import run
import logs

def main():
    game_date = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')
    dates = ['10/10/2024', '11/30/2024']
    
    logs.append_log(f'Attempting to fetch and load game logs from {dates[0]} through {dates[1]}...')
    
    run.fetch_insert_players('dev')
    
    logs.append_log('\nStarting team fetch...')
    batch_tm_df = run.game_logs_batch(dates, player_team='T')
    batch_tm_data = clean.TeamData(batch_tm_df)
    logs.append_log('Team logs fetched and cleaned, starting DB insert...')
    run.inserts(batch_tm_data.table_dfs)
    
    logs.append_log('\nStarting player fetch...')
    batch_pl_df = run.game_logs_batch(dates, player_team='P')
    batch_pl_data = clean.PlayerData(batch_pl_df, batch_tm_data.tgame_df)
    logs.append_log('Player logs fetched and cleaned, starting DB insert...')
    run.inserts(batch_pl_data.table_dfs)

if __name__=='__main__':
    main()
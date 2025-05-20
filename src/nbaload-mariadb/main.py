from datetime import datetime, timedelta
from dbamdb import conn
import fetch
import clean

dbc = conn.Conn()
print(dbc.select('player', 'fetchall'))
#dbc.show_db()


game_date = (datetime.today() - timedelta(1)).strftime('%m/%d/%Y')

raw_tm = fetch.game_logs(game_date=game_date, player_team='T', lg='NBA')
raw_pl = fetch.game_logs(game_date=game_date, player_team='P', lg='NBA')

team_data = clean.TeamData(raw_tm)

# print(team_data.tgame_df)
player_data = clean.PlayerData(raw_pl, team_data.tgame_df)

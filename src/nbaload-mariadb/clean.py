import pandas as pd

# used in both classes
def clean_raw_df(df):
    df.columns = [col.lower() for col in df.columns]
    df = df.rename(columns={'team_abbreviation': 'team', 'min': 'mins', 'plus_minus': 'pm'})
    df = df.infer_objects(copy=False).fillna(0)    
    return df

def get_ot(mins):
    return 0 if mins < 260 else (2 if mins > 280 else 1)

def get_loc(matchup):
    return 'A' if matchup[4] == '@' else 'H'

class TeamData:
    def __init__(self, raw_df):
        if raw_df.empty:
            print(f'No games found')
            return
        else:
            # TODO - dfs in a list
            self.table_dfs = []
            self.raw_df = raw_df
            self.clean_df = clean_raw_df(self.raw_df)
            self.team_df = self.get_team_df()
            self.season_df = self.get_szn_df()
            self.game_df = self.get_game_df()
            self.tgame_df = self.get_tgame_df()
            self.tbox_df = self.get_tbox_df()
            self.tshtg_df = self.get_tshtg_df()
            # self.print_dfs()
        
    def print_dfs(self):
        for i, df in enumerate(self.table_dfs):
            print(f'DF {i+1} of {len(self.table_dfs)}')
            print(df)
        
    def get_team_df(self):
        team_df = self.clean_df[['team_id', 'team', 'team_name', 'lg']].drop_duplicates()
        self.table_dfs.append({'team': team_df})
        return team_df
    def get_szn_df(self):
            
        season_id = self.clean_df['season_id'].unique()
        season = []
        season_desc = []
        wseason = []
        wseason_desc = []
        
        # for loop in case multiple seasons in df
        for id in season_id:
            # characters 1 through 5 in season_id are the year
            year1 = id[1:5]
            year2 = int(year1) + 1
            
            # first character in id is the season type
            stype = int(id[0])
            stypes = {
                1: ['PRE', 'Pre Season'], 2: ['RS', 'Regular Season'], 
                3: ['AS', 'All Star Week'], 4: ['PO', 'Playoffs'], 
                5: ['PI', 'Play-In Tournament'], 6: ['NC', 'NBA Cup'],
                7: ['MI', 'Miscellaneous']
            }      
            
            # default to miscellaneous when id not in the defined seasons
            szn = stypes[stype][0] if stype in stypes else stypes[7][0]
            szn_desc = stypes[stype][1] if stype in stypes else stypes[7][1]
            
            season.append(f'{year1}-{year2}-{szn}')
            season_desc.append(f'{year1}-{year2} {szn_desc}')
            wseason.append(f'{year1}-{szn}')
            wseason_desc.append(f'{year1} WNBA {szn_desc}')
            
            # season.append(f'{year1}-{year2}-{stypes[stype][0]}')
            # season_desc.append(f'{year1}-{year2} {stypes[stype][1]}')
            # wseason.append(f'{year1}-{stypes[stype][0]}')
            # wseason_desc.append(f'{year1} WNBA {stypes[stype][1]}')
                
                
            
        # return a dataframe with the lists as columns
        szn_df = pd.DataFrame(
            {'season_id': season_id, 'season': season, 'season_desc': season_desc, 
            'wseason': wseason, 'wseason_desc': wseason_desc}
            )
        self.table_dfs.append({'season': szn_df})
        return szn_df
    
    def get_game_df(self):
        df = self.clean_df.drop_duplicates(subset=['game_id', 'team_id'])
        def get_final_scores(df): # adds a formatted final score column
            final_scores = df.pivot(index='game_id', columns='team', values='pts')
        
        # returns string formatted score - pass with .apply to each row
            def format_score(row): # pass to each row with apply function
                teams = list(row.dropna().index)
                scores = list(row.dropna().values)
                
                # ensure both teams have data, otherwise return nothing        
                if len(teams) == 2: 
                    if scores[0] > scores[1]: # winning team first
                        return f"{teams[0]} {int(scores[0])} - {int(scores[1])} {teams[1]}"
                    return f"{teams[1]} {int(scores[1])} - {int(scores[0])} {teams[0]}"
                return None

            # get final scores
            final_scores['final'] = final_scores.apply(format_score, axis=1)
            final_scores = final_scores[['final']].reset_index()
            
            # return final scores df merged with df passed in
            return (df.merge(final_scores, on='game_id', how='left')
                    .drop(['team', 'pts'], axis=1).drop_duplicates())
        
        # CALLS START HERE - get copy df for function, call final scores function to get final string
        # ot indicator, if mins > 240, game went to ot. if > 280, game went to double ot
        # drop the rows with @, drop unecessary columns
        game_df = df[['game_id', 'season_id', 'lg', 'team_id', 'team', 'game_date', 'matchup', 'pts', 'mins']].copy()
        game_df = get_final_scores(game_df)
        game_df['ot'] = game_df['mins'].apply(get_ot)
        game_df = game_df[game_df['matchup'].str[4] == 'v'].reset_index() 
        game_df = game_df.drop(columns=['index', 'mins', 'team_id'])
        self.table_dfs.append({'game':game_df})
        return game_df
    
    def get_tgame_df(self):
        tgame_df = self.clean_df[['game_id', 'season_id', 'team_id', 'game_date', 'matchup',
                   'pts', 'pm', 'wl', 'mins']].copy() 

        # add loc field (home/away indicator)
        # add ot indicator (based on minutes) -- 0 if < 260, 2 if > 280, 1 if between 260 & 280
        tgame_df['loc'] = tgame_df['matchup'].apply(get_loc)
        tgame_df['ot'] = tgame_df['mins'].apply(get_ot)
        tgame_df['pm'] = tgame_df['pm'].astype(int)
        tgame_df = tgame_df.rename(columns={'pm': 'diff'})
        tgame_df = tgame_df.drop(['mins'], axis=1)
        self.table_dfs.append({'t_game':tgame_df})
        return tgame_df
    
    def get_tbox_df(self):
        tbox_df = self.clean_df[['game_id', 'season_id', 'team_id', 'mins', 'pts', 'ast', 
           'reb', 'stl', 'blk', 'oreb', 'dreb', 'pm', 'tov', 'pf']].copy()
        self.table_dfs.append({'t_box':tbox_df})
        return tbox_df
    
    def get_tshtg_df(self):
        tshtg_df = self.clean_df[['game_id', 'season_id', 'team_id', 'fgm', 'fga', 'fg3m', 
           'fg3a', 'ftm', 'fta', 'fg_pct', 'fg3_pct', 'ft_pct']].copy() 
        self.table_dfs.append({'t_shtg':tshtg_df})
        return tshtg_df

class PlayerData():
    def __init__(self, raw_df, clean_team_df):
        if raw_df.empty:
            print(f'No game logs to clean')
            return
        else:
            self.table_dfs = []
            self.raw_df = raw_df
            self.clean_df = clean_raw_df(self.raw_df)
            self.player_df = self.get_player_df()
            self.pgame_df = self.get_pgame_df(clean_team_df)
            self.pbox_df = self.get_pbox_df()
            self.pshtg_df = self.get_pshtg_df()
    
    def print_dfs():
        pass
    
    def get_player_df(self):
        return self.clean_df[['player_id', 'player_name']].drop_duplicates()
    
    def get_pgame_df(self, clean_team_df):
        pl_df = self.clean_df[['game_id', 'season_id', 'team_id', 'player_id', 'mins', 'pts']].copy()
        joined = pl_df.merge(clean_team_df, on=['game_id', 'season_id', 'team_id'])
        joined = joined.rename(columns={'pts_x': 'pts'})    
        cols = ['game_id', 'season_id', 'team_id', 'player_id', 'game_date', 
                'matchup', 'mins', 'pts', 'diff', 'wl', 'loc', 'ot']
        pgame_df = joined[cols]
        self.table_dfs.append({'p_game':pgame_df})
        return pgame_df
    
    def get_pbox_df(self):
        pbox_df = self.clean_df[['game_id', 'season_id', 'team_id', 'player_id', 'mins', 'pts', 'ast', 
           'reb', 'stl', 'blk', 'oreb', 'dreb', 'pm', 'tov', 'pf']].copy() 
        self.table_dfs.append({'p_box':pbox_df})
        return pbox_df
    
    def get_pshtg_df(self):
        pshtg_df = self.clean_df[['game_id', 'season_id', 'team_id', 'player_id', 'fgm', 'fga', 'fg3m', 
           'fg3a', 'ftm', 'fta', 'fg_pct', 'fg3_pct', 'ft_pct']].copy() 
        self.table_dfs.append({'p_shtg':pshtg_df})
        return pshtg_df
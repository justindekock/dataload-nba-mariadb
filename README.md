Upon execution this project fetches game logs from the nba_api package, cleans the data, and uses my dba-mariadb package to insert the data into a mariadb server hosted locally on a raspberry pi 5. 

dba-mariadb REPO: https://github.com/justindekock/dba-mariadb.git

MODULES: 

- fetch.py - uses the nba_api package to call the nba stats website and fetch data
- clean.py - cleans team and player game log data. two classes, TeamData and PlayerData return dataframes formatted the same as my database tables
- main.py - file that will be run nightly with a cron job
- run.py - contains process flow functions
- other.py - run other variations of the fetch functions


LOCAL DEPENDENCIES:
- my package for interacting with mariadb is in /home/jdeto/py/dba-mariadb
- - installed with 'pip install ../dba-mariadb'
- - run 'pip install --upgrade .' in the dba repo, then 'pip install --upgrade ../dba-mariadb' in this repo to publish new changes to pip, then get those changes here

HIGH LEVEL OVERVIEW (replicate for NBA, WNBA, G)
- fetch.py: fetch team/player data from nba_api
- clean.py: clean the data and subset into dataframes matching database tables
- val.py: check whether the clean data already exists in the target database table

# TODO - BUILD IN PLAY BY PLAY FETCH/CLEAN

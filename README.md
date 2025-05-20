Upon execution this project fetches game logs from the nba_api package, cleans the data, and uses my dba-mariadb package to insert the data into a mariadb server hosted locally on a raspberry pi 5. 

LOCAL DEPENDENCIES:
- my package for interacting with mariadb is in /home/jdeto/py/dba-mariadb
- - installed with 'pip install ../dba-mariadb'

HIGH LEVEL OVERVIEW (replicate for NBA, WNBA, G)
- fetch.py: fetch team/player data from nba_api
- clean.py: clean the data and subset into dataframes matching database tables
- val.py: check whether the clean data already exists in the target database table

# TODO - BUILD IN PLAY BY PLAY FETCH/CLEAN

import os
from dotenv import load_dotenv
from datetime import datetime as dt
    
load_dotenv()

path = f'{os.environ["LOG_PATH"]}/nbaload_{dt.now().strftime("%m%d%YT%H%M%S")}.log'
    
def append_log(msg):
    with open(path, 'a') as f:
        f.write(f'{dt.now()} -- {msg}\n')
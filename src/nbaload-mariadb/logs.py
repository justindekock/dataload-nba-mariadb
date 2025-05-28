import os
from dotenv import load_dotenv
from datetime import datetime as dt
    
load_dotenv()

path = f'{os.environ["LOG_PATH"]}/nbaload_{dt.now().strftime("%m%d%YT%H%M%S")}.log'
    
def append_log(msg, brk=False):
    with open(path, 'a') as f:
        if brk:
            f.write('\n')
        f.write(f"{dt.now().strftime('%H:%M:%S')} ---------- {msg}\n")
        
def log_print(msg, brk=False):
    print(msg)
    append_log(msg, brk)
    
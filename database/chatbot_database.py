import sqlite3
import json
from datetime import datetime

timeframe= '2015-01'

# Have this because we know we will be working with lots of rows
# You don't want to insert rows one by one.
# You want to insert by doing one large transaction. Should increase speed.
sql_transaction = []

# names databse after timeframe variable.
connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()

def create_table():
  c.execute("""CREATE TABLE IF NOT EXISTS parent_reply
(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT,
comment TEXT, subreddit TEXT, unix INT, score INT) """)

if __name__ == "__main__":
  create_table()
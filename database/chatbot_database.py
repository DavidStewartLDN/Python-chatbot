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


def format_data(data):
  data = data.replace("\n", " newlinechar ").replace("\r"," newlinechar ").replace('"',"'")
  return data

def find_parent(pid):
  try:
    sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
    result = c.fetchone()
    if result != None:
      return result[0]
    else: return False
  except Exception as e:
    print("find_parent", e)
    return False

if __name__ == "__main__":
  create_table()
  #  How many rows have we goen through
  row_counter = 0
  # how many parent and child pairs we have come up with, a lot of comments will go without a reply (and so do not make back and ofurth interaction)
  paired_rows = 0


  with open("/Users/davide/Projects/Dataset/reddit-comment-history/{}/RC_{}".format(timeframe.split('-')[0], timeframe), buffer=1000) as f:
    for row in f:
      row_counter += 1
      row = json.loads(row)
      parent_id = row['parent_id']
      body = format_data(row['body'])
      created_utc = row['created_utc']
      score = row['score']
      subreddit = row['subreddit']


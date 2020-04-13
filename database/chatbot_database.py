import sqlite3
import json
from datetime import datetime

timeframe= '2015-01'

# Have this because we know we will be working with lots of rows
# You don't want to insert rows one by one.
# You want to insert by doing one large transaction. Should increase speed.
sql_transaction = []

# Defined for cleanup
start_row = 0
cleanup = 1000000

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

# create out acceptance criteria. Data cleaning
def acceptable(data):
  # makes sure number of words is betweeen 1 and 50
  if len(data.split(' ')) > 50 or len(data) < 1:
    return False
  # removes comment where length of string greater than 1000
  elif len(data) > 1000:
    return False
  # removes comments that have been removed.
  elif data == '[deleted]' or data == '[removed]':
    return False
  else:
    return True

def find_parent(pid):
  try:
    sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
    c.execute(sql)
    result = c.fetchone()
    if result != None:
      return result[0]
    else: return False
  except Exception as e:
    # print("find_parent", e)
    return False

def find_existing_score(parent_id):
  try:
    sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
    c.execute(sql)
    result = c.fetchone()
    if result != None:
      return result[0]
    else: return False
  except Exception as e:
    # print("find_parent", e)
    return False

# builds queue of SQL commands so that they are ran at the same time - increases speed!
def transaction_bldr(sql):
  global sql_transaction
  sql_transaction.append(sql)
  if len(sql_transaction) > 1000:
    c.execute('BEGIN TRANSACTION')
    for s in sql_transaction:
      try:
        c.execute(s)
      except:
        pass
    connection.commit()
    sql_transaction = []

def sql_insert_replace_comment(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id =?;""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        transaction_bldr(sql)
    except Exception as e:
        print('s-UPDATE insertion',str(e))

def sql_insert_has_parent(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-PARENT insertion',str(e))

def sql_insert_no_parent(commentid,parentid,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-NOPARENT insertion',str(e))

if __name__ == "__main__":
  create_table()
  #  How many rows have we goen through
  row_counter = 0
  # how many parent and child pairs we have come up with, a lot of comments will go without a reply (and so do not make back and fourth interaction)
  paired_rows = 0


  with open("/Users/davide/Projects/Dataset/reddit-comment-history/{}/RC_{}".format(timeframe.split('-')[0], timeframe), buffering=1000) as f:
    for row in f:
      # print(row)
      row_counter += 1
      row = json.loads(row)
      parent_id = row['parent_id']
      body = format_data(row['body'])
      created_utc = row['created_utc']
      score = row['score']
      comment_id = row['name']
      subreddit = row['subreddit']
      parent_data = find_parent(parent_id)

      if score >= 2:
        # Check if current comment fits criteria else don't do other computations.
        if acceptable(body):
          # Checks if the current comment connected to the parent has a lower sc ore than current comment
          existing_comment_score = find_existing_score(parent_id)
          if existing_comment_score:
            # if score is higher, we will replace it
            if score > existing_comment_score:
              sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
          
          else:
            if acceptable(body):
              if parent_data:
                sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                paired_rows +=1
              else:
                sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)
      
      if row_counter % 100000 == 0:
        print("Total rows read: {}, Paired rows: {}, Time: {}".format(row_counter, paired_rows, str(datetime.now())))
      
      if row_counter > start_row:
                if row_counter % cleanup == 0:
                    print("Cleanin up!")
                    sql = "DELETE FROM parent_reply WHERE parent IS NULL"
                    c.execute(sql)
                    connection.commit()
                    c.execute("VACUUM")
                    connection.commit()
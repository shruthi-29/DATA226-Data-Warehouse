from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests

def return_snowflake_conn():

    #Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    #Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def return_last_90d_price(api_key,symbol):
  """
   - return the last 90 days of the stock prices of symbol as a list of json strings
  """
  url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
  r = requests.get(url)
  data = r.json()

  daily_data = data.get("Time Series (Daily)", {})

  sorted_dates = sorted(daily_data.keys(), reverse=True)

  results = []
  for d in sorted_dates[:90]:
    results.append({
            "date": d,
            **daily_data[d]
        })
 
  return results

@task
def load_records(records,symbol):
    con = return_snowflake_conn()
    target_table = "RAW.STOCK_PRICE_DATA"
    try:
        con.execute("BEGIN;")
        con.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
          	DATE DATE,
            OPEN FLOAT,
            HIGH FLOAT,
            LOW FLOAT,
            CLOSE FLOAT,
            VOLUME NUMBER(38,0),
            SYMBOL VARCHAR,
            PRIMARY KEY (SYMBOL, DATE));""")
        con.execute(f"""DELETE FROM {target_table}""")
        for r in records:
            date = r['date']
            open = r['1. open']
            high = r['2. high']
            low = r['3. low']
            close = r['4. close']
            volume = r['5. volume']
            symbol = symbol
            print('Inserting '+date+' '+open+' '+high+' '+low+' '+close+' '+volume+' '+symbol)
            sql = f"INSERT INTO {target_table} (DATE,OPEN,HIGH,LOW,CLOSE,VOLUME,SYMBOL) VALUES ('{date}', '{open}','{high}','{low}','{close}','{volume}','{symbol}')"
            con.execute(sql)
        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e

@task
def count_records(cur, table): #to check number of records loaded into the table
  #cur = con.cursor()
  cur.execute(f"SELECT COUNT(*) FROM {table}")
  return cur.fetchone()[0]
  
with DAG(
    dag_id = 'StocksDAG',
    start_date = datetime(2025,10,6),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    target_table = "RAW.STOCK_PRICE_DATA"
    symbol = 'BAC'
    key = Variable.get("ALPHAVANTAGE_API_KEY")
    cur = return_snowflake_conn()
    data = return_last_90d_price(key,symbol)
    load = load_records(data,symbol)
    count = count_records(cur, target_table)
    data>>load>>count
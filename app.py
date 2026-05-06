import pymysql
import streamlit as st
from datetime import datetime, time, timedelta
import pandas as pd

@st.cache_data
def fetch_data(start_date, end_date):
  # connect to the SQL database with the enviroment variables 
  conn = pymysql.connect(
    host=str(st.secrets.MYSQL_HOST),
    port=int(st.secrets.MYSQL_PORT),
    user=str(st.secrets.MYSQL_USERNAME),
    password=str(st.secrets.MYSQL_PASSWORD),
    database=str(st.secrets.MYSQL_DATABASE),
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=True,
  )
  rows = []
  try:
    # instead of cur = conn.cursor() because with automatically closes
    with conn.cursor() as cur:
      sql = """
          SELECT DISTINCT tracking_number, created_at, latest_router_description, latest_router_time
          FROM transit_third_party_caches
          WHERE created_at >= %s AND created_at < %s
      """
      # convert to datetime object 
      start_dt = datetime.combine(start_date, time.min)
      end_dt   = datetime.combine(end_date, time.max)
      cur.execute(sql, (start_dt, end_dt))
    # could use fetchmany but lazy? not enough rows to do so?
    rows = cur.fetchall()
  finally:
      conn.close()
  return rows 

# takes in a row of the data dataframe and returns a new time. to be used in an apply function
# ALL TIMES ARE IN UTC WHICH IS 4 HOURS AHEAD IN THE SUMMER. MIGHT HAVE TO FIX IN WINTER
def calculate_pickup_time(row):
  created_time = pd.to_datetime(row["created_at"])
  if created_time.weekday() < 4:
    # before 7am EST = picked up today at 2pm EST
    if created_time.hour < 11:
      return datetime(created_time.year, created_time.month, created_time.day, 18)
    # after 7am EST = picked up tomorrow at 2pm EST
    else: 
      return datetime(created_time.year, created_time.month, created_time.day, 18) + timedelta(days=1)
  # fridays: before 7 EST picked up today, after 7 EST picked up monday 
  if created_time.weekday() == 4: 
    # before 7am EST = picked up today at 2pm EST
    if created_time.hour < 11:
      return datetime(created_time.year, created_time.month, created_time.day, 18)
    else:
      return datetime(created_time.year, created_time.month, created_time.day, 18) + timedelta(days = 3)
  # all sat and sun packages are picked up monday 2pm EST
  if created_time.weekday() > 4: 
    between_monday = 7 - created_time.weekday() 
    return datetime(created_time.year, created_time.month, created_time.day, 18) + timedelta(days = between_monday)
  
# returns a timedelta of the time it took to deliver a package.
def calculate_pickup_time(row):
  delivered_timestamp = pd.to_datetime(row["latest_router_time"])
  pickup_time = delivered_timestamp - row["pickup time"] 
  pickup_hours = pickup_time.total_seconds() / 3600
  if pickup_hours < 0:
    return 0
  else: 
    return pickup_hours
  
def calculate_created_to_delivery_time(row):
  created_time = pd.to_datetime(row["created_at"])
  delivered_timestamp = pd.to_datetime(row["latest_router_time"])
  created_to_delivery = delivered_timestamp - created_time
  return created_to_delivery.total_seconds() / 3600  

# start and end date picker
start_date = st.date_input("pick start date")
end_date = st.date_input("pick end date")
st.write("start date:", start_date, "end date:", end_date) 

# button to fetch from db & write the response 
# use session state so it doesn't rerun 
if st.button("fetch from db"):
  st.session_state.data = pd.DataFrame(fetch_data(start_date, end_date))
if "data" in st.session_state:
  st.write(st.session_state.data)

if st.button("calculate pickup times & get rid of undelivered"):
  st.session_state.data = st.session_state.data[st.session_state.data['latest_router_description'] == 'Delivered.']
  st.session_state.data["pickup time"] = st.session_state.data.apply(calculate_pickup_time, axis = 1)
  st.session_state.data["warehouse pickup time"] = st.session_state.data.apply(calculate_pickup_time, axis = 1)
  st.session_state.data["created to delivery time"] = st.session_state.data.apply(calculate_created_to_delivery_time, axis = 1)
  st.write(st.session_state.data)
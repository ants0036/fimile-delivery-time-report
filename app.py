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
          SELECT DISTINCT tracking_number, created_at, latest_router_description, latest_router_time, sender_zip_code, receiver_zip_code
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
def calculate_warehouse_time(row):
  delivered_timestamp = pd.to_datetime(row["latest_router_time"])
  pickup_time = delivered_timestamp - row["pickup time"] 
  pickup_hours = pickup_time.total_seconds() / 3600
  if pickup_hours < 0:
    return 0
  else: 
    return pickup_hours / 24
  
def calculate_created_to_delivery_time(row):
  created_time = pd.to_datetime(row["created_at"])
  delivered_timestamp = pd.to_datetime(row["latest_router_time"])
  created_to_delivery = delivered_timestamp - created_time
  hours = created_to_delivery.total_seconds() / 3600  
  return hours / 24

def find_sender_start(row):
  if row["sender_zip_code"] == "":
    return "N/A"
  if row["sender_zip_code"] in ["91752", "90670", "92324", "92337", "91761", "91789", "92701", "91768", "91766"]:
    return "CA"
  elif row["sender_zip_code"] in ["08817", "08067", "08859", "08810", "08902", "07001", "07064", "07036", "08854"]:
    return "NJ"
  elif row["sender_zip_code"] in ["31308", "29927"]:
    return "SAV"
  elif row["sender_zip_code"] in ["77423", "77060", "77449"]:
    return "TX"
  elif row["sender_zip_code"] in ["30336", "30517", "30297", "30519"]:
    return "ATL"
  elif row["sender_zip_code"] in ["60517"]:
    return "IL"
  else:
    return "Other"
  
def load_zones():
  st.session_state.ca_zones = pd.read_csv('data/ca zones.csv')
  st.session_state.nj_zones = pd.read_csv('data/nj zones.csv')
  st.session_state.sav_zones = pd.read_csv('data/sav zones.csv')
  st.session_state.tx_zones = pd.read_csv('data/tx zones.csv')
  st.session_state.atl_zones = pd.read_csv('data/atl zones.csv')
  st.session_state.il_zones = pd.read_csv('data/il zones.csv')
  st.session_state.fl_sub = pd.read_csv('data/fl subsections.csv')

# only called after load_zones() is called 
# optimize later
def find_zone(row):
  start = row["starting area"]
  receive_zip = int(row["receiver_zip_code"][:5])
  if start == "CA":
    match = st.session_state.ca_zones[st.session_state.ca_zones['zipcode'] == receive_zip]
  elif start == "NJ":
    match = st.session_state.nj_zones[st.session_state.nj_zones['zipcode'] == receive_zip]
  elif start == "TX":
    match = st.session_state.tx_zones[st.session_state.tx_zones['zipcode'] == receive_zip]
  elif start == "SAV":
    match = st.session_state.sav_zones[st.session_state.sav_zones['zipcode'] == receive_zip]
  elif start == "IL":
    match = st.session_state.il_zones[st.session_state.il_zones['zipcode'] == receive_zip]
  elif start == "ATL":
    match = st.session_state.atl_zones[st.session_state.atl_zones['zipcode'] == receive_zip]
  else:
    return "N/A"
  
  if match.empty:
    return 0 
  else: 
    # Check if FL, if FL, return sub-area (tampa or miami) instead of
    if match.iloc[0]["state"] == "FL":
      area = st.session_state.fl_sub[st.session_state.fl_sub['zipcode'] == receive_zip]
      return pd.Series([match.iloc[0]["zone"], area.iloc[0]["area"]])
    else: 
      return pd.Series([match.iloc[0]["zone"], match.iloc[0]["state"]])

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

if st.button("calculate times & zones"):
  st.session_state.data = st.session_state.data[st.session_state.data['latest_router_description'] == 'Delivered.']

  st.session_state.data["pickup time"] = st.session_state.data.apply(calculate_pickup_time, axis = 1)
  st.session_state.data["warehouse pickup time"] = st.session_state.data.apply(calculate_warehouse_time, axis = 1)
  st.session_state.data["created to delivery time"] = st.session_state.data.apply(calculate_created_to_delivery_time, axis = 1)
  st.session_state.data["starting area"] = st.session_state.data.apply(find_sender_start, axis = 1)
  load_zones()
  st.session_state.data[["zone", "receive state"]] = st.session_state.data.apply(find_zone, axis = 1)
  st.write(st.session_state.data)

if st.button("calculate avg"):
  grouped = st.session_state.data.groupby(['starting area', 'receive state']).agg({'warehouse pickup time': 'mean'})
  st.write(grouped)

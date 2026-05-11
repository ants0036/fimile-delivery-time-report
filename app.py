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
  if row["sender_zip_code"] in ["91752", "90670", "92324", "92337", "91761", "91789", "92701", "91768", "91766", "92376"]:
    return "CA"
  elif row["sender_zip_code"] in ["08817", "08067", "08859", "08810", "08902", "07001", "07064", "07036", "08854"]:
    return "NJ"
  elif row["sender_zip_code"] in ["31308", "29927", "31302"]:
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
# finds the zone and the receiver state/area 
def find_zone(row):
  start = row["starting area"]
  try: 
    receive_zip = int(row["receiver_zip_code"][:5])
  except:
    return pd.Series([0, "N/A"])
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
    return pd.Series([0, "N/A"])
  
  if match.empty:
    return pd.Series([0, "N/A"])
  else: 
    # Check if FL, if FL, return sub-area (tampa or miami) instead of
    if match.iloc[0]["state"] == "FL":
      area = st.session_state.fl_sub[st.session_state.fl_sub['zipcode'] == receive_zip]
      return pd.Series([match.iloc[0]["zone"], area.iloc[0]["area"]])
    else: 
      return pd.Series([match.iloc[0]["zone"], match.iloc[0]["state"]])

# takes in dataframe of raw data and calculates various stats for only delivered packages
def calculate_delivered_stats(raw_data):
  delivered_data = raw_data[raw_data['latest_router_description'] == 'Delivered.']

  delivered_data["pickup time"] = delivered_data.apply(calculate_pickup_time, axis = 1)
  delivered_data["pickup time to delivery time"] = delivered_data.apply(calculate_warehouse_time, axis = 1)
  delivered_data["created to delivery time"] = delivered_data.apply(calculate_created_to_delivery_time, axis = 1)
  delivered_data["starting area"] = delivered_data.apply(find_sender_start, axis = 1)
  load_zones()
  delivered_data[["zone", "receive state"]] = delivered_data.apply(find_zone, axis = 1)
  # st.write(delivered_data)

  # aggregated stats
  st.session_state.areas_times = delivered_data.groupby(['starting area', 'receive state']).agg({'pickup time to delivery time': 'mean', "created to delivery time": 'mean'})
  st.session_state.areas_times["pickup time to delivery time"] = round(st.session_state.areas_times["pickup time to delivery time"], 2).astype(str) + " days"
  st.session_state.areas_times["created to delivery time"] = round(st.session_state.areas_times["created to delivery time"], 2).astype(str) + " days"
  st.session_state.zones_times = delivered_data.groupby(['zone']).agg({'pickup time to delivery time': 'mean', "created to delivery time": 'mean'})
  st.session_state.zones_times["pickup time to delivery time"] = round(st.session_state.zones_times["pickup time to delivery time"], 2).astype(str) + " days"
  st.session_state.zones_times["created to delivery time"] = round(st.session_state.zones_times["created to delivery time"], 2).astype(str) + " days"

def calculate_undelivered_percentage(row):
  return str(round((1 - row["delivered packages"] / row["all packages"]) * 100 , 2)) + "%"

def build_area_zone_recieve(data):
  data["starting area"] = data.apply(find_sender_start, axis = 1)
  data[["zone", "receive state"]] = data.apply(find_zone, axis = 1)
  return data

def group_by_zone_and_area(data, col_name):
  by_zone = data.groupby(['zone']).agg(**{col_name: ("tracking_number",'count')})
  by_area = data.groupby(['starting area', 'receive state']).agg(**{col_name: ("tracking_number",'count')})
  return by_zone, by_area

def build_undelivered_table(all, delivered):
  combined = pd.concat([all, delivered], axis=1)
  combined["% undelivered"] = combined.apply(calculate_undelivered_percentage, axis = 1)
  return combined

# tried to optimize this as much as possible but it still repeats a lot 
def calculate_undelivered_stats(raw_data):
  load_zones()

  # all packages minus cancelled packages 
  without_cancelled = raw_data[raw_data
  ['latest_router_description'] != 'Shipment cancelled.']
  build_area_zone_recieve(without_cancelled)
  # build groups 
  zones_without_cancelled, areas_without_cancelled = group_by_zone_and_area(without_cancelled, "all packages")

  # only delivered packages 
  only_delivered = st.session_state.raw_data[st.session_state.raw_data
  ['latest_router_description'] == 'Delivered.']
  build_area_zone_recieve(only_delivered)
  zones_only_delivered, areas_only_delivered = group_by_zone_and_area(only_delivered, "delivered packages")

  # aggregate into one table and calculate percentages 
  st.session_state.zones_undelivered = build_undelivered_table(zones_without_cancelled, zones_only_delivered)
  st.session_state.areas_undelivered = build_undelivered_table(areas_without_cancelled, areas_only_delivered)

def within_target(row, zone_targets):
  target = zone_targets[row["zone"]]
  return float(target) > float(row["pickup time to delivery time"])

# optimize these theyre copies of the other one
def calculate_target_percentage(row):
  return str(round(row["within target"] / row ["all packages"] * 100, 2)) + "%"

# optimize these theyre copies of the other one
def build_target_table(all, target):
  combined = pd.concat([all, target], axis=1)
  combined["% in target"] = combined.apply(calculate_target_percentage, axis = 1)
  return combined

# start and end date picker
start_date = st.date_input("pick start date")
end_date = st.date_input("pick end date")
st.write("start date:", start_date, "end date:", end_date) 

# use session state so raw data doesn't change with button pushes 
if st.button("fetch from db"):
  st.session_state.raw_data = pd.DataFrame(fetch_data(start_date, end_date))
if "raw_data" in st.session_state:
  st.write(st.session_state.raw_data)

if st.button("calculate time stats"):
  calculate_delivered_stats(st.session_state.raw_data)
if "areas_times" in st.session_state:
  st.write("Aggregations by area")
  st.write(st.session_state.areas_times)
if "zones_times" in st.session_state:
  st.write("Aggregations by zone")
  st.write(st.session_state.zones_times)

if st.button("calculate undelivered stats"):
  calculate_undelivered_stats(st.session_state.raw_data)
if "zones_undelivered" in st.session_state:
  st.write(st.session_state.zones_undelivered)
if "areas_undelivered" in st.session_state:
  st.write(st.session_state.areas_undelivered)

st.divider()
zone2_target = st.text_input("Zone 2 Target Time", "2")
zone3_target = st.text_input("Zone 3 Target Time", "3")
zone4_target = st.text_input("Zone 4 Target Time", "5")
zone6_target = st.text_input("Zone 6 Target Time", "7")
zone8_target = st.text_input("Zone 8 Target Time", "9")

zone_targets = {0: 0, 2: zone2_target, 3: zone3_target, 4: zone4_target, 6: zone6_target, 8: zone8_target}

if st.button("calculate target time stats"):
  # all packages without cancelled packages
  without_cancelled = st.session_state.raw_data[st.session_state.raw_data['latest_router_description'] != 'Shipment cancelled.']
  x = build_area_zone_recieve(without_cancelled)
  
  # filter into only delivered packages that are within the target times
  # optimize this 
  only_delivered = st.session_state.raw_data[st.session_state.raw_data
  ['latest_router_description'] == 'Delivered.']
  build_area_zone_recieve(only_delivered)
  only_delivered["pickup time"] = only_delivered.apply(calculate_pickup_time, axis = 1)
  only_delivered["pickup time to delivery time"] = only_delivered.apply(calculate_warehouse_time, axis = 1)
  only_delivered["within target"] = only_delivered.apply(lambda row: within_target(row, zone_targets), axis = 1)
  st.write(only_delivered)

  # boolean mask filtering
  in_target = only_delivered[only_delivered["within target"]]

  # aggregate into zones and areas for calculation 
  zones_without_cancelled, areas_without_cancelled = group_by_zone_and_area(without_cancelled, "all packages")
  zones_within_target, areas_within_target = group_by_zone_and_area(in_target, "within target")

  st.session_state.target_zones = build_target_table (zones_without_cancelled, zones_within_target)
  st.session_state.target_areas = build_target_table (areas_without_cancelled, areas_within_target)

  if "target_zones" in st.session_state:
    st.write(st.session_state.target_zones)
  if "target_areas" in st.session_state:
    st.write(st.session_state.target_areas)

  

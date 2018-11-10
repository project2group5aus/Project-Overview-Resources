# Dependencies
import pandas as pd
import os
import sqlalchemy
import requests
import json


# import Google API Key from config.py file
from config import gkey


# Import data from local csv file to DataFrame
# Not doing so in main script-- importing data from databse below
# schools_df = pd.read_csv("../../group_docs/ncesdata_austin_schools_utf8.csv")

# Import data by using SQL Alchemy + pandas to read in SQLlite database as df
from sqlalchemy import create_engine

# connection to sqlite db
engine = create_engine('sqlite:///../../group_docs/schools_Austin_region.sqlite') # this is a relative path to the db file
conn = engine.connect()

schools_df = pd.read_sql_table("SchoolsAustin", conn, index_col="id")

# reduce df and cast all values as strings
addressesDF = schools_df[['campus_id', 'campus_name', 'address', 'zipcode']].astype(str)

# concatenate full addresses from df and push to list
fullAdresses = addressesDF['address']+' '+ addressesDF['zipcode']
fullAdresses.tolist()

# lists to hold coordinate results
lats = []
lngs = []

# api call loop
for i in fullAdresses:
    address = i
    
    # Build the endpoint URL
    target_url = "https://maps.googleapis.com/maps/api/geocode/json?" \
        "address=%s&key=%s" % (address, gkey)
    
    # Run a request to endpoint and convert result to json
    geo_data = requests.get(target_url).json()
    
    # Extract latitude and longitude values and append to coordinate lists
    lats.append(geo_data["results"][0]["geometry"]["location"]["lat"])
    lngs.append (geo_data["results"][0]["geometry"]["location"]["lng"])

# add coords to current df if desired.
# not doing so in main script-- going to make separate df below
# school_list['lat'] = lats
# school_list['lng'] = lngs
 
# creating a new DF via dictionary with just the campus ID and coords
d = {}
d['campus_id'] = addressesDF.loc[:, 'campus_id'].tolist()
d['latitude'] = lats
d['longitude'] = lngs

coordinatesDF = pd.DataFrame(data=d)

# export DF as a new table to database access via join on campusID
coordinatesDF.to_sql("school_coords", conn, index=False)



# =============================================================
# alternatively here is the syntax to create a dict object 
# similar to what is already expected by logic.js
# This is useful for copying over hard-coded data for testing
# not doing so in main script since this data will come dynamically from the flask route on Index.html

# school_coords = []

# for column,row in school_list.iterrows():
#     d = {}
#     coords = []
#     d['School Name'] = row['School Name']
#     coords.append(row['lats'])
#     coords.append(row['lngs'])
#     d['location'] = coords
#     school_list.append(d)

# example of output:
# school_coords = [
#   {'School Name': 'ADULT TRANSITION SERVICES','location': [30.2729452, -97.8020004]},
#   {'School Name': 'AKINS H S', 'location': [30.1489844, -97.80185469999999]}...]
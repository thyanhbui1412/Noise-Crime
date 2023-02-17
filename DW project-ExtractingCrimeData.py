#!/usr/bin/env python
# coding: utf-8

# ## Import packages

# In[19]:


import pandas as pd
from sodapy import Socrata
from pandas.io import gbq
import reverse_geocoder as rg
import pprint


# ## Connect to source with API

# In[38]:


data_url='data.cityofnewyork.us'    
DATASET_ID='8h9b-rp9u'     
app_token='tcq59OUouGqafBvifmUs3j6B8' 
client = Socrata(data_url,app_token,timeout=60000)
where_clause="date_extract_y(arrest_date) IN (2021) AND latitude IS NOT NULL"
select_clause="ARREST_KEY,ARREST_DATE,PD_CD,PD_DESC,KY_CD,OFNS_DESC,ARREST_BORO,ARREST_PRECINCT,AGE_GROUP,PERP_SEX,PERP_RACE,Latitude,Longitude"


# # Total rows of dataset

# In[39]:


record_count = client.get(DATASET_ID, where=where_clause, select="COUNT(*)")


# In[40]:


record_count


# # Streaming data from API to GCP
# 

# In[41]:


start = 0            # Start at 0
chunk_size = 200000    # Fetch 2000 rows at a time
datadump=[]
while True:
    rows=client.get(DATASET_ID, where=where_clause, select=select_clause, offset=start, limit=chunk_size)
    df = pd.DataFrame.from_records(rows).drop_duplicates(subset='ARREST_KEY', keep='first', inplace=False)
    df.to_gbq(destination_table='Crime.Crime2021',project_id='dw-finalproject',chunksize=2000,reauth=False,table_schema=None,if_exists='append',auth_local_webserver=True, progress_bar=True, credentials=None)
    datadump.extend(rows)
    start = start + chunk_size
    if(start > 155507):
        break
# Convert the list into a data frame


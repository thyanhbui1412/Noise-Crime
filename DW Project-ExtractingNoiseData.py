#!/usr/bin/env python
# coding: utf-8

# ## Import packages

# In[1]:


import pandas as pd
from sodapy import Socrata
from pandas.io import gbq


# ## Connect to source with API

# In[2]:


data_url='data.cityofnewyork.us'    
DATASET_ID='erm2-nwe9'     
app_token='tcq59OUouGqafBvifmUs3j6B8' 
client = Socrata(data_url,app_token,timeout=60000)
where_clause="complaint_type LIKE '%Noise%' AND date_extract_y(created_date) IN (2022) AND latitude IS NOT NULL"
select_clause="distinct unique_key,created_date,closed_date,status,Open_Data_Channel_Type,complaint_type,location_type,descriptor,incident_zip,incident_address,borough,latitude,longitude"


# # Total rows of dataset

# In[3]:


record_count = client.get(DATASET_ID, where=where_clause, select="COUNT(*)")


# In[ ]:


record_count


# # Streaming data from API to GCP 
# 

# In[4]:


start = 0             # Start at 0
chunk_size = 200000     # Fetch 2000 rows at a time
datadump=[]
while True:
    rows=client.get(DATASET_ID, where=where_clause, select=select_clause, offset=start, limit=chunk_size)
    df = pd.DataFrame.from_records(rows).drop_duplicates(subset='unique_key', keep='first', inplace=False)
    df.to_gbq(destination_table='Noise.Noise2022',project_id='dw-finalproject',chunksize=2000,reauth=False,table_schema=None,if_exists='append',auth_local_webserver=True, progress_bar=True, credentials=None)
    datadump.extend(rows)
    start = start + chunk_size
    if(start > 683297):
        break
# Convert the list into a data frame


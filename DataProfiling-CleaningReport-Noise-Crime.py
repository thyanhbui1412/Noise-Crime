#!/usr/bin/env python
# coding: utf-8

# In[2]:


# Import dependancies
import pandas as pd
from pandas_profiling import ProfileReport
import db_dtypes 
import google
from google.cloud import bigquery_storage
from google.cloud import bigquery


# In[3]:


#Authentificate GCP credentials
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/thyan/Downloads/dw-finalproject-18097d4b15be.json"


# In[4]:


client = bigquery.Client()
# function to create dataframe
def get_data(sql_query):

    # Make an API request.
    query_job = client.query(sql_query)

    # Convert as dataframe
    dataframe = (
        query_job
        .result()
        .to_dataframe()
    )

    return dataframe


# In[4]:


## Noise 
df=get_data("""SELECT * FROM `dw-finalproject.Noise.AllNoise` ORDER BY RAND() LIMIT 10000""")


# In[5]:


profile = df.profile_report(title='Pandas Profiling Noise Data')


# In[ ]:


profile.to_file(output_file="pandas_profiling-NoiseData.html")


# In[5]:


## Check After Cleaning
df1=get_data("""SELECT * FROM `dw-finalproject.Noise.Noise19-22`""")
df1.isnull().any()


# In[ ]:


df2=get_data("""SELECT * FROM `dw-finalproject.Crime.Crime` ORDER BY RAND() LIMIT 10000""")


# In[ ]:


profile1 = df.profile_report(title='Pandas Profiling Crime Data')


# In[ ]:


profile1.to_file(output_file="pandas_profiling-CrimeData.html")


# In[6]:


## Check After Cleaning
df3=get_data("""SELECT * FROM `dw-finalproject.Crime.Crime19-22`""")
df3.isnull().any()


# In[ ]:






# coding: utf-8

# In[81]:

import urllib.request as request
import warnings
warnings.filterwarnings('ignore')
import math
import os
import shutil
import zipfile
import pandas as pd
import numpy as np
import logging
import argparse
import boto3
from botocore.client import Config


# In[82]:

# create logger with 'CaseStudy1Part2'
logger = logging.getLogger('CaseStudy1Part2')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('CaseStudy1Part2.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)


# In[83]:

parser = argparse.ArgumentParser('CaseStudy1Part2')
parser.add_argument('-y','--year', help='The year for which the log file is to be downloaded.', type=str, default = '2005', nargs = '?')
parser.add_argument('-d','--day', help='The day of each month for which the log file is to be downloaded.', type=str, default = '01', nargs = '?')
parser.add_argument('-k','--awskey', help='The key to the aws account where the files is to be uploaded.', type=str)
parser.add_argument('-s','--awssecretkey', help='The secret key to the aws account where the files is to be uploaded', type=str)
parser.add_argument('-b','--awsbucket', help='The bucket where the files is to be uploaded', type=str)
args = parser.parse_args()


# In[84]:

yy = args.year
dd = args.day
awskey = args.awskey
awssecretkey = args.awssecretkey
awsbucket = args.awsbucket
months = ['01','02','03','04','05','06','07','08','09','10','11','12']
path = '.\\Part2Logs\\'


# In[85]:

os.makedirs(os.path.join(path), exist_ok=True)
for mm in months:
    url = 'http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/' + yy + '/Qtr' + str(math.ceil(int(mm)/3.)) + '/log' + yy + mm + dd + '.zip'
    request.urlretrieve(url,os.path.join(path,'log' + yy + mm + dd + '.zip'))


# In[86]:

dir_name = '.\\Part2Logs'
ext = '.zip'
for item in os.listdir(dir_name): 
    if item.endswith(ext): 
        file_name = os.path.join(path,item) 
        zip_ref = zipfile.ZipFile(file_name)
        zip_ref.extractall(dir_name) 
        zip_ref.close() 
        os.remove(file_name) 


# In[87]:

combined_csv = pd.concat( [ pd.read_csv(path + item) for item in os.listdir(dir_name) if item.endswith('.csv') ] )
for item in os.listdir(dir_name):
    if item.endswith('.csv'):
        os.remove(os.path.join(path,item))                 


# In[88]:

def handle_missing_browser(df):
    b_df = df[['ip','browser','cik']]
    b_df = b_df[b_df['browser'].notnull()].drop_duplicates()
    b_df = b_df.groupby(by=['ip', 'browser']).count().reset_index().sort_values(by=['cik'], ascending=False).groupby(by=['ip', 'browser']).head(1)
    b_df = b_df[['ip', 'browser']]
    df = pd.merge(df, b_df, on='ip', how='outer')   
    df['browser_y'].fillna('Unknown', inplace=True)
    df.drop(['browser_x'], axis=1, inplace=True)
    df.rename(columns={'browser_y': 'browser'}, inplace=True)
    return df


# In[89]:

def handle_missing_size(df):
    file_size_dict = df[['accession', 'extention', 'size']].drop_duplicates()[df['size'].notnull()]
    for index, row in df.iterrows():
        if row['size'] == np.NaN:
            row_accession = row['accession']
            row_extention = row['extention']
            row['size'] = extention_size_dict.loc[(df['accession'] == row_accession) & (df['extention'] == row_extention)]
    ext_size_dict = df[['extention', 'size']].drop_duplicates()[df['size'].notnull()].groupby('extention').mean()
    for index, row in df.iterrows():
        if row['size'] == np.NaN:
            row_extention = row['extention']
            row['size'] = extention_size_dict.loc[row_extention]
    return df


# In[90]:

df = combined_csv
nan_df = df.apply(lambda x: sum(x.isnull()), axis=0).reset_index()
nan_df.columns = ['Column_Name', 'No of NaNs']
nan_df.to_csv(os.path.join(path,'missingdata.csv'))
bdf = handle_missing_browser(df)
edf = handle_missing_size(bdf)
edf.to_csv(os.path.join(path,'combined.csv'))


# In[91]:

edf['datetime'] = edf['date'].map(str) + ' ' + edf['time'].map(str)
edf['datetime'] = pd.to_datetime(edf['datetime'])
edf.drop(['date', 'time'], axis=1, inplace=True)


# In[92]:

edf['quarter'] = edf['datetime'].dt.quarter
edf['month'] = edf['datetime'].dt.month
temp_df = edf[['cik', 'month', 'ip']]
temp_df = temp_df.groupby(['month', 'cik']).count()
temp_df = temp_df.reset_index()
temp_df = temp_df.sort_values(by=['month', 'ip'], ascending=False).groupby('month').head(3)
temp_df.to_csv(os.path.join(path,'cik-month.csv'))


# In[93]:

edf['hour'] = edf['datetime'].dt.hour
conditions = [(edf['hour'] >= 18.0) & (edf['hour'] <= 23.0),(edf['hour'] <18.0) & (edf['hour'] >12.0),(edf['hour'] <=12.0)&(edf['hour'] >= 0.0)] 
choices = ['Night','Day','Morning']
edf['tod'] = np.select(conditions, choices)
temp_df = edf[['tod','ip']].groupby('tod').count().reset_index().sort_values(by=['ip'], ascending=False).head(3)
temp_df.to_csv(os.path.join(path,'tod-ipcount.csv'))


# In[94]:

shutil.make_archive('Part2logs', 'zip', path)
shutil.rmtree(os.path.join('.\\Part2Logs')) 


# In[95]:

file = 'Part2Logs.zip'
data = open(file, 'rb')
# S3 Connect
s3 = boto3.resource(
    's3',
    aws_access_key_id=awskey,
    aws_secret_access_key=awssecretkey,
    config=Config(signature_version='s3v4')
)
# File Upload
s3.Bucket(awsbucket).put_object(Key=file, Body=data, ACL='public-read-write')
data.close()
os.remove(os.path.join('.\\',file))
print ("Done")


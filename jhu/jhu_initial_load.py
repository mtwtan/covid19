import datetime
import boto3
import os
import csv
import io
import numpy as np
import pandas as pd
import json
import requests

bucket = "<S3 Bucket Name>" #### Enter your S3 bucket name
rootkey = "covid-19/jhu/daily/" #### Modify as necessary for the key in your S3 bucket. Remember to ADD trailing slash /
baseurl = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"
jhu_folder = "/tmp/jhu/"

# Functions (beginning)

def f_exist(status):
  if status == 200:
    return jhu_file + ' exists'
  else:
    return jhu_file + ' does not exist'

def getCsvFile(month,day,year):
  jhu_file = month + "-" + day + "-" + year + ".csv"
  return jhu_file

def getS3key(month,day,year, jhu_file):
  key = year + "/" + month + "/" + day + "/" + jhu_file
  return key

def download_file(url,jhu_file):

  if not os.path.exists(jhu_folder):
    os.makedirs(jhu_folder)
  
  df_jhu = pd.read_csv(url, delimiter=',', header=0)
  df_jhu.columns = df_jhu.columns.str.replace('/','_')
 
  tmppath = jhu_folder + jhu_file
  df_jhu.to_csv(tmppath, index=False, sep=",", encoding='utf-8')
  return_msg = "File downloaded to " + tmppath

  return return_msg

def upload_s3(day,month,year,jhu_file):
  daily_file = "daily.csv"
  s3 = boto3.resource('s3')
  s3key = rootkey + 'year=' + year + '/month=' + month + '/day=' + day + '/' + daily_file
  s3.meta.client.upload_file(jhu_folder + jhu_file, bucket, s3key)

  return_msg = "Uploaded to S3 on s3://" + bucket + '/' + s3key

  return return_msg

# Functions (end)

day_delta = datetime.timedelta(days=1)
s_date = "01-22-2020" ### Date of the first CSV file in repository
start_date = datetime.datetime.strptime(s_date, '%m-%d-%Y')
e_date = "04-01-2020" ### Change date to the latest CSV file in repository
end_date = datetime.datetime.strptime(e_date, '%m-%d-%Y')

# Looping to download from GitHub and upload into S3 bucket in your account

for i in range((end_date - start_date).days + 1):
    currdate = start_date + i*day_delta
    print(currdate)

    day = str(currdate.day).rjust(2, '0')
    month = str(currdate.month).rjust(2, '0')
    year = str(currdate.year)

    jhu_file = getCsvFile(month,day,year)

    key = getS3key(month,day,year,jhu_file)

    url = baseurl + jhu_file

    request = requests.get(url)
    print(f_exist(request.status_code))

    if request.status_code == 200:
      print(download_file(url,jhu_file))
    
    if os.path.exists(jhu_folder + jhu_file):
      print(upload_s3(day,month,year,jhu_file))

"""
Jaye Hicks
Obligatory legal disclaimer:
  You are free to use this source code (this file and all other files 
  referenced in this file) "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER 
  EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. 
  THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THIS SOURCE CODE
  IS WITH YOU.  SHOULD THE SOURCE CODE PROVE DEFECTIVE, YOU ASSUME THE
  COST OF ALL NECESSARY SERVICING, REPAIR OR CORRECTION. See the GNU 
  GENERAL PUBLIC LICENSE Version 3, 29 June 2007 for more details.
  
This module serves as an AWS Lambda function intended to be invoked (on
a scheduled, regular basis) by a CloudWatch Events Rule in order to 
archive raw sensor data.  Raw sensor data housed in the DynamoDB table 
DynTableForStationData that has aged beyond the designated age threshold is
archived into files placed in the S3 bucket
'my_bucket.'  

Lonesome Vine Vineyard operates a maximum of 21 sensors stations.  On a 
monthly basis the maximum number of DynamoDB Items that could be added
to the DynamoDB table DynTableForStationData is 15,624 (21 stations x 24 sensor
station reports per station per day x 31 days).  An upper bound estimate
of 70 bytes per single sensor station report yields 1,093,680 bytes of 
total raw sensor data per month.

Empirical observation of this module running on a laptop, working with
max volumes of raw sensor data, yielded run times of between 5 and 20
seconds.  This was over a 50 MBs Internet connection (i.e., north
Texas to North Virginia).  Throttling was when the DynamoDB table was
configured at the default settings of 5 read capacity units and 5 write 
capacity units.  To address the throttling issue the table was modified 
from default capacity settings to 'on-demand' read/write capacity.

The DynamoDB table DynTableForStationData contains raw vineyard sensor data 
across all vineyard sensor stations reporting data (either in an 
online status or in an offline status).  A separate Python module is 
responsible for deleting raw data that has been archived by this module.

If this module finds raw sensor data for a given calendar month that has
not yet been archived, it will archive that data into a CSV file,  
placing that file into the S3 archival bucket 
'my_bucket.'  A single CSV file will archive raw 
sensor data across all sensors stations for a given calendar month.  
Archive files are named using an exacting naming standard that encodes 
the year and month within the file name.  This module wil not overwrite 
any preexisting CSV files contained in the S3 archive bucket.

A custom system logging class was created to capture system logging
messages generated while this module executes.  At the conclusion of
this module's execution the system logging object writes all of the
system logging messages to DynamoDB tables.  Because this module
executes as an AWS Lambda function, and the Lambda retains containers
for a short period of time after the function exits (in hopes of 
reusing the container for a future call of the same function), it is
necessary to reset/clear the system logging object at the beginning
of this module's execution.

Usage: 
  A CloudWatch Events Rule invokes, on an regularly scheduled basis, this
  AWS Lambda function 

Dependencies:
  import csv
  import boto3
  from boto3.dynamodb.conditions import Key
  from botocore.exceptions import ClientError
  from dateutil.relativedelta import * 
  from io import StringIO, BytesIO
  from datetime import datetime, timedelta
  import sys_log
"""
import csv
import boto3
from   boto3.dynamodb.conditions import Key
from   botocore.exceptions       import ClientError
from   dateutil.relativedelta    import * 
from   io                        import StringIO, BytesIO
from   datetime                  import datetime, timedelta

import sys_log
          
RAW_DATA_TABLE             = 'DynTableForStationData'
STATIONS_PER_ARCHIVE_TABLE = 'DynTableForArchive'
MONTHS_BACK_LIMIT          = 12 #how months back to go back
OLDEST_ARCHIVE             = '2020-11'
ARCHIVAL_BUCKET_NAME       = 'my_bucket'

#global object provides system logging to DynamoDB tables
sl = sys_log.sys_log('archive_data','DynTableForInfo',
                                    'DynTableForIssues','','')


def _store_num_stations_in_archive(file_name, count):
  """
  Record the number of sensor stations that reported raw sensor data
  in a given year-month time period.
  """
  results = True
  try:
    dynamo_db_access = boto3.resource('dynamodb')
    table = dynamo_db_access.Table(STATIONS_PER_ARCHIVE_TABLE)
    try:
      response = table.put_item(Item={'date' : file_name[:7],
                                      'count' : str(count)})
    except Exception as e:
      results = False
      sl.log_message('7', 'ERROR', '', e)   
  except Exception as e:
    results = False
    sl.log_message('8', 'ERROR', '', e)
  return(results)
 
     
def _archive_raw_sensor_data(new_file):
  """
  Retrieve all sensor data, across all sensors, that falls within the
  year-month specified by the paramater and arhive off to a file in
  the S3 bucket 'my_bucket.'  The csv archival file
  named using a strict naming standard encoding year-month of data.
  """
  result = True
  items_archived = 0
  csv_headers = ['location', 'day', 'time ', 'battery', 
                 'temp', 'sms1', 'sms2', 'sms3','timestamp', 'id']
  year_month = new_file[:7]
    
  try:
    dynamo_db_access = boto3.resource('dynamodb')
    table = dynamo_db_access.Table(RAW_DATA_TABLE)
    try:
      response = table.query(
        KeyConditionExpression=Key('date').eq(year_month))
      if(response['Items']):
        page_to_process = True
        in_memory_buffer = StringIO()
        csv_writer = csv.writer(in_memory_buffer)
        csv_writer.writerow(csv_headers)
        locs_reporting = []
        while(page_to_process):
          for item in response['Items']:
            parts = item['day_ts_loc'].split('+')
            location = parts[2]
            if(location not in locs_reporting):
              locs_reporting.append(location)
            date_time = datetime.fromtimestamp(int(parts[1]))
            date_time = date_time - timedelta(hours=6) #UTC to US Central
            csv_writer.writerow([location, str(date_time.day), 
                                 str(date_time.hour).zfill(2) + ':'
                                 + str(date_time.minute).zfill(2), 
                                 item['battery'], item['temp'], 
                                 item['sms1'], item['sms2'],
                                 item['sms3'], parts[1],item['id']])
            items_archived += 1
            
          #if response has multiple pages, process them all
          if('LastEvaluatedKey' in response):
            try:
              response = table.query(
                KeyConditionExpression=Key('date').eq(year_month),
                ExclusiveStartKey=response['LastEvaluatedKey'])            
            except Exception as e:
              result = False
              page_to_process = False
              sl.log_message('9', 'ERROR', '', e)               
              break
          else:
            page_to_process = False
            
        if(result and items_archived):
          if(locs_reporting):
            if(not _store_num_stations_in_archive(new_file, len(locs_reporting))):
              sl.log('16', 'ERROR', 'Couldnt write to archive count table','')
          try:
            s3_access = boto3.client('s3')
            encoded_data = in_memory_buffer.getvalue().encode()
            s3_access.put_object(Bucket=ARCHIVAL_BUCKET_NAME, Key=new_file, 
                                 Body=encoded_data)
          except Exception as e:
            result = False
            sl.log_message('10', 'ERROR', '', e)   
    except Exception as e:
      result = False
      sl.log_message('11', 'ERROR', '', e)
  except Exception as e:
    result = False
    sl.log_message('12', 'ERROR', '', e)
  if(result and items_archived):
    sl.log_message('13', 'INFO', 'Archived all raw sensor data records for: ' + 
                   year_month + ' (' + str(items_archived) + 
                   ' sensor records archived).', '')
  return(result)


def _all_possible_archive_file_names():
  """
  Create a list of all object names (i.e., files containing archived raw
  sensor data) that should exist in the S3 archive bucket named
  'my_bucket'.  Archive files created using strict
  naming conventions.  The '>1' in while statement keeps the current
  an previous months from being considered for archival.
  """
  file_names = []
  CURRENT_DATE_TIME = datetime.now()
  month_counter = MONTHS_BACK_LIMIT
  
  while(month_counter > 1):
    prior_date_time = (CURRENT_DATE_TIME - relativedelta(months=month_counter))
    year_month = (str(prior_date_time.year) + '-' + 
      str(prior_date_time.month).zfill(2))
    if(year_month >= OLDEST_ARCHIVE):      
      file_names.append(year_month +  '-VineyardStations.csv')
    month_counter -= 1
  return(file_names)
  
  
def archive_data(event, context):
  """
  Archive sensor data for a given year-month, across all sensors, to
  a single file (strict naming standard that encodes year-month) to
  the S3 bucket 'my_bucket.'  Any preexisting S3 
  files will be preserved; not overwritten.
  
  Note: the non existance of expected archive file is detected by 
  catching an exception (which does not represent a processing error)  
  """
  sl.reset()
  
  try:
    s3_access_client = boto3.client('s3')
    s3_access_client.head_bucket(Bucket=ARCHIVAL_BUCKET_NAME)
    file_names = _all_possible_archive_file_names()
    try:
      s3_access_resource = boto3.resource('s3')        
      for a_file in file_names:
        try:
          s3_access_resource.Object(ARCHIVAL_BUCKET_NAME, a_file).load()
        except ClientError as e: 
          error_code = int(e.response['Error']['Code'])
        
          if(error_code == 404):          #not an error; detects missing files
            if(not _archive_raw_sensor_data(a_file)):
              break
          else:                           #a processing error detected
            sl.log_message('1', 'ERROR', '', e)
            break
        except Exception as e:            #caught non ClientError exception
          sl.log_message('14', 'ERROR', '', e)           
          break
    except Exception as e:
      sl.log_message('2', 'ERROR', '', e)
            
  except ClientError as e:
    error_code = int(e.response['Error']['Code'])
    if(error_code == 404):
      sl.log_message('3', 'ERROR', 'The archival bucket "' + 
                     RCHIVAL_BUCKET_NAME + '" does not exist.', '')  
    else:
      sl.log_message('4', 'ERROR', '', e)
  except Exception as e:                 #caught non ClientError exception
    sl.log_message('15', 'ERROR', '', e) 
  
  if(sl.error_messages):
    sl.log_message('5', 'WARN', 
                   'archive_data() job did not complete successfully.', '')
  else:
    sl.log_message('6', 'INFO', 'archive_data() job completed successfully.', 
                   '') 
  sl.save_messages_to_db()
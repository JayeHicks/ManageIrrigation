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
  
This module serves as an AWS Lambda function intended to be invoked on
a regular scheduled basis by a CloudWatch Events rule in order to 
detect missing archived vineyard sensor station files as well as
archived vineyard sensor station files that appear unusually small.
This will be accomplished by accessed the archival S3 bucket, 
searching for the existance of files, and checking the size of any
archival files located.  archive-check functionality relies on
solution-wide adherance to a strict naming standard for the CSV 
archival files.  These archival files contain raw vineyard sensor 
data, across all 21 stations, bundled up in calendar monthly increments
and reside in the S3 bucket named 'my_bucket.'  
This module will not send alerts; it will log a WARN informational
logging message if it uncovers anything suspect.

It is recommended that this Python module be run every 2 - 4 weeks.

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
  A CloudWatch Events Rule invokes, on an regularly scheduled basis, 
  this AWS Lambda function

Dependencies:
  import boto3
  from botocore.exceptions import ClientError
  from boto3.dynamodb.conditions import Key
  from dateutil.relativedelta import * 
  from datetime import datetime, timedelta
  import sys_log
"""
import boto3
from   botocore.exceptions       import ClientError
from   boto3.dynamodb.conditions import Key
from   dateutil.relativedelta    import * 
from   datetime                  import datetime

import sys_log

ARCHIVAL_BUCKET_NAME       = 'my_bucket'
CONTROL_TABLE              = 'ADynamoDBTable1'
STATIONS_PER_ARCHIVE_TABLE = 'ADynamoDBTable2'
CONTROL_ITEM_SELECT        = '2'
MONTHS_BACK_LIMIT          = 12 #how months back to go back
OLDEST_ARCHIVE             = '2020-11'

#global object provides system logging to DynamoDB tables
sl = sys_log.sys_log('archive_check','DynamoDBTableForInfo',
                                     'DynamoDBTableForIssues','','')

 
def _archive_file_is_small(file, size, threshold):
  """
  Retrieve the number of sensor stations that contributed sensor data
  to the archive file.  Based on this number and the threshold, which
  represents the minimum amount of data that a single sensor station 
  should contribute per month, determine if the archive file appears
  unusually small.
  
  Args:
    file (str)      the archive file
    size (int)      the size of the archive file in bytes
    threshold(int)  the minimum amount of bytes that a single sensor 
                    station will generate in a month
                    
  Retruns:
    True:  if archive file is too small
    False: if archive file is ok
  
  """
  result = False
    
  try:
    dynamo_db_access = boto3.resource('dynamodb')
    table = dynamo_db_access.Table(STATIONS_PER_ARCHIVE_TABLE)
    try:
      response = table.query(
        KeyConditionExpression=Key('date').eq(file[:7]))
      if(response['Items']):
        number_of_stations = int(response['Items'][0]['count'])
        if(size < (number_of_stations * threshold)):
          result = True        
      elif(size < threshold):
        result = True                
    except Exception as e:
      sl.log_message('1', 'ERROR', '', e)    
  except Exception as e:
    sl.log_message('2', 'ERROR', '', e)
  return(result)


def _all_possible_archive_file_names():
  """
  Create a list of all object names (i.e., files containing archived raw
  sensor data) that should exist in the S3 archive bucket named
  'my_bucket'.  Archive files created using strict
  naming conventions.  The '>1' in while statement keeps the current
  an previous months from being included in the list.
  """
  file_names        = []
  CURRENT_DATE_TIME = datetime.now()
  month_counter     = MONTHS_BACK_LIMIT
  
  while(month_counter > 1):
    prior_date_time = (CURRENT_DATE_TIME - relativedelta(months=month_counter))
    year_month = (str(prior_date_time.year) + '-' + 
      str(prior_date_time.month).zfill(2))
    if(year_month >= OLDEST_ARCHIVE):      
      file_names.append(year_month +  '-VineyardStations.csv')
    month_counter -= 1
  return(file_names)
 
 
def archive_check(event, context):
  """
  Look across into the S3 archival bucket and generate a list of all
  missing archive files and a list of all archive files that appear 
  unusually small.  Log WARN messages if anything found.
  
  Note: the non existance of expected archive file is detected by 
  catching an exception (which does not represent a processing error)  

  """
  warning_prefix1 = 'The following archive files are missing: '
  warning_prefix2 = 'The following archive files are unusually small: '
  
  sl.reset()
  
  try:
    s3_access_client = boto3.client('s3')
    s3_access_client.head_bucket(Bucket=ARCHIVAL_BUCKET_NAME)
    try:
      s3_access_resource = boto3.resource('s3')
      try:
        dynamo_db_access = boto3.resource('dynamodb')
        table = dynamo_db_access.Table(CONTROL_TABLE)
        try:
          response = table.query(
            KeyConditionExpression=Key('control_id').eq(CONTROL_ITEM_SELECT))
          if(response['Items']):
            ARCH_FILE_SIZE_THREHSOLD = int(
                                         response['Items'][0]['min_file_size'])
            file_names = _all_possible_archive_file_names()
            missing_files = []
            small_files = []
            for a_file in file_names:
              try:
                s3_access_resource.Object(ARCHIVAL_BUCKET_NAME, a_file).load()
                object_summary = s3_access_resource.ObjectSummary(
                                                  ARCHIVAL_BUCKET_NAME, a_file)
                if(_archive_file_is_small(a_file, object_summary.size, 
                                          ARCH_FILE_SIZE_THREHSOLD)):
                  small_files.append(a_file)
              except ClientError as e: 
                error_code = int(e.response['Error']['Code'])
        
                if(error_code == 404):    #not an error; detects missing files
                  missing_files.append(a_file)
                else:                     #a processing error detected
                  sl.log_message('3', 'ERROR', '', e)  
                  break
              except Exception as e:      #caught non ClientError exception
                sl.log_message('12', 'ERROR', '', e)  
                break
                
            if(missing_files):
              message = warning_prefix1
              for file in missing_files:
                message += file + ', '
              sl.log_message('4', 'WARN', message[:-2],'')

            if(small_files):
              message = warning_prefix2
              for file in small_files:
                message += file + ', '
              sl.log_message('5', 'WARN', message[:-2],'')     
                 
          else:
            sl.log_message('6', 'ERROR', 'Could not read control Item ' + 
              CONTROL_ITEM_SELECT + ' from DynamoDB control table.','')
        except Exception as e:
          sl.log_message('7', 'ERROR', '', e) 
      except Exception as e:
        sl.log_message('8', 'ERROR', '', e)               
    except Exception as e:
      sl.log_message('9', 'ERROR', '', e)             
  except ClientError as e:
    error_code = int(e.response['Error']['Code'])
    if(error_code == 404):
      sl.log_message('10', 'ERROR', 'The archival bucket "' + 
        ARCHIVAL_BUCKET_NAME + '" does not exist.', '')
    else:
      sl.log_message('11', 'ERROR', '', e)               
  except Exception as e:                  #caught non ClientError exception
    sl.log_message('13', 'ERROR', '', e)               

  if(sl.error_messages):
    sl.log_message('14', 'WARN', 'archive_check() job did not complete ' +
                   'successfully.', '')
  else:
    sl.log_message('15', 'INFO',
                   'archive_check() job completed successfully.', '')      
  sl.save_messages_to_db()
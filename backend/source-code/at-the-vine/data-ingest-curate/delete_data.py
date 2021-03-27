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
delete raw sensor data housed in a DynamoDB table DynTableForSensorData.  
Specifically, data that has aged beyond the defined age threhold will
be deleted. 

Lonesome Vine Vineyard operates a maximum of 21 sensors stations.  On a 
monthly basis the maximum number of DynamoDB Items that could be added
to the DynamoDB table DynTableForSensorData is 15,624 (21 stations x 24 sensor
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

The DynamoDB table contains raw vineyard sensor station data across all 
21 vineyard sensor stations.  A separate Python module is responsible 
for archiving off this raw data (i.e., before this module deletes it) 
by grouping all data for all sensors that belong to a single calendar 
month and saving that data in a CSV file that is stored in an S3 bucket.
A check is made to ensure that data has been archived prior to deleting
the data.

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
  AWS Lambda function by calling the function lambda_handler and passing
  to it the input parameters 'event' and 'context.'

Dependencies:
  import boto3
  from boto3.dynamodb.conditions import Key
  from botocore.exceptions import ClientError
  from dateutil.relativedelta import * 
  from datetime import datetime
  import sys_log
"""
import boto3
from   boto3.dynamodb.conditions import Key
from   botocore.exceptions       import ClientError
from   dateutil.relativedelta    import * 
from   datetime                  import datetime

import sys_log

ARCHIVAL_BUCKET_NAME           = 'my-bucket'
RAW_DATA_TABLE                 = 'DynTableForSensorData'       
MONTHS_BACK_LIMIT              = 12 #how months back to go back

#global object provides system logging to DynamoDB tables
sl = sys_log.sys_log('delete_data','DynTableForInfo',
                                   'DynTableForIssues','','')
 
 
def _archive_file_exists(date):
  """
  Determine if the arhival file for all of the raw sensor data for a
  'year-mo' exists in the archival bucket.
  """
  exists = False
  archive_file = date + '-VineyardStations.csv'
    
  try:
    s3_access_client = boto3.client('s3')
    s3_access_client.head_bucket(Bucket=ARCHIVAL_BUCKET_NAME)
    try:
      s3_access_resource = boto3.resource('s3')
      s3_access_resource.Object(ARCHIVAL_BUCKET_NAME, archive_file).load()
      exists = True #processing got here, file exists, return True
    except Exception as e: 
        pass #processing got here, file doesn't exist, return False
            
  except ClientError as e:
    error_code = int(e.response['Error']['Code'])
    if(error_code == 404):
      sl.log_message('11', 'ERROR', 'The archival bucket "' + 
                   ARCHIVAL_BUCKET_NAME + '" does not exist.', '')  
    else:
      sl.log_message('12', 'ERROR', '', e)
  except Exception as e: 
    sl.log_message('13', 'ERROR', '', e)  
   
  return(exists)
 

def _delete_items(date, response):
  """
  Delete all Items from Table with parition key == to parameter 'date'
  For example, delete all Items from DynamoDB table DynTableForSensorData
  where the Item's attribute 'date' (i.e., the partition key) is ==
  to a specific 'year-mo' (e.g., '2020-08')
  """
  result = True
  page_to_process = True
  items_deleted = 0
  
  try:
    dynamo_db_access = boto3.resource('dynamodb')
    table = dynamo_db_access.Table(RAW_DATA_TABLE)
    with table.batch_writer() as batch:
      while(page_to_process):
        for item in response['Items']:
          try:
            batch.delete_item(Key={'date': date,
                                   'day_ts_loc': item['day_ts_loc']})
            items_deleted += 1
          except Exception as e:
            result = False
            sl.log_message('6', 'ERROR', '', e)
            break
        #if response has multiple pages, process them all
        if('LastEvaluatedKey' in response):
          try:
            response = table.query(
              KeyConditionExpression=Key('date').eq(date),
              ExclusiveStartKey=response['LastEvaluatedKey'])               
          except Exception as e:
            result = False
            page_to_process = False
            sl.log_message('7', 'ERROR', '', e)
            break
        else:
          page_to_process = False
   
  except Exception as e:
    result = False
    sl.log_message('8', 'ERROR', '', e)
   
  if(result):
    sl.log_message('9', 'INFO', 'Deleted all raw sensor data records for: ' + 
                   date + ' (' + str(items_deleted) + ' sensor records ' +
                   'deleted).', '')
  return(result)
 
  
def _all_possible_months_to_delete():
  """
  Create a list of year-month values (e.g., ['2019-12', '2020-01']) from the
  limit of how many months to go back till two months from present.  The '>1' 
  in while loop keeps the current month and prior month off of the list.
  """
  months_to_delete = []
  CURRENT_DATE_TIME = datetime.now()
  month_counter = MONTHS_BACK_LIMIT
  
  while(month_counter > 1):
    prior_date_time = (CURRENT_DATE_TIME - relativedelta(months=month_counter))
    year_month = (str(prior_date_time.year) + '-' + 
      str(prior_date_time.month).zfill(2))              
    months_to_delete.append(year_month)
    month_counter -= 1
  return(months_to_delete)
  

def delete_data(event, context):
  """
  Raw vineyard sensor data for all sensor stations is stored in a single
  DynamoDB table.  Generate a list of all possible partition keys for 
  Items in the table (e.g., ['2020-01', '2020-02', ...]) for data that
  is old enough to have been archived.  Step through this list and
  delete any raw sensor data records from the table that has a partition
  key past the threshold (e.g., delete all records older than 6 months).

  """  
  sl.reset()
  delete_dates = _all_possible_months_to_delete() 
  
  try:
    dynamo_db_access = boto3.resource('dynamodb')
    table = dynamo_db_access.Table(RAW_DATA_TABLE)
    for date in delete_dates:
      try:
        response = table.query(KeyConditionExpression=Key('date').eq(date))
        if(response['Items']):
          if(_archive_file_exists(date)):
            if(not _delete_items(date,response)):
              break
          else:
            sl.log_message('1', 'WARN', 'Stopped deletion of raw sensor ' +
              'data records belonging to "' + date + '" because they have ' +
              'not yet been archived.', '')           
      except Exception as e:
        sl.log_message('2', 'ERROR', '', e)
  except Exception as e:
    sl.log_message('3', 'ERROR', '', e) 
    
  if(sl.error_messages):
    sl.log_message('4', 'WARN', 
      'delete_data() job did not complete successfully.', '') 
  else:
    sl.log_message('5', 'INFO', 'delete_data() job completed successfully.', 
                   '')  
  sl.save_messages_to_db()  
 
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
an adhoc basis when an update occurs to an IoT Core shadow document.
The update to IoT Core shadow documents typically occurs on a regularly
scheduled basis; updates occur as a result of the regular execution 
of the job pull_data.

A Python job, named pull_data, is regularly executed to access all 
recently arrived sensor data from any of the 21 vineayrd sensor
stations.  It retrieves this data from an Internet endpoint (i.e., 
www.thingspeak.com).  After collecting the sensor data records, 
pull_data cycles through records updating the IoT Core shadow document
that corresponds to the sensor record.  A dedicated IoT Core shadow 
document is assigned to each of the 21 sensor stations.  Incoming 
sensor data needs to route to three different AWS destinations: two 
DyanmoDB tables and to one of 21 CloudWatch custom metrics.  AWS 
service-to-service integrations handle two of these destinations but 
a Lambda function (i.e. this module) is required to handle the 
'existing Item update' integration with the DynamoDB table 
DynTableForStatus.
 
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
  Invoked via IoT Core -> Lambda service integration.  Values to use for
  updating the DynamoDB table passed via the default Lambda service
  parameters of 'event' and 'context.'

Dependencies:
  import boto3
  from boto3.dynamodb.conditions import Key
  import sys_log
"""
import boto3
from   boto3.dynamodb.conditions import Key

import sys_log

STATUS_TABLE = 'DynTableForStatus'
LOCATIONS    = ['A02N', 'A08M', 'A02S', 'B02N', 'B08M', 'B02S', 'C02N',
                'C08M', 'C02S', 'D02N', 'D08M', 'D02S', 'E02N', 'E08M', 
                'E02S', 'F02N', 'F08M', 'F02S', 'G02N', 'G08M', 'G02S']

#global object provides system logging to DynamoDB tables
sl = sys_log.sys_log('update_status','DynTableForInfo',
                                     'DynTableForIssues','','') 
 

def update_status(event, context):
  """
  The DynamoDB table dedicated for maintaining the last reported values
  for vineyard sensor stations, DynTableForStatus, contains 21 Items;
  one for each of the 21 stations.  There is a dedicated IoT Core
  shadow document for each of the 21 stations.  At any given point in
  time DynTableForStatus will contain the most recent sensor data
  readings across all 21 stations.
  
  IoT Core to DynamoDB service-to-service integration only supports 
  the insertion of new data (i.e., new Items).  That service
  integration is used for the DynamoDB table VinStationsData.  However,
  this module is required in order to support the updating of an 
  existing DynamoDB record/Item.  The IoT Core to DynamoDB 
  service-to-service integration calls this Lambda function which in 
  turn carries out the update of the Item.

  """
  sl.reset()
  
  if(event):
    parts = event['day_ts_loc'].split('+')
    location = parts[2].upper()
    if(location in LOCATIONS):
      try:
        dynamo_db_access = boto3.resource('dynamodb')
        table = dynamo_db_access.Table(STATUS_TABLE)
          
        try:
          update_expression = 'set battery = :b, tstamp = :ti, '
          update_expression += 'temperature = :te, sms1 = :s1, sms2 = :s2, '
          update_expression += 'sms3 = :s3'
          table.update_item(
            Key={'location': location},
                 UpdateExpression=update_expression,
                 ExpressionAttributeValues={':b' : event['battery'],
                                            ':ti' : parts[1],
                                            ':te' : event['temp'],
                                            ':s1' : event['sms1'],
                                            ':s2' : event['sms2'],
                                            ':s3' : event['sms3']}) 
        except Exception as e:
          sl.log_message('1', 'ERROR', '', e)
      except Exception as e:
        sl.log_message('2', 'ERROR', '', e)
    else:
      sl.log_message('3', 'ERROR', 'Raw sensor data update record received ' +
                     'with invalid station location: ' + parts[2], '')
  else:
    sl.log_message('4', 'ERROR', 'Empty parameter sent to Lambda function.', '')
  
  if(sl.error_messages):
    sl.log_message('5', 'WARN', 
                   'update_status() job did not complete successfully.', '')
  else:
    sl.log_message('6', 'INFO', 
                   'update_status() job completed successfully.', '')
  sl.save_messages_to_db() 
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

It is possible for one or more sensor stations to report a temperature 
below the threshold and yet not send an ALERT or log an ALARM
message.  Firstly, the 'freeze_guard' may be turned off (setting 
resides in the DynamoDB ADynamoDBTable2).  The 'freeze_guard' 
setting of 'on' / 'off' applies to all 21 vineyard stations.  Also, 
individual stations may be taken 'offline' (setting resides in the 
DynamoDB table ADynamoDBTable1).  Even if a station is taken 
'offline', incoming raw sensor data for that station will continue to 
update the VinStationsData table and the ADynamoDBTable1 table. And 
finally, if the most recent temperature reading for a sensor is below
the temperature threshold this will only qualify for an alarm state if
the most recent temperature reading is not outside of the data shelf 
life limit set for Freeze Guard.

Given that vineyard sensor stations report temperature once an hour (at
most), a small percentage of sensor reports fail to register in the
AWS backent, none of the 21 sensors are coordinated to report
sensor data at the same time, and the critically urgent need for as much
forewarning of a freeze event as possible, it is recommended that this
job run every 15 mins.

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
  this AWS Lambda function by calling the function lambda_handler and 
  passing to it the input parameters 'event' and 'context.'

Dependencies:
  import boto3
  from boto3.dynamodb.conditions import Key
  from datetime import datetime, timedelta
  import sys_log
  import send_alerts
"""
import boto3
from   boto3.dynamodb.conditions import Key
from   datetime                  import datetime, timedelta

import sys_log
import send_alerts

STATUS_TABLE        = 'ADynamoDBTable1'
CONTROL_TABLE       = 'ADynamoDBTable2'
CONTROL_ITEM_SELECT = '1'
SNS_TOPIC_ARN       = 'arn:aws:sns:us-east-1:123456789012:MyAlerts'

#global object provides system logging to DynamoDB tables
sl = sys_log.sys_log('freeze_guard','ADynDBTableForInfo',
                                    'ADynDBTableForIssues','','')


def _freeze_guard_is_on(setting):
  """
  The value of the "freeze_guard" attribute in the Control database
  can be "on", "off", or an epoch time.  "on" / "off" are self-
  explanatory.  An epoch time stamp is used to designate that
  the battery guard functionality should be considered off until the
  future point in time designated by the epoch time stamp. If an
  expired epoch time stamp is discovered, or an invalid one, then
  the "freeze_guard" attribute will be set to "on"
   
  Returns
    True   if the "freeze_guard" attribute is set to "on" or the 
           epoch time stamp represents a date time in the past or
           the epoch time stamp is not a valid epoch time stamp
    False  if the "freeze_guard" attribure is set to "off" or the
           epoch time stamp represents a date time in the future
  """
  if(setting.lower() == 'on'):
    result = True
  else:
    if(setting.lower() == 'off'):
      result = False
    else:
      try:
        time_stamp = abs(int(setting))
        if(time_stamp > int(datetime.now().timestamp())):
          result = False
        else:
          result = True
          time_stamp = 0
      except Exception as e:
        result = True    #revert to default setting of 'on'
        time_stamp = 0
        sl.log_message('1', 'ERROR', '', e)
          
      #expired/invalid time stamp; set to 'on'
      if(not time_stamp):
        try:
          dynamo_db_access = boto3.resource('dynamodb')
          table = dynamo_db_access.Table(CONTROL_TABLE)
          
          try:
            update_expression = 'set freeze_guard = :b'
            table.update_item(
              Key={'control_id': CONTROL_ITEM_SELECT},
                   UpdateExpression=update_expression,
                   ExpressionAttributeValues={':b' : 'on'}) 
          except Exception as e:
            sl.log_message('2', 'ERROR', '', e)
        except Exception as e:
          sl.log_message('3', 'ERROR', '', e)
  return(result)


def _send_alert(message):
  """
  Send an alert message to the SNS topic
  """
  if((type(message == str)) and (message != '')):
    params = [{'channel' : 'sns',
               'message' : message,
               'topic_arns':[SNS_TOPIC_ARN]}]
    alert = send_alerts.send_alerts(params)
    if(alert.issues):
      for issue in alert.issues:
        sl.log_message('13+', 'ERROR', issue, '') 
  else:
    sl.log_message('14', 'WARN', 'Avoided sending empty message to SNS.','') 
  

def freeze_guard(event, context):
  """
  If the freeze-guard feature is enabled, find any vineyard sensor
  station with a most recent temperature reading (but dont consider 
  anything that is not recent [e.g., over 12 hours old]) that is 
  below the temp threshold and with a status setting of 'on.'  Report
  all stations below temp threshold in a single message posted to SNS 
  topic.
  
  Note: 2038 epoch time roll over issue

  """
  alarm_message_prefix =  'The following station(s) has reported a low '
  alarm_message_prefix += 'temperature within the past '

  sl.reset()

  try:
    dynamo_db_access = boto3.resource('dynamodb')
    table = dynamo_db_access.Table(CONTROL_TABLE)
    try:
      response = table.query(
        KeyConditionExpression=Key('control_id').eq(CONTROL_ITEM_SELECT))
      if(response['Items']):
        if(_freeze_guard_is_on(response['Items'][0]['freeze_guard'])):
          FREEZE_THRESHHOLD = float(response['Items'][0]['freeze_thresh'])
          FREEZE_DATA_CURRENT = int(response['Items'][0]['freeze_cutoff'])
          alarm_message_prefix += str(FREEZE_DATA_CURRENT) + ' hours: '
          try:
            table = dynamo_db_access.Table(STATUS_TABLE)
            try:
              freezing_stations = []
              cut_off_ts = int((datetime.now() - 
                           timedelta(hours=FREEZE_DATA_CURRENT)).timestamp())
              response = table.scan()
              if(response['Items']):
                for item in response['Items']:
                  if((int(item['tstamp']) > cut_off_ts) and
                     (float(item['temperature']) <= FREEZE_THRESHHOLD) and
                     (item['tstatus'].lower() == 'online')):
                    freezing_stations.append(item['location'])
                if(freezing_stations):
                  message = alarm_message_prefix
                  for station in freezing_stations:
                    message += station + ', '
                  sl.log_message('4', 'ALARM', message[:-2], '')
                  _send_alert(message[:-2])
              else:
                sl.log_message('5', 'ERROR', 'Scan of DyanmoDB status table ' +
                               'returned 0 Items.', '')
            except Exception as e:
              sl.log_message('6', 'ERROR', '', e)   
          except Exception as e:
            sl.log_message('7', 'ERROR', '', e)
      else:
        sl.log_message('8', 'ERROR', 'Could not read control Item ' + 
                       CONTROL_ITEM_SELECT + ' from DynamoDB control ' +
                       'table.', '') 
    except Exception as e:
      sl.log_message('9', 'ERROR', '', e)           
  except Exception as e:
    sl.log_message('10', 'ERROR', '', e)
 
  if(sl.error_messages):
    sl.log_message('11', 'WARN', 'freeze_guard() job raised an alarm or did ' +
                   'not complete successfully.', '')
  else:
    sl.log_message('12', 'INFO', 'freeze_guard() job completed successfully.',
                   '')
  sl.save_messages_to_db()
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
detect any vinedyard sensor stations reporting a soil moisture sensor
readings that indicate irrigation is required.  This will be 
accomplished by checking the DynamoDB table ADynamoDBTable1 dedicated
to containing the most recent vineyard sensor station reading for each 
station.  If a station(s) reported soil moisture levels indicate that 
irrigation is required, a single consolidated message will be sent to 
an SNS topic, listing all of the offending stations.  And an ALARM 
system message will also be logged.

It is possible for one or more sensor stations to report soil moisture 
levels below the threshold and yet not send an ALERT or log an ALARM
message.  Firstly, the 'moisture_guard' may be turned off (setting 
resides in the DynamoDB ADynamoDBTable2).  The 'moisture_guard' 
setting of 'on' / 'off' applies to all 21 vineyard stations.  Also, 
individual stations may be taken 'offline' (setting resides in the 
DynamoDB table ADynamoDBTable1).  Even if a station is taken 
'offline', incoming raw sensor data for that station will continue to 
update the DynamoDBTableForSensorData table and the ADynamoDBTable1 
table. And 
finally, if the most recent moisture levels for a sensor exceed the
threshold this will only qualify for an alarm state if the most recent 
sensor data is not outside of the data shelf life limit set for 
Moisture Guard.

The Vinduino sensor boards deployed at the grape vines take soil 
moisture readings once a day.  The 24 sensor reports sent by the
board all contain the date obtained from this single read.  Soil
moisture level increase / decrease relatively slowly over time.
Considering these factors, it is recommended that this module be 
run once every 24 hours.

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
SNS_TOPIC_ARN       = 'arn:aws:sns:us-east-1:123456789012:My-Alerts'

#global object provides system logging to DynamoDB tables
sl = sys_log.sys_log('moisture_guard','DynamoDBTableForInfo',
                                      'DynamoDBTableForIssues','','')
 

def _moisture_guard_is_on(setting):
  """
  The value of the "moisture_guard" attribute in the Control database
  can be "on", "off", or an epoch time.  "on" / "off" are self-
  explanatory.  An epoch time stamp is used to designate that
  the battery guard functionality should be considered off until the
  future point in time designated by the epoch time stamp. If an
  expired epoch time stamp is discovered, or an invalid one, then
  the "moisture_guard" attribute will be set to "on"
   
  Returns
    True   if the "moisture_guard" attribute is set to "on" or the 
           epoch time stamp represents a date time in the past or
           the epoch time stamp is not a valid epoch time stamp
    False  if the "moisture_guard" attribure is set to "off" or the
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
            update_expression = 'set moisture_guard = :b'
            table.update_item(
              Key={'control_id': CONTROL_ITEM_SELECT},
                   UpdateExpression=update_expression,
                   ExpressionAttributeValues={':b' : 'on'}) 
          except Exception as e:
            sl.log_message('2', 'ERROR', '', e)
        except Exception as e:
          sl.log_message('3', 'ERROR', '', e)   
  return(result)
  
 
def _station_is_dry(station, cut_off_ts, station_threshold):
  """
  For stations that have reported soil moisture readings within the cut
  off number of hours and are currently 'online', determine if the
  station requires irrigation.  
  
  All stations have three soil moisture sensors buried below grade at 
  progressive depths.  SMS1 is at 12”, SMS2 is at 24” and SMS is at 48".  
  The age of grapevines at each station is documented.  For vines 2 
  years or younger average, average the reading of SMS1 and SMS2 and 
  compare against the threshold value documented for that station.  
  For vines older than 2 years average all three soil moisture sensor 
  readings and campare against the threshold value documented for the 
  station.
  """ 
  result = False
  TWO_YEARS_OLD = 63072000   #63,072,000 seconds in 2 years
  
  if((station['tstatus'].lower() == 'online') and 
     (int(station['tstamp']) > cut_off_ts)):   
    
    vine_age = int(datetime.now().timestamp()) - int(station['planted'])
    if(vine_age <= TWO_YEARS_OLD):  
      moisture = int((int(station['sms1']) + int(station['sms2'])) / 2)
    else:
      moisture = int((int(station['sms1']) + int(station['sms2']) + 
                  int(station['sms3'])) / 3)
    if(moisture > station_threshold):
      result = True

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
    

def moisture_guard(event, context):
  """
  If the moisture-guard feature is enabled, find any vineyard sensor
  station with a most recent soil moisture readings (but dont consider 
  anything that is not recent [e.g., over 24 hours old]) that is below 
  the soil moisture level threshold and with a status setting of 'on.'
  Report all stations below soil moisture threshold in a single message 
  posted to SNS topic.
  
  Note: 2038 epoch time roll over issue
  
  """
  alarm_message_prefix =  'The following station(s) has reported a low '
  alarm_message_prefix += 'soil moisture within the past '
  
  sl.reset()
  
  try:
    dynamo_db_access = boto3.resource('dynamodb')
    table = dynamo_db_access.Table(CONTROL_TABLE)
    try:
      response = table.query(
        KeyConditionExpression=Key('control_id').eq(CONTROL_ITEM_SELECT))
      if(response['Items']):
        if(_moisture_guard_is_on(response['Items'][0]['moisture_guard'])):
          MOIST_THRESHHOLDS = response['Items'][0]['moisture_thresh']
          MOIST_DATA_CURRENT = int(response['Items'][0]['moisture_cutoff'])
          alarm_message_prefix += str(MOIST_DATA_CURRENT) + ' days: '
          try:
            table = dynamo_db_access.Table(STATUS_TABLE)
            try:
              dry_stations = []
              cut_off_ts = int((datetime.now() - 
                            timedelta(days=MOIST_DATA_CURRENT)).timestamp())
              response = table.scan()
              if(response['Items']):
                for item in response['Items']:
                  station_threshold = int(MOIST_THRESHHOLDS[item['location']])
                  if(_station_is_dry(item, cut_off_ts, station_threshold)):
                    dry_stations.append(item['location'])
                if(dry_stations):
                  message = alarm_message_prefix
                  for station in dry_stations:
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
                       CONTROL_ITEM_SELECT + ' from DynamoDB control table.', 
                       '') 
    except Exception as e:
      sl.log_message('9', 'ERROR', '', e)             
  except Exception as e:
    sl.log_message('10', 'ERROR', '', e) 
     
  if(sl.error_messages):
    sl.log_message('11', 'WARN', 'moisture_guard() job raised an alarm or ' +
                   'did not complete successfully.', '')  
  else:
    sl.log_message('12', 'INFO', 'moisture_guard() job completed successfully.',
                   '')  
  sl.save_messages_to_db()
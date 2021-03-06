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
  
Overall, sensor data flows from vineyard sensor stations to 
www.thingspeak.com to the AWS backend. This module is responsible for
pulling data from www.thingspeak.com and kicking off the data's
ingestion into the AWS backend.

This module serves as an AWS Lambda function intended to be invoked (on
a scheduled, regular basis) by a CloudWatch Events Rule in order to 
perform two separte functions.  First it checks to see if any sensor
data from any sensor has been processed within the threshold amount
of time (i.e., call _recent_activity_check() function).  Second, this
modulde will retrieve all sensor data records, from a public Internet 
end point, that have arrived within MINS_BACWARD minutes in the past,
and ingest them into the AWS backend.  Updating IoT Core shadow
documents, via na MQTT queue, is the ingress point for this ingestion.

A custom system logging class was created to capture system logging
messages generated while this module executes.  At the conclusion of
this module's execution the system logging object writes all of the
system logging messages to DynamoDB tables.  Because this module
executes as an AWS Lambda function, and the Lambda retains containers
for a short period of time after the function exits (in hopes of 
reusing the container for a future call of the same function), it is
necessary to reset/clear the system logging object at the beginning
of this module's execution.

Vineyard sensor stations report once an hour.  Due to how quickly 
real world conditions can deteriorate (e.g., Freeze Guard), this 
Python module should be executed every 15 minutes.

  Usage: 
    A CloudWatch Events Rule invokes, on an regularly scheduled basis, 
    this AWS Lambda function by calling the function lambda_handler and
    passing to it the input parameters 'event' and 'context.'

  Dependencies:
    import boto3
    import urllib.request
    import json
    from   datetime                  import datetime, timedelta
    from   boto3.dynamodb.conditions import Key
    from   AWSIoTPythonSDK.MQTTLib   import AWSIoTMQTTShadowClient
    import sys_log
    import send_alerts
    
  Security Requirements beyond assigning the appropirate IAM role.
    This module needs to be able to push MQTT messages on a queue
    in order to update an IoT shadow document for a thing.  This
    is the way vineyard sensor data is ingested.  To write sensor
    date to a queue, one for each sensor location, three unique files
    are required but one of the can be shared across all sensors.
    The unique files are a private key (form a public / private key
    pair) and a digital certificate.  The shared file is a root
    certificate from the certificate authority that issued the
    digital certificate.  This root certificate can be shared 
    across all queues / sensors if the same certificate authority
    is used to generate all of the invidivual certificates.
"""
import boto3
import urllib.request
import json
from   datetime                  import datetime, timedelta
from   boto3.dynamodb.conditions import Key
from   AWSIoTPythonSDK.MQTTLib   import AWSIoTMQTTShadowClient

import sys_log
import send_alerts

STATUS_TABLE         = 'ADynamoDBTable1'
CONTROL_TABLE        = 'ADynamoDBTable2'
CONTROL_ITEM_SELECT  = '2'

SNS_TOPIC_ARN        = 'arn:aws:sns:us-east-1:123456789012:My-Alert'

COMMON_ENDPOINT      = 'https://api.thingspeak.com/channels/'
API_KEY              = '12345678901234567'   #production
CHANNEL_ID           = '1234567'            #production
MINS_BACKWARD        = '30'                 #production
                 
device_shadows = {} #explained in header comments of _get_device_shadow_client()
       
LOCATIONS    = ['A02N', 'A08M', 'A02S', 'B02N', 'B08M', 'B02S', 'C02N',
                'C08M', 'C02S', 'D02N', 'D08M', 'D02S', 'E02N', 'E08M', 
                'E02S', 'F02N', 'F08M', 'F02S', 'G02N', 'G08M', 'G02S']
                
MQTT_VALUES = {'host_name' : '12345678901234-ats.iot.us-east-1.amazonaws.com',
               'mqtt_port' : 8883}

SHADOW_SECURITY = {'A02N' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'A02N'},                             
                   'A02S' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'A02S'}, 
                   'A08M' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'A08M'},
                   'B02N' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'B02N'},
                   'B02S' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'B02S'},
                   'B08M' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'B08M'},
                   'C02N' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'C02N'},
                   'C02S' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'C02S'},
                   'C08M' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'C08M'},
                   'D02N' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'D02N'},
                   'D02S' : {'private_key' : 'a-key-private.pem.key'',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'D02S'},
                   'D08M' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'D08M'},
                   'E02N' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'E02N'},
                   'E02S' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'E02S'},
                   'E08M' : {'private_key' : 'a-key-private.pem.key'',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'E08M'},
                   'F02N' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'F02N'},
                   'F02S' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'F02S'},
                   'F08M' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'F08M'},
                   'G02N' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'G02N'},
                   'G02S' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'G02S'},
                   'G08M' : {'private_key' : 'a-key-private.pem.key',
                             'cert_file' : 'a-cert--certificate.pem.crt',
                             'root_ca' : 'Security/Amazon_root_CA_1.pem',
                             'handler' : 'G08M'}}
                             
#global object provides system logging to DynamoDB tables
sl = sys_log.sys_log('pull_data','DynTableForInfo',
                                 'DynTableForIssues','','')


def _recent_activity_check():
  """
  Determine if any sensor data has flowed from the vineyard through 
  www.thingspeak.com to AWS within the past NO_UPDATE_THRESHOLD  
  amount of time.  If not, this is a strong indication that vineyard 
  infrastructure is down or the ISP is down or that www.thingspeak.com
  is down. If detected, this condition constitues an alarm state.
  """
  try:
    dynamo_db_access = boto3.resource('dynamodb')
    table = dynamo_db_access.Table(CONTROL_TABLE)
    try:
      response = table.query(
        KeyConditionExpression=Key('control_id').eq(CONTROL_ITEM_SELECT))
      if(response['Items']):
        NO_UPDATE_THRESHOLD = int(response['Items'][0]['no_update_thresh'])
        dt = datetime.now() - timedelta(minutes=NO_UPDATE_THRESHOLD)                        
        cut_off_ts = int(dt.timestamp())                     
        try:
          dynamo_db_access = boto3.resource('dynamodb')
          table = dynamo_db_access.Table(STATUS_TABLE)
          try:
            response = table.scan()
            if(response['Items']):
              recent_activity = False
              for item in response['Items']:
                if(int(item['tstamp']) >= cut_off_ts):
                  recent_activity = True
                  break
              if(not recent_activity):
                message = (str(NO_UPDATE_THRESHOLD) + ' mins have elapsed ' +
                           'with no vineyard sensor station reporting ' +
                           'sensor data.')
                sl.log_message('1', 'ALARM', message, '')              
                _send_alert(message)                  
            else:
              sl.log_message('2', ERROR, 'Scan of DyanmoDB status table ' +
                             'returned 0 Items.', '')
          except Exception as e:
            sl.log_message('3', ERROR, '', e)            
        except Exception as e:
          sl.log_message('4', ERROR, '', e)   
      else:
        sl.log_message('5', ERROR, ' Could not read control Item ' + 
                       CONTROL_ITEM_SELECT + ' from DynamoDB control table.', 
                       '')
    except Exception as e:
      sl.log_message('6', ERROR, '', e)   
  except Exception as e:
    sl.log_message('7', ERROR, '', e)
      
  if(sl.error_messages):
    sl.log_message('8', 'WARN', '_recent_activity_check() function raised ' +
                   'an alarm or did not complete successfully.', '')
  else:
    sl.log_message('9', 'INFO', '_recent_activity_check() function completed' +
                   ' successfully.', '')


def _get_id_ts_of_last_record_processed():
  """
  The Internet API endpoint service provider, www.thingspeak.com,
  adds 'created_at' and 'entry_id' values to every incoming
  sensor data record that it receives from vineyard sensor stations.
  The values assigned, for both of these attributes, increase over time
  such that the last sensor record received by www.thingspeak.com will
  have the largest 'create_at' and 'entry_id' values across all 
  previoulsy received sensor records.
  
  This function will retrieve, from the control database, the 
  'entry_id' and the 'created_at' fields from the last sensor record 
  processed the last time this module was executed.  These values will 
  be the largest entry_id and created_at values for not only the last 
  execution of this module but for all prior executions of this module.
  
  Returns
    '0', '0'    if the id and/or time stamp cannot be retrieved
    str, str    id and timestamp of last sensor record processed the  
                last time this module was executed
  """
  last_id = '0'
  last_ts = '0'
  try:
    dynamo_db_access = boto3.resource('dynamodb')
    table = dynamo_db_access.Table(CONTROL_TABLE)
    try:
      response = table.query(
        KeyConditionExpression=Key('control_id').eq(CONTROL_ITEM_SELECT))
      if(response['Items']):
        last_id = response['Items'][0]['last_id_processed']
        last_ts = response['Items'][0]['last_ts_processed']
      else:
        sl.log_message('10', ERROR, 'Could not read control Item ' + 
                       CONTROL_ITEM_SELECT + ' from DynamoDB control table.', 
                       '')
    except Exception as e:
      sl.log_message('11', ERROR, '', e)
  except Exception as e:
    sl.log_message('12', ERROR, '', e)  
  return(last_id, last_ts)


def _set_id_ts_of_last_record_processed(last_record_id, last_record_ts):
  """
  The Internet API endpoint service provider, www.thingspeak.com,
  adds 'created_at' and 'entry_id' values to every incoming
  sensor data record that it receives from vineyard sensor stations.
  The values assigned, for both of these attributes, increase over time
  such that the last sensor record received by www.thingspeak.com will
  have the largest 'create_at' and 'entry_id' values across all 
  previoulsy received sensor records.
  
  This function stores the 'entry_id' and 'created_at' of the last 
  sensor record that was processed by the current execution of this 
  module (i.e., the backend process pull_data)
  """
  try:
    dynamo_db_access = boto3.resource('dynamodb')
    table = dynamo_db_access.Table(CONTROL_TABLE)      
    try:
      update_expression = 'set last_id_processed = :li, last_ts_processed = :lt'
      table.update_item(Key={'control_id': CONTROL_ITEM_SELECT},
                        UpdateExpression=update_expression,
                        ExpressionAttributeValues={':li' : last_record_id,
                                                   ':lt' : last_record_ts}) 
    except Exception as e:
      sl.log_message('13', ERROR, '', e)    
  except Exception as e:
    sl.log_message('14', ERROR, '', e)
   
  
def _get_device_shadow_client(location):
  """
  Each vineyard sensor station is set up in IoT Core as a 'Thing' with
  its own 'shadow document.'  In order to push data into AWS, for a 
  sensor, you update its shadow document.  To do this you use a 'shadow
  client' to place a message, containing the incoming sensor data, on 
  the message queue assigned to the sensor/thing/shadow document.  Each
  IoT shadow document is assigned its own shadow client.  Shadow 
  clients are created by this module, on demand, and exist for the 
  duration of this module's execution.  Shadow clients will be reused 
  (for the duration of this module's execution) in cases where multiple 
  data sensor records are processed for the same sensor station.  Reuse 
  of shadow clients is possible because of the 'True' flag in the 
  createShadowHandlerWithName() call.
  
  Returns
    None           if a device shadow could not be retrieved/created
    device_shadow  if a device shadow could be retrieved/created
  """
  global device_shadows
  a_device_shadow = None
  if(location in device_shadows):
    a_device_shadow = device_shadows[location]
  else:
    a_client = AWSIoTMQTTShadowClient(location)
    a_client.configureEndpoint(MQTT_VALUES['host_name'], 
                               MQTT_VALUES['mqtt_port'])
    a_client.configureCredentials(SHADOW_SECURITY[location]['root_ca'],
                                  SHADOW_SECURITY[location]['private_key'],
                                  SHADOW_SECURITY[location]['cert_file'])
    a_client.configureConnectDisconnectTimeout(10)
    a_client.configureMQTTOperationTimeout(5)
    
    if(not a_client.connect()):
      sl.log_message('15', ERROR, 'Could not connect to shadow document' +
                     ' for location: ' + location, '')
    else:
      a_device_shadow = a_client.createShadowHandlerWithName(
                          SHADOW_SECURITY[location]['handler'], True)
      device_shadows[location] = a_device_shadow
  return(a_device_shadow)
 
 
def _update_shadow_document(location, update):
  """
  Each vineyard sensor station is set up in IoT Core as a 'thing' with
  its own 'shadow document.'  In order to push data into AWS, for a 
  sensor, you update its shadow document.  AWS service-to-service 
  integrations propogate new sensor data DynamoDB tables and 
  to CloudWatch custom metrics.
  
  The shadowUpdate() call in this function is asynchronous.  Processing
  will continue immediately without waiting on the results of this call
  to the AWS backend.  A process separate from this module handles the 
  shadow document update and it will call a function contained within 
  this module with the end results of the call to shadowUpdate() once 
  the results are available (i.e., will be either 'accepted' or 
  'rejected.').  The funciton that the process will call when it has
  the results is _my_shadow_update_callback().
  
  Returns
    True    If no errors occured before calling shadowUpdate().  
    
    False   If an error occured in the steps leading up to calling the
            shadowUpdate() function.  
  """
  result = True
 
  if(SHADOW_SECURITY[location]['handler'] != 'shadow-not-configured-yet'):
    a_device_shadow = _get_device_shadow_client(location)
    if(a_device_shadow):
      update_string = '{"state":{"reported":'
      update_string += json.dumps(update)
      update_string += '}}'
       
      try:
        #asynch call; IoT service will provide results by calling
        #the _my_shadow_update_callback() and pass in results
        a_device_shadow.shadowUpdate(update_string, 
                                     _my_shadow_update_callback, 5)
      except Exception as e:
        sl.log_message('35', ERROR, 'Exception thrown updating shadow: ' + 
                     location + '. ', e)
    else:
      result = False
      sl.log_message('16', ERROR, 'Could not obtain a device shadow client ' +
                     'for location: ' + location, '')
  else:
    result = False
    sl.log_message('17', ERROR, 'Security credentials havent yet been ' +
                   'provisioned for location: ' + location, '')
  return(result)
   
   
def _my_shadow_update_callback(payload, response_status, token):
  """
  A process external to this module will call this function once final
  results are available for the asynchronous call shadowUpdate() that
  was made by the _update_shadow_document() function, contained in this
  module, in order to update the AWS backend.  

  places data on the MQTT queue and monitors the progress
  of the IoT Core shadow document update.  This external process 
  eventually calls this function once it has the results of the shadow 
  document update.
  Args:
    payload:          the data that was supplied in the shadow document 
                      update plus some extra information
    response_status:  'accepted' or 'rejected'
    token:            client token; the token is also contained in 
                      payload
  """
  if(response_status != 'accepted'): 
    sl.log_message('18', ERROR, 'The IoT Core service returned a value on ' +
                   'a shadow document update indicating failure.', '')  


def _process_sensor_record(record):
  """ 
  Convert an individual raw sensor data record into a format consistant
  with the DynamoDB table VinStationsData.  Each vineyard sensor 
  station has its own IoT Core shadow document and its own MQTT queue.
  The UTC date/time value supplied by Internet Endppoint is translated 
  to US Central Time Zone timestamp.
  
  NOTE: the return codes only consider processing up to the
        asynchronous call to the AWS backend.  Errors that occur
        within the processing of that asynchronous call are not 
        reflected by the return code.
    
  Example date/time value supplied by Internet Endpoint API:
    '2020-03-26T19:25:36Z'
    
  Returns
    True    raw sensor data record processed correctly
    False   raw sensor data record could not be processed correctly
  """
  result = False
  update = {}
  try:
    location = record['field1'].upper()
    if(location in LOCATIONS):      
      utc = datetime.strptime(record['created_at'],'%Y-%m-%dT%H:%M:%S%z')
      ts = int(utc.timestamp())
      cst = utc - timedelta(hours=6) #Convert UTC to US Central time zone
      year = str(cst.year)
      month = str(cst.month).zfill(2)
      day = str(cst.day).zfill(2)
            
      update['date']        = year + '-' + month
      update['day_ts_loc']  = day + '+' + str(ts) + '+' + location
      update['battery']     = record['field3']
      update['temp']        = record['field2']
      update['sms1']        = record['field4']
      update['sms2']        = record['field5']
      update['sms3']        = record['field6']
      update['id']          = record['entry_id']
      
      result = _update_shadow_document(location, update)
    else:
      sl.log_message('19', ERROR, 'New raw sensor data record received with ' +
                     'invalid station location of: ' + location, '')    
  except Exception as e:
    sl.log_message('31', ERROR, 'Exception thrown updating shadow for: ' + 
                   location + '. ', e)  
  return(result)


def _process_sensor_data(sensor_data, last_id_processed, last_ts_processed):
  """
  'sensor_data' contains all sensor data records retrieved from the
  Internet endpoint - zero or more individual sensor data records.
  All 21 vineyard sensor stations report their data hourly to a 
  single, shared channel. Each individual sensor data records received
  by from vineyard sensor stations has a UTC time stamp applied to it
  by the Internet endpoint. The Internet endoint also applies a unique
  entry_id to each incoming sensor data record.  As all sensors share 
  the same channel, entry_id values across all sensor data records for
  all sensor stations are unique. This entry_id is an integer value
  that starts at 1 and is auto incremented by +1 each time it is 
  assigned to a sensor record. So the last timestamp and the last 
  entry_id assigned will be the largest of each kind assigned by the 
  Internet endpoint so far.
  
  Depending on how often the pull_data process is invoked and the
  setting of MINS_BACKWARD, it is quite possible that pull_data 
  will obtain a given raw sensor data reocord multiple times from
  the Internet endpoint.  The pull_data process avoids processing 
  the same sensor data record multiple times by storing off the 
  timestamp and entry_id of the last data sensor record that is
  processes each time pull_data is executed.
  
  Multiple data sensor records can be assigend the same time 
  stamp because the timestamp assigned by the Internet endpoint is
  in second granularity (i.e., not millisecond) and there is no
  coordination across the 21 vineyard sensors as to when they report
  sensor data.  To handle this potential, but unlikely, situaiton 
  the entry_id values of sensor data records are used.
  
  This function relies on a relative ordering of the sensor data
  records: as you move towards the end of the list of records the 
  time stamps and entry ids grow larger; ensure ts_just_processed 
  is largest time stamp value processed in this invocation of 
  pull_data

  Returns
    True    if the entire set of incoming raw sensor record processed
            correctly
    False   if an error occured when processing the set of incoming
            raw sensor records
  """
  result = True
  processed_ids = []
  ts_just_processed = 0
  
  try:
    json_object = json.loads(sensor_data)
    sensor_records = json_object['feeds']
    
    for record in sensor_records:
      try:
        utc = datetime.strptime(record['created_at'],'%Y-%m-%dT%H:%M:%S%z')
        record_ts = int(utc.timestamp())        
        record_id = int(record['entry_id'])
      
        if(record_ts > last_ts_processed):
          if(not _process_sensor_record(record)):
            result = False
            break
          else:
            ts_just_processed = record_ts
            processed_ids.append(record_id)
   
        elif(record_ts == last_ts_processed):  
          if(record_id > last_id_processed):       #unlikely but possible
            if(not _process_sensor_record(record)):
              result = False
              break
            else:
              ts_just_processed = record_ts
              processed_ids.append(record_id)                         
      except Exception as e:
        result = False
        sl.log_message('33', ERROR, '', e)
    if(processed_ids):
      processed_ids.sort()
      sl.log_message('21', 'INFO', 'Ingested ' + str(len(processed_ids)) + 
                     ' sensor data records with IDs ranging from ' + 
                     str(processed_ids[0]) + ' to ' + str(processed_ids[-1]), 
                     '')
      #entry_id is an attributed introduced by www.thingspeak.com
      #was the entry_id of the first sensor record the value expected?
      if((last_id_processed + 1) != processed_ids[0]):
        sl.log_message('34', 'WARN', 'An entry_id gap found between ' +
                       'processed id: ' + str(last_id_processed) + 
                       ' and processed id: ' + str(processed_ids[0]) + 
                       ' during sensor record processing.', '')
                     
      #any entry_id gaps within this set of sensor records processed?
      id_gaps = sorted(set(range(processed_ids[0], 
                           processed_ids[-1])) - set(processed_ids))
      if(id_gaps):
        sl.log_message('22', 'WARN', 'The following entry_id(s) found ' +
                       'missing during sensor record processing: ' + 
                       str(id_gaps), '')
      _set_id_ts_of_last_record_processed(str(processed_ids[-1]), 
                                          str(ts_just_processed))
  except Exception as e:
    result = False
    sl.log_message('23', ERROR, '', e)          
  return(result) 
  
  
def _extract_data_from_API():
  """
  MINS_BACKWARD specifies how far back in time to go when retrieving
  sensor data from the Internet endpoint.  For example, specifying 30
  would retrieve all sensor records that the Internet endpoint has 
  received from the vineyard sensor stations within the last 30 mins.
  
  Returns
    ''        if no raw sensor data could be retrieved
    str       raw sensor data retrieved
  """
  TARGET_URL = (COMMON_ENDPOINT + CHANNEL_ID + '/feeds.json?api_key=' + API_KEY
                + '&minutes=' + MINS_BACKWARD)
  raw_sensor_data_str = ''

  try:
    with urllib.request.urlopen(TARGET_URL) as response:
      raw_sensor_data_bytes = response.read()
  except Exception as e:
    raw_sensor_data_str = ''
    sl.log_message('24', ERROR, '', e)  
  else:
    try:
      raw_sensor_data_str = raw_sensor_data_bytes.decode()
    except:
      raw_sensor_data_str = ''
      sl.log_message('32', ERROR, '', e) 
 
  return(raw_sensor_data_str)


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
        sl.log_message('28+', 'ERROR', issue, '') 
  else:
    sl.log_message('29', 'WARN', 'Avoided sending empty message to SNS.','')


def pull_data(event, context):
  """
  Retrieve record id of the last sensor reocrd (will be largest id
  value) processed by the previous execution of this module (i.e., 
  the pull_data process).  Obtain all newly arrived sensor data 
  from the Internet API endpoint and process them skipping any 
  sensor records that have already been processed.
  """
  #Becuase AWS Lambda service leaves containers around for a short while,
  #need to clear any potential residual values from global vars
  sl.reset()
  global device_shadows
  device_shadows = {}
  
  _recent_activity_check()
  last_id_processed, last_ts_processed = _get_id_ts_of_last_record_processed()
  
  if((last_id_processed != '0') and (last_ts_processed != '0')):
    incoming_data = _extract_data_from_API()
    if(incoming_data):
      _process_sensor_data(incoming_data, int(last_id_processed), 
                           int(last_ts_processed))
    #else:  logging to capture this error is in _extract_data_from_API()
  else:
    sl.log_message('25', ERROR, 'Couldnt retrieve last_id_processed or ' +
                              'last_ts_processed from DynamoDB table.', '')
    
  if(sl.error_messages):
    sl.log_message('26', 'WARN', 
      'pull_data() job and/or _recent_activity_check() function did not ' +
      'complete without ERROR or raising ALARM.', '')
  else:
    sl.log_message('27', 'INFO', 'pull_data() job completed sucessfully.', '') 
  sl.save_messages_to_db()
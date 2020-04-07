""" 
This module serves as an AWS Lambda function intended to be invoked (on a
scheduled, regular basis) by a CloudWatch Events Rule in order to collect 
sensor data from a Vineyard Sensor Station (e.g., custom circuit board 
commercially available from Vinduino [www.vinduino.com]).  This is achieved
by accessing an Internet endpoint provided by ThingSpeak (www.thingspeak.com).

  Usage: 
    A CloudWatch Events Rule invokes, on an regularly scheduled basis, this
    AWS Lambda function by calling the function lambda_handler and passing
    to it the input parameters 'event' and 'context.'

  Dependencies:
    import json
    import urllib.request
    from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
    
  Security Requirements to Enable Pushing MQTT Messages:
    a digital certificate
    AWS IAM access key (i.e., private key)
    AWS root certificate
"""
import urllib.request
import json
import time
from calendar import timegm
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient

API_KEY       = 'XXXXXXXXXXXXXXXX'             #unique per vineyard station
CHANNEL_ID    = 'YYYYYYY'                      #unique per vineyard station
SHADOW_CLIENT = 'VineyardStation1'             #arbitrary value

# REST API public endpoint used to update the Vineyard station's IoT shadow
HOST_NAME = 'xxxxxxxxxxx-ats.iot.us-east-1.amazonaws.com' #unique per AWS acct

ROOT_CA = 'Security/Amazon_root_CA_1.pem'      #public, shared across stations
PRIVATE_KEY = 'Security/xxxxxxxxxx-private.pem.key'    #unique per V station
CERT_FILE = 'Security/xxxxxxxxxx-certificate.pem.crt'  #unique per V station
SHADOW_HANDLER = 'VineyardStation1'                    #unique per V station
MQTT_PORT = 8883

# set in _my_shadow_update_callback() as result of updating shadow document
return_value = {}


def lambda_handler(event, context):
  """
  Create an AWS IoT shadow client for a specific remote sensor station.
  Collect sensor data from the station via an Internet endpoint.  Update the
  shadow client with the sensor station data values.

  Args (supplied by AWS Lambda service)
    event: information about who/what invoked the Lambda function
    context: information about the Lambda function's runtime environment
  
  Returns: 
    { 'statusCode': <int value>,
      'body': <status string> }  
  """

  shadow_client = AWSIoTMQTTShadowClient(SHADOW_CLIENT)
  shadow_client.configureEndpoint(HOST_NAME, MQTT_PORT)
  shadow_client.configureCredentials(ROOT_CA, PRIVATE_KEY, CERT_FILE)
  shadow_client.configureConnectDisconnectTimeout(10)
  shadow_client.configureMQTTOperationTimeout(5)

  if(shadow_client.connect()):
    #Create access point for AWS IoT shadow document
    device_shadow = shadow_client.createShadowHandlerWithName(SHADOW_HANDLER, 
                                                              True)
    sensor_data = _extract_sensor_data()
    
    if(sensor_data):
      # specific format required; ' "state" : {"reported" : {....... '
      update_string = '{"state":{"reported":'
      update_string += json.dumps(sensor_data)
      update_string += '}}'
      
      # result of updating shadow document; set in _my_shadow_update_callback()
      device_shadow.shadowUpdate(update_string, _my_shadow_update_callback, 5)
    else:
      return_value['statusCode'] = 400
      return_value['body'] = 'Error connecting to sensor or with sensor data.'
  else:
    return_value['statusCode'] = 500
    return_value['body'] = 'Could not connect to IoT shadow.'
  return (return_value)

 

def _my_shadow_update_callback(payload, response_status, token):
  """
  This function is not directly called by anything in this module.  Instead,
  it is called by AWS IoT when an IoT shadow document is updated.
    
  Global Variable: 
    return_value: a global variable set by this function
  
  Args:
    All arguments are set by AWS IoT before this function is called
    
  Returns:
    nothing
  """
  if(not response_status):
    return_value['statusCode'] = 500
    return_value['body'] = 'Shadow document update rejected.'
  else:
    return_value['statusCode'] = 200
    return_value['body'] = 'OK. Shadow document update accepted.'
    

def _pull_data_from_endpoint(channel_id):
  """
  Args
    channel_id : unique id (string) per Vineyard Sensor Station
  
  Returns
    RAW_HTML_STRING: string containing all sensor data from  the polled station
  """
  COMMON_ENDPOINT = 'https://api.thingspeak.com/channels/'
  TARGET_URL = (COMMON_ENDPOINT + channel_id + '/feeds.json?api_key=' + API_KEY
                + '&results=1')
  RAW_HTML_STRING = ''

  if(channel_id):
    try:
      with urllib.request.urlopen(TARGET_URL) as response:
        RAW_HTML_BYTES = response.read()
    except Exception as e:
      RAW_HTML_STRING = ''
      #logging.error(f'Error accessing ThingSpeak endpoint. Exception thrown: {e}')
    else:
      RAW_HTML_STRING = RAW_HTML_BYTES.decode()
  
  return (RAW_HTML_STRING)


def _extract_sensor_data():
  """
  Return
    reported_data: JSON object containing all sensor data values

  """
  reported_data = {}
  RAW_HTML_STRING = _pull_data_from_endpoint(CHANNEL_ID)
  if(RAW_HTML_STRING):
    sensor_data = json.loads(RAW_HTML_STRING) 
    
    # www.thingspeak.com supplies date-time in human readable string.
    # convert this to the database field format (i.e., epoch time digits)    
    utc_time = time.strptime(sensor_data['feeds'][0]['created_at'], 
                             "%Y-%m-%dT%H:%M:%SZ")
    reported_data['time']     = str(timegm(utc_time))
    reported_data['location'] = sensor_data['feeds'][0]['field1']
    reported_data['temp']     = sensor_data['feeds'][0]['field2']
    reported_data['battery']  = sensor_data['feeds'][0]['field3']
    reported_data['sms1']     = sensor_data['feeds'][0]['field4']
    reported_data['sms2']     = sensor_data['feeds'][0]['field5']
    reported_data['sms3']     = sensor_data['feeds'][0]['field6']
  
  #else:
  #  logging.error('Could not retrieve sensor data from WeatherFlow endpoint.')
  return (reported_data)
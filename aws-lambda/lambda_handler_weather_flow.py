""" 
This module serves as an AWS Lambda function intended to be invoked (on a
scheduled, regular basis) by a CloudWatch Events Rule in order to collect 
sensor data from a Weather Station (e.g., WeatherFlow's Smart Home Weather 
Station).  For the Smart Home Weather Station this is achieved by accessing
an Internet endpoint that is provided by WeatherFlow.

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
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient

WF_SHARED_KEY = '?api_key=20c70eae-e62f-4d3b-b3a4-8586e90f3ac8'
#LONESOME_VINE_API_KEY = 'private API key guarantees avail / throughput'

API_KEY = WF_SHARED_KEY
WF_STATION_ID = '15420'                        #unique per WF station
SHADOW_CLIENT = "WFStationShadow"              #arbitrary value

# REST API public endpoint used to update the WeatherStation's IoT shadow
HOST_NAME = 'xxxxxxxxxxxx-ats.iot.us-east-1.amazonaws.com' #unique per AWS acct

ROOT_CA = 'Security/Amazon_root_CA_1.pem'      #public, shared across stations
PRIVATE_KEY = 'Security/xxxxxxxxxx-private.pem.key'    #unique per WF station
CERT_FILE = 'Security/xxxxxxxxxx-certificate.pem.crt'  #unique per WF station
SHADOW_HANDLER = 'WeatherStation'                      #unique per WF station
MQTT_PORT = 8883

# set in _my_shadow_update_callback() as result of updating shadow document
return_value = {}

def lambda_handler(event, context):
  """
  Create an AWS IoT shadow client for a specific remote sensor station.
  Collect sensor data from the station via an Internet endpoint.  Update the
  shadow client with the desired subset of available sensor station data.

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
    

def _pull_data_from_endpoint(station_id):
  """
  NOTE: all WeatherFlow customers have the ability to use the shared 
  API key (i.e., SHARED_KEY) for testing or for low-volume production.
  Securing a private API key guarantees availability and throughput
  
  Args
    station_id: unique id (string) of the WeatherFlow station to poll
  
  Returns
    RAW_HTML_STRING: string containing all sensor data from  the polled station
  """
  COMMON_ENDPOINT = 'https://swd.weatherflow.com/swd/rest/observations/station/'
  TARGET_URL = COMMON_ENDPOINT + station_id + API_KEY
  RAW_HTML_STRING = ''

  if(station_id):
    try:
      with urllib.request.urlopen(TARGET_URL) as response:
        RAW_HTML_BYTES = response.read()
    except Exception as e:
      RAW_HTML_STRING = ''
      #logging.error(f'Error accessing WeahterFlow endpoint. Exception thrown: {e}')
    else:
      RAW_HTML_STRING = RAW_HTML_BYTES.decode()
  
  return (RAW_HTML_STRING)


def _extract_sensor_data():
  """
  Return
    reported_data: JSON object containing desired subset of all available 
    WeatherFlow sensor data readings

  """
  reported_data = {}
  RAW_HTML_STRING = _pull_data_from_endpoint(WF_STATION_ID)
  if(RAW_HTML_STRING):
    sensor_data = json.loads(RAW_HTML_STRING)
    sensor_readings = sensor_data['obs'][0]

    # have to convert C->F; despite configuring for F, WF still returns C
    air_temp = (sensor_readings['air_temperature'] * 9.00/5.0) + 32
    
    reported_data['station_id'] = str(sensor_data['station_id'])
    reported_data['time'] = str(sensor_readings['timestamp'])
    reported_data['temp'] = str(round(air_temp, 2))
    reported_data['pressure'] = str(sensor_readings['barometric_pressure'])
    reported_data['humidity'] = str(sensor_readings['relative_humidity'])
    reported_data['rain_accum'] = str(sensor_readings['precip_accum_local_day'])
    reported_data['rain_mins'] = str(sensor_readings['precip_minutes_local_day'])
    reported_data['wind_avg'] = str(sensor_readings['wind_avg'])
    reported_data['wind_dir'] = str(sensor_readings['wind_direction'])
    reported_data['wind_gust'] = str(sensor_readings['wind_gust'])
    reported_data['wind_lull'] = str(sensor_readings['wind_lull'])
    reported_data['wind_chill'] = str(sensor_readings['wind_chill'])
    reported_data['radiation'] = str(sensor_readings['solar_radiation'])
    reported_data['uv'] = str(sensor_readings['uv'])
    reported_data['brightness'] = str(sensor_readings['brightness'])
    reported_data['heat'] = str(sensor_readings['heat_index'])  

  #else:
  #  logging.error('Could not retrieve sensor data from WeatherFlow endpoint.')
  return (reported_data)

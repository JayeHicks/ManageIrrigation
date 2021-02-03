""" 
Jaye Hicks 2020

Obligatory legal disclaimer:
  You are free to use this source code (this file and all other files 
  referenced in this file) "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER 
  EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. 
  THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THIS SOURCE CODE
  IS WITH YOU.  SHOULD THE SOURCE CODE PROVE DEFECTIVE, YOU ASSUME THE
  COST OF ALL NECESSARY SERVICING, REPAIR OR CORRECTION. See the GNU 
  GENERAL PUBLIC LICENSE Version 3, 29 June 2007 for more details.
  
ver: 2020-08-12
This module serves as manual data import utility for the
VinStationsData table.  As it will be invoked ad hoc by an end user
through an interactive Python command line session, print statements
will be used to provide feedback to the end user (i.e., errors, 
success, how many sensor records uploaded).  Also, the Lonesome Vine
system logging for vineyard sensor data (i.e., DynamoDB tables 
table_name_1 and table_name_2) will not be used.
Instead basic logging provided by the standard library will be used to
create a log file for the execution of this module.

The module will process all sensor station data records that it finds
in the specified input file.

NOTE: the anticipated use of this utility is 'low volume.'  If a
      'high volume' of sensor records is ingested, use a batch_writer
      (see delete_data.py)
      
      incoming data records are assumed correct (e.g., vineyard sensor
      location code).

Args:
  input_file (str):  Required.  Relative path to file containing the
                     JSON objects representing sensor data records
  
  id (str):          Optional. AWS access key id to use for auth
  secret(str):       Optional. AWS secret access key to use for auth
  region(str):       Optional. Regional endpoint to use for API calls
  
Returns:
  (str): list of S3 bucket names or 'no buckets were located'
  
Usage:
  'python direct_load.py input_file.json'
  'python direct_load.py input_file.json -i 12345678901234567890
   -s 1234567890123456789012345678901234567890 -r us-east-1'
 
  '>>> import direct_load'
  '>>> direct_load('input_file.json')
  '>>> direct_load.direct_load('input_file.json', 
         id='12345678901234567890', 
         secret= '1234567890123456789012345678901234567890', 
         region='us-east-1')' 

Dependencies:
  argparse
  logging
  json
  re
  boto3
"""
import argparse
import logging
import json
import re
import boto3
from datetime import datetime, timedelta

__all__ = ['direct_load']

DATA_TABLE        = 'table_name_1'
logging.basicConfig(filename='direct_load.log',level=logging.INFO)


def _validate_region(input_region):
  """ If arg not AWS standard region retun '' otherwise return 
  the arg passed in, in lower case.  
  """
  region = ''
  if(isinstance(input_region, str)):
    region = input_region.lower()
    if(not region in boto3.Session().get_available_regions('dynamodb')):
      region = ''
  return(region)


def _create_custom_session(id, key, endpoint):
  """ Attempt to create a custom boto3 session to overide use of the 
  default profile in the ".aws/credentials" file.  
  """
  custom_session = None
  if(id.isalnum and (len(id)  == 20)):
    if(re.match(r'^[A-Za-z0-9/+]*$', key) and (len(key) == 40)):
      region = _validate_region(endpoint)
      if(not region):
        logging.warning(f'"{endpoint}" is an invalid region specification.')
        print(f'"{endpoint}" is an invalid region specification.')
        region = 'us-east-1'
      try:
        custom_session = boto3.Session(aws_access_key_id= id, 
                                       aws_secret_access_key= key,
                                       region_name= region) 
      except Exception as e:
        custom_session = None
        logging.error(f'Boto3 exception thrown: {e}')
        print(f'Boto3 exception thrown: {e}')
  return(custom_session)


def direct_load(input_file, id='', secret='', region=''):
  """
  Open the specified input file and ingest all of the valid JSON
  objects representing individual sensor station data records.
  The JSON objects are specific to www.thingspeak.com channel data.
  
  NOTE: the anticipated use of this utility is 'low volume.'  If
        a 'high volume' of sensor records is ingested, use a 
        batch_writer (see delete_data.py)
  """
  records_read = 0
  records_processed = 0
  final_result = 'Could not ingest vineyard sensor records.'
  custom_session = None
  
  if(isinstance(input_file, str) and input_file):
    try:
      with open(input_file, 'r') as data_file:
        data = data_file.read()
      objects = json.loads(data)
      
      #override default AWS credentials?
      if((isinstance(id, str) and isinstance(secret, str)) and 
         (id and secret)):
        custom_session = _create_custom_session(id, secret, region)
      if(custom_session):
        dynamodb_access = custom_session.resource('dynamodb')
        print('using custom session')
      else:
        dynamodb_access = boto3.resource('dynamodb')
        print('using default session')
            
      records_read = len(objects)
      if(records_read):
        table = dynamodb_access.Table(DATA_TABLE)   
        for record in objects:
          utc = datetime.strptime(record['created_at'],'%Y-%m-%dT%H:%M:%S%z')
          ts = int(utc.timestamp())
          cst = utc - timedelta(hours=6) #Convert UTC to US Central time zone
          year = str(cst.year)
          month = str(cst.month).zfill(2)
          day = str(cst.day).zfill(2)
          location = record['field1'].upper()
           
          table.put_item(Item={'date' : year + '-' + month,
                               'day_ts_loc' : day + '+' + str(ts) + '+' + location,
                               'temp' : record['field2'],
                               'battery' : record['field3'],
                               'sms1' : record['field4'],
                               'sms2' : record['field5'],
                               'sms3' : record['field6'],
                               'id' : record['entry_id']})
          records_processed += 1
    except Exception as e:
      logging.error(f'Exception thrown: {e}')
      print(f'Exception thrown: {e}') 
  else:
    logging.error(f'Invalid input file: "{input_file}" specified.') 
    print(f'Invalid input file: "{input_file}" specified.')
    input_file = ''
  if(input_file):  
    final_result = f'Read {records_read} records, ingested '
    final_result += f'{records_processed} records.'
  return(final_result)    
 
 
if __name__ == '__main__':    
  parser = argparse.ArgumentParser(
    description = 'directly load sensor data into table')
  parser.add_argument('input_file', type=str, 
                      help='specify JSON input file with data records')                      
  parser.add_argument('-i', '--id', type=str,
                      help='specify access key id to authorize access')
  parser.add_argument('-s', '--secret', type=str,
                      help='specify secret access key to authorize access')
  parser.add_argument('-r', '--region', type=str,
                      help='specify which AWS regional endpoint to use')
  args = parser.parse_args() 
  
  convert_None_to_empty = lambda arg: '' if arg is None else arg 
  
  print(direct_load(args.input_file, convert_None_to_empty(args.id), 
                                     convert_None_to_empty(args.secret), 
                                     convert_None_to_empty(args.region)))
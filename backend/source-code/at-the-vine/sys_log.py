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
  
This module provides a highly targeted, specialized system logging
facility for serverless applications hosted in AWS.  You migh find
it preferrable to Python's logging module (i.e., standard library)
if you are looking for a smaller footprint and / or direct message
injection into DynamoDB tables.

It was designed to provide single use system logging for an Lambda
function serving as a RESTful API or a component of a stateless
applicaiton.  It has not been tested out as a system logging 
solution for a long running application, but the sys_log.reset()
method is offered, as a starting point, if you want to blaze that
trail.

sys_log categorizes messages into multiple levels of condition 
severity. The default levels of INFO, WARN, ALARM, and ERROR can be 
easily reworked by modifing source code but two things should be kept 
in mind. First, the different message categories flow from informational
(i.e., INFORM) to somewhat concerning (i.e., WARN) to concering
(i.e., ALARM) to seriously concerning (i.e., ERROR).  Second, in most
applications, system log messaging occurs much more frequently on the 
informational message side of this spectrum.  Owing to this second 
observation, an automated data lifecycle management strategy is 
provided by sys_log to manage informational messages whereas managing /
deleting concerning messages remains a manual process.

The text for a given system log message will be passed into the
log_message() call via parameters.  These include a fixed text
string or a Python Exception object or both.

Usage:
  0) Set up two DynamoDB tables
     create tables (one for info, one for errors) each with a primary
       key named 'stamp_mod' of type string
     after data has flowed into info messages table, enable TTL using the
       attribute 'expiry'
  1) create a system logging object
  2) log messages.  The system logging object keeps a local data
     structure to capture any run time issues that it encounters
  3) flush write all log messages to DynamoDB tables

Be aware: 
  1) As sys_log was designed to support serverless applications running in
     AWS, diagnostic print statements are in place as they will show in 
     CloudWatch, given correct configuration

  2) sys_log was not designed to support high-volume system logging.  Note
     the 1 second delay introduced in sys_log.log_message()

  3) After creating a sys_log object, sys_log.init_issues should be empty.  
     If it is not then system logging will most likely not function 
     properly

Dependencies:
  time
  boto3
  from boto3.dynamodb.conditions import Key
  from datetime import datetime, timedelta
"""
class sys_log():
  """
  Provides specialized system logging for stateless application
  components, storing log messages in DynamoDB tables.  Requires
  DynamoDB tables previously created.
  
  if(sys_log.init_issues): 
    #some issue arose when creating the sys_log object
    
  if(sys_log.run_issues):
    #some issue arose during the sys_log object's last execution
  
  Usage:
    import sys_log
    sl = sys_log.sys_log('my_module','info_table','errors_table',
                         -6, 155520000)
    if(not sl.init_issues):
      #the system logging object is ok
    try:
      x = 1 / 0
    except Exception as e:
      sl.log_message('1','error','',e)
    sl.log_message('3','warn','noticed a possible issue','')
    if(not sl.run_issues):
      #the last system logging message processed ok
    if(sl.success = sl.save_messages_to_db()):
      #all system logging messages successfully written to Dynamo tables
  """
  import time
  import boto3
  from   boto3.dynamodb.conditions import Key
  from   datetime                  import datetime, timedelta
 
 
  class message_core():
    """
    This inner/internal class serves to validate parameters sent to
    sys_log.log_message() and then isolate/build the basic message 
    elements that will be used when logging the complete system log
    message to a DynamoDB table.
    
    if(message_core.issues): 
      #some issue arose when creating the message_core object
    """
    
 
    def __init__(self, locator, message_level, message, exception, 
                 message_types):
      """
      Args:
        locator (str) :        location within source code
        message_level(str):    system logging level (e.g., "INFO")
        message (str):         the log message
        exception (Exception): Python run time exception object
        message_types[(str)]:  the supported messages levels
      """
      self.issues = []
      self.message = ''
          
      if((type(locator) != str) or (locator == '')):
        self.issues.append('Invalid locator parameter')
        print('Invalid locator parameter')
      else:
        self.locator = locator
        
      if((type(message_level) != str) or (message_level == '')):
        self.issues.append('Invalid message_level parameter')
        print('Invalid message_level parameter')
      elif(not (message_level.upper() in message_types)):
        self.issues.append('Invalid message level specified')
        print('Invalid message level specified')
      else:
        self.message_level = message_level.upper()
        
      if((type(message) != str) or (message == '')):
        self.issues.append('Invalid message parameter')
        print('Invalid message parameter')
      else:
        if(message):
          self.message += message
          
      if((not isinstance(exception, Exception)) and (exception != '')):
        self.issues.append('Invalid exception parameter')
        print('Invalid exception parameter')
      else:
        if(exception):
          try:
            if(exception.response['Error']['Message']):
              self.message += ' ' + exception.response['Error']['Message']
            else:
              self.message += ' ' + str(exception)
          except:
            self.message   += ' ' + str(exception)
        
  
  def  __init__(self, module, info_table, errors_table, tz_offset, ttl):
    """ 
    Initialize a system logging object.  The module, info_table, and 
    error_table parameters are required.  The tz_offset, and ttl 
    parameters are optional.
    
    Args:
      module       (str): meaningful name for code in source code file
      info_table   (str): DynamoDB table name for logginf info messages
      errors_table (str): DynamoDB table name for logging error messages 
      tz_offset    (int): time zone offset from UTC
      ttl          (int): time to live for information messages
    """
    self.NUM_SECONDS_IN = {'1 month'  : 2592200, 
                           '2 months' : 5184000, 
                           '6 months' : 155520000}
    self.MESSAGE_TYPES  = {'INFO': 1, 'WARN': 2, 'ALARM': 6, 'ERROR': 7}
    self.error_messages = {}
    self.info_messages  = {}
    self.init_issues    = []
    self.run_issues     = []
      
    if((type(module) != str) or (module == '')):
      self.init_issues.append('Invalid module parameter')
      print('Invalid module parameter')
    else:
      self.module = module
      
    if((type(info_table) != str) or (info_table == '')):
      self.init_issues.append('Invalid info_table parameter')
      print('Invalid info_table parameter')
    else:
      self.info_table = info_table
      
    if((type(errors_table) != str) or (errors_table == '')):
      self.init_issues.append('Invalid errors_table parameter')
      print('Invalid errors_table parameter')
    else: 
      self.errors_table = errors_table
    
    if((tz_offset == '') or (tz_offset == None)):
      self.TZ_OFFSET = 6                               #default is CST
    else:
      try:
        if(type(tz_offset) == str):
          tz_offset = int(tz_offset)
        if((type(tz_offset) != int) or (not(-13 < tz_offset < 15))):
          self.init_issues.append('Invalid tz_offset parameter')
          print('Invalid tz_offset parameter')
        else:
          self.TZ_OFFSET = tz_offset
      except Exception as e:
        self.init_issues.append('Invalid time zone offset specified. ' + str(e))
        print('Invalid time zone_offset specified. ' + str(e))
        self.TZ_OFFSET = 6                    #defaul to offset for CST
        
    if((ttl == '') or (ttl == None)):
      self.TTL = self.NUM_SECONDS_IN['2 months']  #default
    else:
      try:
        if(type(ttl) == str):
          ttl = int(ttl)     
        if((type(ttl) != int) or 
           (ttl < self.NUM_SECONDS_IN['1 month']) or
           (ttl > self.NUM_SECONDS_IN['6 months'])): 
          self.init_issues.append('Invalid ttl parameter')
          print('Invalid ttl parameter')
        else:
          self.TTL = ttl
      except Exception as e:
        self.init_issues.append('Invalid time to live specified. ' + str(e))
        print('Invalid time to live parameter specified. ' + str(e))       
        self.TTL = self.NUM_SECONDS_IN['2 months']            #default        
    
    
  def reset(self):
    """
    If a sys_log object was to be used in a long running application 
    you would want to clear out data associated with the last use of 
    the object.  This includes error and info messagess that have 
    already been written to DynamoDB and run time issues / errors 
    that occured.
    """
    self.error_messages = {}
    self.info_messages  = {}
    self.run_issues     = []
    
    
  def log_message(self, locator, message_level, message, exception):
    """
    The granularity of time stamp used is seconds vs. milliseconds.
    Inbound messages are time stamped and this stamp is used as a
    componenet of a key (i.e., dictionary, DynamoDB table sort). AS
    This key must be unique, a 1 second time delay is introduced to
    avoid key conflicts.  This is not an issue when sys_log is used
    in low volume, infrequent call situaitons.

    Args:
      locator (str):        req; location inside module (source code)
      message_level (str):  req; 'INFO', 'WARN', 'ALARM', or 'ERROR' 
      message (str):        opt; free form error message or empty str
      exception(Exception): opt; Exception object or empty string
      
    Returns:
      True  system logging messages was successfully processed
      False processing of system logging messaged resulted in an error
    """
    results = False
    if(not self.init_issues):
      a_message_core = self.message_core(locator, message_level, message, 
                                       exception, self.MESSAGE_TYPES)
      if(not a_message_core.issues):   
        self.time.sleep(1)
        now = self.datetime.now()
        timestamp = int(now.timestamp()) #converts millisecons to seconds
        if(self.TZ_OFFSET >= 0):
          local = now - self.timedelta(hours=self.TZ_OFFSET)    #UTC -> local
        else:
          local = now + self.timedelta(hours=self.TZ_OFFSET)    #UTC -> local
        date = str(local.year) + '-' + str(local.month).zfill(2)
        date += '-' + str(local.day).zfill(2)
        stamp_mod = str(timestamp) + '+' + self.module
        
        #all pieces valid; assemble message and store in dict; will write
        #to DynamoDB table in _save_messages_to_db() function
        message = (a_message_core.message_level + ': (' + 
                   a_message_core.locator + ') ' + 
                   a_message_core.message)
        if(self.MESSAGE_TYPES[a_message_core.message_level] < 
           self.MESSAGE_TYPES['ALARM']):
          expiration = timestamp + self.TTL
          self.info_messages[stamp_mod] = {'date' : date,
                                           'message' : message,
                                           'expiry' : expiration}
        else:
          self.error_messages[stamp_mod] = {'date' : date,
                                            'message' : message}
        results = True
      else:
        self.run_issues.append('One or more message_core() elements invalid')
        print('One or more message_core() elements invalid')      
    else:
      self.run_issues.append('System logging object inoperable.')
      print('sys_log() object invalid / inoperable')
      
    return(results)
    
       
  def save_messages_to_db(self):
    """
    Write all of the informaitonal and error messages stored in the
    sys_log object's buffers to DynamoDB tables.
    
    Returns:
      True  if no errors were encountered
      False if an error was encountered
    """
    results = True
    try:
      dynamo_db_access = self.boto3.resource('dynamodb')     
      if(self.error_messages):
        try:
          table = dynamo_db_access.Table(self.errors_table)
          for key, value in self.error_messages.items():
            table.put_item(Item={'date' : value['date'], 
                                 'stamp_job' : key,
                                 'message': value['message']})
        except Exception as e:
          results = False
          self.run_issues.append('Exception thrown connecting / writing to ' +
            'error messages DynamoDB table.  Exception: ' + str(e))
          print('Exception thrown connecting / writing to error messages ' + 
                'DynamoDB table.  Exception: ' + str(e))
      if(self.info_messages):
        try:
          table = dynamo_db_access.Table(self.info_table)
          for key, value in self.info_messages.items():
            table.put_item(Item={'date': value['date'],
                                 'stamp_job' : key, 
                                 'message': value['message'], 
                                 'expiry' : value['expiry']})
        except:
          results = False
          self.run_issues.append('Exception thrown connecting / writing to ' +
            'info messages DynamoDB table.  Exception: ' + str(e))
          print('Exception thrown connecting / writing to info messages ' + 
                'DynamoDB table.  Exception: ' + str(e))
    except:
      results = False
      self.run_issues.append('Could not connect to DynamoDB service')
      print('Could not connect to DynamoDB')
      
    return(results)
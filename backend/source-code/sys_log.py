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
facility for serverless applications hosted in AWS.  You may find
it preferrable to Python's logging module (i.e., standard library)
with its smaller footprint and direct message injection into DynamoDB.

This module was designed to provide a temporal, single-use system 
logging object to service Lambda functions that provide the backend 
of RESTful APIs or as components of a stateless application.  It has 
not been tested for suitability within a long running application.  
Further, handling a high volume of system log messaging was not a 
design goal.

sys_log categorizes messages into multiple levels of condition 
severity. While the default levels of INFO, WARN, ALARM, and ERROR can
be easily modified keep the following two things in mind.  First, the 
different message categories flow from informational (i.e., INFORM) to 
somewhat concerning (i.e., WARN) to concering (i.e., ALARM) to 
seriously concerning (i.e., ERROR).  Second, for a well-designed and
well-built applicaiton, system log messages on the informational side
of the spectrum will be generated far more frequently than system log 
messages on the seriously concerning side of the spectrum.  Due to this
second point, automated data lifecycle management should be put in place
to manage the DynamoDB table holding informational messages.  You will
notice a TTL attribute is created for system log messages destned for
this table.  Lifecycle management of messages contained in the DynamoDB 
table holding concerning messages should be a manual process for several 
reasson (i.e., low volume, you don't want to delete messages until full 
RCA has completed). 

Future enhancement: accept an additional parameter for the creation of
sys_log() objects that determines what level (e.g., INFORM) are stored
in DynamoDB.  This would be a nice feature as the number of INFORM and 
WARN messages can be quite large and that can introduce DynamoDB cost 
concerns (i.e., write capacity units - hrs consumed per month).  For 
now, a stop gap solution in the form of a read of a DynamoDB table to
determine if the INFO and WARN level messages should be written to the
DynamoDB table or not (i.e., _store_in_dynamo()).  ALARM and ERROR
messages should be very rare, low volume, and should always be written
to DynamoDB.

Usage Overview:
  0) DynamoDB
     Create tables (one for info, one for errors) each with a primary
       key named 'date' of type string and a sort key named 'stamp_job' 
       of type string
     After data has flowed into info messages table, enable TTL using 
       the attribute 'expiry'
  1) In the Python module requiring system logging, import this module
     and create a global system logging object
  2) In the Python module requiring system logging, use the global object
     to submit system logging messages
  3) Before existing the Python module, flush all of the system logging
     messages to DynamoDB tables

Be aware: 
  1) As sys_log was designed to support serverless applications running in
     AWS, diagnostic print statements are in place as they will show in 
     CloudWatch, given correct configuration

  2) sys_log was not designed to support high-volume system logging.  Note
     the 1 second delay introduced in sys_log.log_message()

  3) After creating a sys_log object, sys_log.init_issues should be empty.  
     If this list is not empty, the sys_log object will most likely not work

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
    if(sl.save_messages_to_db()):
      #system logging object successfully stored all messags in Dynamo
    if(not sl.run_issues):
      #another way to check on the success of write to Dynamo
  """
  import time
  import boto3
  from   boto3.dynamodb.conditions import Key
  from   datetime                  import datetime, timedelta

  class message_core():
    """
    This class validates parameters sent to sys_log.log_message() 
    and isolates / builds the basic system log message elements that 
    will be used when assembling the complete message.
    
    if(message_core.issues): 
      #issues encountered when creating this message_core object
    """


    def _issue_occurred(self, err_msg):
      """
      """
      self.issues.append(err_msg)
      print(err_msg)


    def __init__(self, locator, message_level, message, exception, 
                 message_types):
      """
      Args:
        locator (str) :        location within source code
        message_level(str):    system logging level (e.g., "INFO")
        message (str):         log message or '' or None
        exception (Exception): Python exception object or '' or None
        message_types[(str)]:  supported messages levels
      """
      self.issues = []
      self.message = ''

      if((type(locator) != str) or (locator == '')):
        self._issue_occurred('Invalid locator parameter')
      else:
        self.locator = locator

      if((type(message_level) != str) or (message_level == '')):
        self._issue_occurred('Invalid message_level parameter')
      elif(not (message_level.upper() in message_types)):
        self._issue_occurred('Invalid message level specified')
      else:
        self.message_level = message_level.upper()

      if((type(message) != str) and (message != None)):
        self._issue_occurred('Invalid message parameter')
      else:
        if(message):
          self.message += message

      if((not isinstance(exception, Exception)) and 
         (exception != '') and (exception != None)):
        self._issue_occurred('Invalid exception parameter')
      else:
        if(exception):
          try:
            if(exception.response['Error']['Message']):
              self.message += ' ' + exception.response['Error']['Message']
            else:
              self.message += ' ' + str(exception)
          except:
            self.message   += ' ' + str(exception)


  def _issue_occurred(self, err_type, err_msg):
    """
    """
    if(err_type == 'init'):
      self.init_issues.append(err_msg)
      print('Init issue: ' + err_msg)
    elif(err_type == 'run'):
      self.run_issues.append(err_msg)
      print('Run issue: ' + err_msg)
    else:
      self.run_issues.append(err_msg)
      print('? issue: ' + err_msg)


  def  __init__(self, module, info_table, errors_table, 
                tz_offset, ttl, strict=False):
    """ 
    Initialize a system logging object.  The module, info_table, and 
    error_table parameters are required.  The tz_offset, and ttl 
    parameters are optional.

    Args:
      module       (str):  meaningful name for code in source code file
      info_table   (str):  DynamoDB table name for logginf info messages
      errors_table (str):  DynamoDB table name for logging error messages 
      tz_offset    (int):  time zone offset from UTC
      ttl          (int):  time to live for information messages
      strict       (bool): auto recover from invalid tz_offset and ttl
                           parameter values being passed in
    """
    self.NUM_SECONDS_IN = {'1 month'  : 2592200, 
                           '2 months' : 5184000, 
                           '6 months' : 155520000}
    self.MESSAGE_TYPES  = {'INFO': 1, 'WARN': 2, 'ALARM': 6, 'ERROR': 7}
    self.error_messages = {}
    self.info_messages  = {}
    self.init_issues    = []
    self.run_issues     = []

    tz_default  = 6             #US Central (5 - 6 hours behind UTC)
    ttl_default = self.NUM_SECONDS_IN['2 months']

    if((type(module) != str) or (module == '')):
      self._issue_occurred('init', 'Invalid module parameter')
    else:
      self.module = module

    if((type(info_table) != str) or (info_table == '')):
      self._issue_occurred('init', 'Invalid info_table parameter')
    else:
      self.info_table = info_table

    if((type(errors_table) != str) or (errors_table == '')):
      self._issue_occurred('init', 'Invalid errors_table parameter')
    else: 
      self.errors_table = errors_table

    if((tz_offset == '') or (tz_offset == None)):
      self.TZ_OFFSET = tz_default
    else:
      try:
        if(type(tz_offset) == str):
          tz_offset = int(tz_offset)
        if((type(tz_offset) != int) or (not(-13 < tz_offset < 15))):
          if(strict):
            self._issue_occurred('init', 'Invalid tz_offset parameter')
          self.TZ_OFFSET = tz_default
        else:
          self.TZ_OFFSET = tz_offset
      except Exception as e:
        if(strict):
          self._issue_occurred('init', 'Invalid tz_offset parameter ' + 
            'specified. Exception: ' + str(e))
        self.TZ_OFFSET = tz_default

    if((ttl == '') or (ttl == None)):
      self.TTL = ttl_default
    else:
      try:
        if(type(ttl) == str):
          ttl = int(ttl)     
        if((type(ttl) != int) or 
           (ttl < self.NUM_SECONDS_IN['1 month']) or
           (ttl > self.NUM_SECONDS_IN['6 months'])): 
          if(strict):
            self._issue_occurred('init', 'Invalid ttl parameter')
          self.TTL = ttl_default
        else:
          self.TTL = ttl
      except Exception as e:
        if(strict):
          self._issue_occurred('init', 'Invalid ttl parameter specified. ' +
            'Exception: ' + str(e))
        self.TTL = ttl_default


  def _store_in_dynamo(self):
    """
    This is a stop gap measure.  Ultimately an additional parameter
    should be added to sys_log.__init__() that will designate what 
    logging level (e.g., INFO, WARN, ALARM, ERROR) of messages should
    be stored in DynamoDB.  This function serves as a table driven
    means to turn the storage of INFO and WARN level logging messages
    on or off.  An attribute from an item in a DynamoDB control table
    is referenced to make this determination.
    """
    CONTROL_TABLE       = 'MyTablName'
    CONTROL_ITEM_SELECT = '123'
    rc = False

    try:
      dynamo_db_access = self.boto3.resource('dynamodb')

      try:
        table = dynamo_db_access.Table(CONTROL_TABLE)
        response = table.query(
          KeyConditionExpression=self.Key('control_id').eq(CONTROL_ITEM_SELECT))
        if(response['Items']):
          if((response['Items'][0]['info_logs_store']) == 'on'):
            rc = True
      except Exception as e:
        self._issue_occurred('run', 'Exception thrown connecting / writing ' +
          'to DynamoDB control table.  Exception: ' + str(e))
    except Exception as e:
      self._issue_occurred('run', 'Exception thrown connecting to DynamoDB ' +
        'service.  Exception: ' + str(e))

    return(rc)	


  def reset(self):
    """
    If a sys_log object was to be used in a long running application 
    you would want to clear out data associated with the last use of 
    the object.  This includes error and info messagess that have 
    already been written to DynamoDB and run time issues / errors 
    that occured.
    
    No concerted thought/effort was placed into ensuring that sys_log
    would function well for a long running applicaiton.  This function
    is just a start, you may need to do more.
    """
    self.error_messages = {}
    self.info_messages  = {}
    self.run_issues     = []


  def log_message(self, locator, message_level, message, exception):
    """
    The granularity of time stamp used is seconds vs. milliseconds.
    Inbound messages are time stamped and this stamp is used as a
    componenet of a key (i.e., dictionary, DynamoDB table sort). As
    this key must be unique, a 1 second time delay is introduced to
    avoid key conflicts.  This is not an issue when sys_log is used
    in situations with a relatively low volume of system logging.

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
        stamp_job = str(timestamp) + '+' + self.module
        
        #all pieces valid; assemble message, store in dict holding all messages
        message = (a_message_core.message_level + ': (' + 
                   a_message_core.locator + ') ' + 
                   a_message_core.message)
        if(self.MESSAGE_TYPES[a_message_core.message_level] < 
           self.MESSAGE_TYPES['ALARM']):
          expiration = timestamp + self.TTL
          self.info_messages[stamp_job] = {'date' : date,
                                           'message' : message,
                                           'expiry' : expiration}
        else:
          self.error_messages[stamp_job] = {'date' : date,
                                            'message' : message}
        results = True
      else:
        self._issue_occurred('run', 'One or more message_core() elements invalid')
    else:
      self._issue_occurred('run', 'sys_log() object invalid / inoperable')

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
          self._issue_occurred('run', 'Exception thrown connecting / writing' +
            ' to error messages DynamoDB table.  Exception: ' + str(e))

      if((self.info_messages) and (self._store_in_dynamo())):
        try:
          table = dynamo_db_access.Table(self.info_table)
          for key, value in self.info_messages.items():
            table.put_item(Item={'date': value['date'],
                                 'stamp_job' : key, 
                                 'message': value['message'], 
                                 'expiry' : value['expiry']})
        except Exception as e:
          results = False
          self._issue_occurred('run', 'Exception thrown connecting / writing' +
            ' to info messages DynamoDB table.  Exception: ' + str(e))

    except:
      results = False
      self._issue_occurred('run', 'Could not connect to DynamoDB service.')

    return(results)
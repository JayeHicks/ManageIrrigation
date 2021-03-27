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
  
This module serves as an AWS Lambda function.  It will be invoked
ad hoc, via a service integration with a single API Gateway API
in order to enable invocation of Absence Guard, Battery Guard, 
Freeze Guard, or Moisture Guard from the Control Sensors / Alarms
single page web application.

Usage: 
  The single page web application Contorl Sensors / Alarms provides
  buttons that an end user can select to invoke various guards. 
  
  This module is invoked on an ad hoc basis

A custom system logging class was created to capture system logging
messages generated while this module executes.  At the conclusion of
this module's execution the system logging object writes all of the
system logging messages to DynamoDB tables.  Because this module
executes as an AWS Lambda function, and the Lambda retains containers
for a short period of time after the function exits (in hopes of 
reusing the container for a future call of the same function), it is
necessary to reset/clear the system logging object at the beginning
of this module's execution.

Dependencies:
  import boto3
  import json
  import sys_log
"""
import boto3
import json

import sys_log

ALL_JOBS = ['absence_guard', 'archive_check', 'archive_data', 'battery_guard',
            'delete_data', 'freeze_guard', 'moisture_guard', 'pull_data', 
            'update_status']
GUARDS   = ['absence_guard', 'battery_guard', 'freeze_guard', 'moisture_guard']

#global object provides system logging to DynamoDB tables
sl = sys_log.sys_log('invoke_job','ADynDBTableForInfo',
                                  'ADynDBTableForIssues','','')
   
   
def invoke_job(event, context):                  
  """
  Invoked ad hoc by single page web application via API Gateway.
  
  Args (supplied by AWS Lambda service)
    event: information about who/what invoked the Lambda function
    context: information about the Lambda function's runtime environment
  """  
  sl.reset()
  
  if(event):
    job = event['job'].lower()
    if(job in GUARDS):
      try:
        lambda_access = boto3.client('lambda')  
        try:
          lambda_access.invoke(FunctionName = job, 
                                  InvocationType = 'Event')
        except Exception as e:
          sl.log_message('1', 'ERROR', '', e)
      except Exception as e:
        sl.log_message('2', 'ERROR', '', e)
    else:
      sl.log_message('3', 'ERROR', 'Invalid job name "' + job + 
                     '" specified.', '')
  else:
    sl.log_message('4', 'ERROR', 'Empty parameter to Lambda function.', '')
  
  if(sl.error_messages):
    sl.log_message('5', 'WARN', 
      'invoke_job() process did not complete successfully.', '')
  else:
    sl.log_message('5', 'INFO',
      'invoke_job() process completed successfully.', '')
  sl.save_messages_to_db()
  
  # API Gateway integration that invokes this module will use its own CORS 
  # and ignore these.  However, it does not harm to supply them and will
  # prove instructive if, in the future, a need arises to call this module
  # directly vs. through an API Gateway integration.  For future reference,
  # Access-Control-Allow-Headers is used for the OPTIONS method while
  # GET only requires the return of Access-Control-Allow-Origin
  return({'statusCode': 200,
          'headers': {'Access-Control-Allow-Headers': 'Content-Type,' +
                                                      'X-Amz-Date,' +
                                                      'Authorization,' +
                                                      'X-Api-Key,' +
                                                      'X-Amz-Security-Token',
                      'Access-Control-Allow-Origin': '*',
                      'Access-Control-Allow-Methods': 'OPTIONS,GET'},
          'body': json.dumps('all good here...')})
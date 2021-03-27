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

This module provides a highly targeted, specialized alert messaging
facility for serverless applications hosted in AWS.  Currently, only
supports the SNS distribuiton channel but desinged to support any
other distribution channel. 

Usage:
  0) set up all of the requisite AWS SNS topics
  1) create a send_alerts object, supplying all of the alert messages;
     object creation results in sending messages
  2) interrogate the send_alrerts object to determine the success
     / failure of both initialization of object and the processing
     of each individual alert message
     channel(s)
  
Be aware: 
1) As send_alerts was designed to support serverless applications 
   running in AWS, diagnostic print statements are in place as they 
   will show in CloudWatch, given correct configuration

2) send_alerts was not designed to support high-volume alert messaging

3) After object creation, the send_alerts object offers a list
   containing strings that represent any object initialization errors
   or run time errors that occured alert message transmission over the
   distribution channel(s)

Dependencies:
  boto3
"""
class send_alerts():
  """
  Provides specialized alert messaging for stateless applications
    
  The send_alerts.__init__() takes a single parameter.  It is flexible
  enough to accomodate multiple messages and each message can be sent
  to multiple distribution channels. Currently, the only message 
  distribution channel implemented is AWS SNS.
  
  Usage:
    import send_alerts
    param = [{'channel':'sns', 'message':'an alert message', 
      'topic_arns':['arn:aws:sns:us-east-1:12345678901:MyAlert']}]
    a_sm = send_alerts(param)
    if(a_sm.issues): 
      #issue(s) occured during initialization or sending message(s)
  """
  import boto3
  
  
  def _send_sns_messages(self, param):
    """
    Send a single message to one or more AWS SNS topics.  
    
    Note: message publication will stop if an exception occurs, even
          the message has not yet been sent to all arns.
    
    Args:
      param  [{})]: required.  example parameter:
        [{'channel':'sns', 
          'message':'an alert message', 
          'topic_arns':['arn:aws:sns:us-east-1:12345678901:MyAlert']}]
    """
    try:
      sns_access = self.boto3.client('sns')
      for arn in param['topic_arns']:
        print('Sending to: ' + arn + ' a message of: ' + param['message'])        
        resp = sns_access.publish(TargetArn=arn,
                                  Message=param['message'])
        if(resp['ResponseMetadata']['HTTPStatusCode'] != 200):
          error_code = str(resp['ResponseMetadata']['HTTPStatusCode'])
          self.issues.append('Response from publish to sns topic indicates ' +
                             'failure. HTTPStatusCode: ' + error_code)
          print('Response from publish to sns topic indicates ' + 
                'failure. HTTPStatusCode: ' + error_code)
        else:
          print('Successfully published message to topic: ' + arn)
    except Exception as e:
      self.issues.append('Exception while publishing to sns topic. ' +
                         'Exception involving: ' + str(e))
      print('Exception while publishing to sns topic. Exception ' +
            'involving: ' + str(e))
          

  def _validate_sns_message(self, param):
    """
    Future exercise: determine canonical AWS SNS arn and use RE to validate
    
    Args:
      param  [{})]: required.  example parameter:
        [{'channel':'sns', 'message':'an alert message', 
         'topic_arns':['arn:aws:sns:us-east-1:12345678901:MyAlert']}]
          
    Return:
      True  if everything checks out
      False if an issue was uncovered
    """  
    results = True
    
    try:     
      if((type(param['message']) != str) or (param['message'] == '')):
        results = False
        self.issues.append('Invalid message parameter for an sns alert message')
        print('Invalid message parameter for an sns alert message')
      
      if((type(param['topic_arns']) != list) or (not param['topic_arns'])):
        results = False
        self.issues.append('Empty or invalid list of topic arns supplied')
        print('Empty or invalid list of sns topic arns supplied')
      
      for arn in param['topic_arns']:
        if((type(arn) != str) or (arn == '') or ('arn' not in arn) or  
           ('aws' not in arn) or ('sns' not in arn)):
          results = False
          self.issues.append('Invalid sns topic arn specified')
          print('Invalid sns topic arn specified')
    except Exception as e:
      results = False
      self.issues.append('Exception thrown involving: ' + str(e))
      print('Exception thrown involving: ' + str(e))
    return(results)
  
  
  def  __init__(self, params):
    """ 
    Initialize a send_alert object.  Process all of the alert of the
    valid messages, even if an invalid message is encountered.  To 
    date, AWS SNS is the only supported channel supported.  After 
    a send_alert object is created the 'issues' attribute can be 
    checked to determine if either an initialization or a run time 
    error occured.
    
    Args:
      param  [{})]: required.  example parameter:
        [{'channel':'sns', 'message':'an alert message', 
         'topic_arns':['arn:aws:sns:us-east-1:12345678901:MyAlert']}]
    """
    self.supported_channels = ['sns']
    self.issues             = []
    
    try:
      if(params):
        for param in params:
          if(param['channel'] in self.supported_channels):
            if(param['channel'] == 'sns'):
              if(self._validate_sns_message(param)):
                self._send_sns_messages(param)
              
            #future: add additional distribution channels here
          else:
            self.issues.append('Unrecognized message distribution channel ' +
                               'specified')
            print('Unrecognized message distribution channel specified')  
    
      else:
        self.issues.append('Attempt to create send_alerts object with an ' +
                           'empty input parameter')
        print('Attempt to create send_alerts object with an empty input ' + 
              'parameter')
    except Exception as e:
      self.issues.append('Exception thrown involving: ' + str(e))
      print('Exception thrown involving: ' + str(e))
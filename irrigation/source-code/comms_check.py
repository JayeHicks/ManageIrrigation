"""
Jaye Hicks 2020

Deployment check list: 
  - set comms_check.env to 'debug' or 'prod'
  - remove commenting out of API invcation; comment out for debug

Obligatory legal disclaimer:
  You are free to use this source code (this file and all other files 
  referenced in this file) "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER 
  EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. 
  THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THIS SOURCE CODE
  IS WITH YOU.  SHOULD THE SOURCE CODE PROVE DEFECTIVE, YOU ASSUME THE
  COST OF ALL NECESSARY SERVICING, REPAIR OR CORRECTION. See the GNU 
  GENERAL PUBLIC LICENSE Version 3, 29 June 2007 for more details.

NOTE: the identifier 'boto3' is overidden in the comms_check()
  
This class is used to determine if a communication link is available
from the platform executing this module to the AWS backend.  This is
achieved by attempting to successfully invoke a known, valid AWS API 
Gwy endpoint (AWS ensures high availability for all APIs hosted in
its API Gateway service).  A binary result is returned by objects of
this class: 'comms_good' or 'comms_bad.'  If 'comm_bad' is returned, 
no attempt is made by this class to diagnose or repair any 
communications issues.

Usage:
  >>> import comms_check
  >>> cc = comms_check.comms_check()
  >>> cc.contact_aws()
  True
  >>>
"""
class comms_check():
  import logging
  import boto3
  import boto3.session
  import requests
  from   requests_aws4auth import AWS4Auth


  def __init__(self):
    """
    NOTE: identifier 'boto3' overidden to refer to a boto3 session
    """
    self.logger = self.logging.getLogger(__name__)

    self.logger.info('entering: __init__()')
    self.env    = 'debug'  #set to 'debug' or 'prod'
    
    if(self.env   == 'debug'):
      self.config = {'end_point' : 
        'https://abcdefghji.execute-api.us-east-1.amazonaws.com/prod',
                     'region' : 'us-east-1'}
      #overide identifier 'boto3' (session using IAM credentials below)
      try:
        self.boto3 = self.boto3.Session(
          aws_access_key_id     = '123456789012345678901', 
          aws_secret_access_key = '1234567890123456789012345678901234567890',
          region_name           = self.config['region'])
      except Exception as e:
        self.logger.error(f'1 Could not create boto3 session.  Exception: {e}')
    else: 
      self.config = {'end_point' : 
        'https://abcdefghij.execute-api.us-east-1.amazonaws.com/prod',
                     'region' : 'us-east-1'}
      #overide identifier 'boto3' (session using AWS CLI default profile)
      try:
        self.boto3 = self.boto3.session.Session()
      except Exception as e:
        self.logger.error(f'2 Could not create boto3 session. Exception: {e}')


  def contact_aws(self):
    """
    Args:

    Returns:
      None                Issue before attempting contact w AWS
      True                API Gwy endpoint successfully called
      False               Issue invoking API Gwy endpoint
    """
    self.logger.info('entering: contact_aws()')

    result      = None
    try:
      credentials = self.boto3.get_credentials()
      auth        = self.AWS4Auth(credentials.access_key, 
                                  credentials.secret_key,
                                  self.config['region'], 
                                  'execute-api')
      endpoint    = self.config['end_point']
      method      = 'GET'
      headers     = {}
      body        = ''
    
      try:
        response    = self.requests.request(method, endpoint, auth=auth, 
                                            data=body, headers=headers)
        #status_code = response.status_code
        #print(f'99 response: {response.text}')   #handy for debugging

        # contact made; status_code == 200 (or not) makes no difference
        result = True 
  
      except Exception as e:
        result = False
        self.logger.error(f'3 Apparent comms issue. Exception: {e}')
    except Exception as e:
        self.logger.error(f'4 Issue before comms test.  Exception: {e}')
    return(result)
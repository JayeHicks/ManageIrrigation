"""
Jaye Hicks 2021

Deployment check list: set alarms.env to 'debug' or 'prod'

Obligatory legal disclaimer:
  You are free to use this source code (this file and all other files 
  referenced in this file) "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER 
  EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. 
  THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THIS SOURCE CODE
  IS WITH YOU.  SHOULD THE SOURCE CODE PROVE DEFECTIVE, YOU ASSUME THE
  COST OF ALL NECESSARY SERVICING, REPAIR OR CORRECTION. See the GNU 
  GENERAL PUBLIC LICENSE Version 3, 29 June 2007 for more details.
  
If an alarm state is detected, regardless of the alarm type, a 
flat file containing the alarm's detail will be placed in a special
directory.  Objects of type alarms() can be used to manage these
individula alarm files.  This involves sending alarm information to
the AWS backend, deleting local alarm flat files that are no longer
needed and detecting / managing alarm files that are suspect (e.g.,
corrupt file, result in a bad response code from AWS, have aged
beyond the acceptable threshold because continued attempts to send
the files keep failing before actual call to an AWS endpoint.

Individual alarm files are augmented by dura_file functionality.

NOTE: the identifier 'boto3' is overidden in the class alarms()

Alarm files must follow a stict naming standard.  Specifically, the
alarm's creation year must follow the second'_' in the name, the
creation month must follow the third '_', and the creation day must
follow the fourth '_'.  The pattern is: 
'alarm_<type>_<yyyy_mm_dd>_<sched id>_<sequence>.json'

Examples: 
'alarm_under_2021_10_31_55_1.json'
'alarm_over_2021_10_31_08_02.json'

If a specific alarm file is determined to be corrupt or invoking the
API Gateway API with the file results in a non-200 return code then
this Python module will move the alarm flat file to special folder 
that a different Python module will access in order to upload the
suspect files to the AWS backend for future forensic analysis.

If an error occurs before the module invokes an API Gatewwy API the
alarm file will be left in place and future invocations of this moulde
will retry transmission to AWS.  This will continue until the file ages
beyond the maximum age threshold, at which point the file will be 
placed in a special directory that a different Python module will 
access in order to upload the file to the AWS backend for future 
forensic analysis.
"""
class alarms():
  import logging
  import boto3
  import boto3.session
  import requests
  from   requests_aws4auth import AWS4Auth
  from   pathlib           import Path
  from   datetime          import datetime, timedelta

  import dura_file
  import lv_paths


  def __init__(self):
    """
    NOTE: identifier 'boto3' overidden to refer to a boto3 session
    """
    self.logger = self.logging.getLogger(__name__)

    self.logger.info('entering: __init__()')
    self.env          = 'debug'                   #set to 'deubg' or 'prod'
    self.max_age_days = 2  # upload files aged past threshold to S3
    self.df           = self.dura_file.dura_file()
    self.paths        = self.lv_paths.lv_paths()
    
    if(self.env   == 'debug'):
      self.config = {'end_point' : 
        'https://abcdefghij.execute-api.us-east-1.amazonaws.com/prod',
                     'region' : 'us-east-1'}
      #overide identifier 'boto3' (session using IAM credentials below)
      try:
        self.boto3 = self.boto3.Session(
          aws_access_key_id     = '12345678901234567890', 
          aws_secret_access_key = '123456789012345678901234567890123',
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
        self.logger.error(f'2 Cloud not create boto3 session. Exception: {e}')

    self._reset_status()


  def _alarm_file_aged_past_limit(self, file_name):
    """
    Args
      file_name(str)      name of alarm file.  Must follow strict
                            standard.  See comments top of module
                            Example: 
                              'alarm_under_2021_10_31_55_1.json'
    Returns
      True                the alarm files age is beyond threshold
      False               the alarm files age is not beyond threshold
    """
    self.logger.info('entering: _alarm_file_aged_past_limit()')
    
    result = False
    try:
      now_date_time = self.datetime.today()

      file_year     = file_name.split('_')[2]
      file_month    = file_name.split('_')[3]
      file_day      = file_name.split('_')[4]

      date_time_limit = self.datetime(year=int(file_year),
                                      month=int(file_month),
                                      day=int(file_day),
                                      hour=23,
                                      minute=59)
      date_time_limit += self.timedelta(days=self.max_age_days)
  
      if(date_time_limit < now_date_time):
        result = True
    except Exception as e:
      self.logger.error(f'3 Exception: {e}')
    return(result)


  def _reset_status(self):
    """
    Clear out communication details from last transmission of alarms 
    """
    self.logger.info('entering: _reset_status()')  

    self.comms_ok   = None
    self.trans_good = []     # all files successfully sent to API
    self.trans_bad  = []     # all files that couldnt be sent to API
    self.clean_good = []     # all files successfully deleted
    self.clean_bad  = []     # all files that couldnt be deleted
    self.move_good =  []     # all suspect files successfuly moved
    self.move_bad  =  []     # all suspect files that couldnt be moved


  def good_comms(self):
    self.logger.info('entering: good_comms()')  
    return(self.comms_ok)


  def good_trans_cnt(self):
    self.logger.info('entering: good_trans_cnt()')  
    return(len(self.trans_good))


  def bad_trans_cnt(self):
    self.logger.info('entering: bad_trans_cnt()')  
    return(len(self.trans_bad))


  def good_clean_cnt(self):
    self.logger.info('entering: good_clean_cnt()')  
    return(len(self.clean_good))


  def bad_clean_cnt(self):
    self.logger.info('entering: bad_clean_cnt()')  
    return(len(self.clean_bad))


  def good_move_cnt(self):
    self.logger.info('entering: good_move_cnt()')  
    return(len(self.move_good))

  
  def bad_move_cnt(self):
    self.logger.info('entering: bad_move_cnt()')  
    return(len(self.move_bad))


  def _file_valid(self, path):
    """
    Uses dura_file() class functionality to determine validity
  
    Args:
      path(pathlib)   pathlib object represents single alarm file

    Returns:
      None            Could not open file; damaged or nonexistant
      True            The file's contents are valid
      False           The file's contents are invalid
    """
    self.logger.info('entering: _file_valid()')  
    result = None
    if(path):
      if(path.exists() and path.is_file()):
        try:
          with path.open('r') as fd:
            contents = fd.read()
            if(self.df.check_object(json_object=contents)):
              result = True
            else:
              result = False
        except Exception as e:
          self.logger.error(f'4 Could not open file. Exception: {e}')
      else:
        self.logger.error(f'5 No such file exists: {path.name}')
    return(result)


  def _move_file(self, file, target):
    """
    Move file that is either corrupt or resulted in a failed 
    communication attempt.
 
    Args:
      file(pathlib)     pathlib object represents corrupted file
      targert(str)      index into lv_paths object to obtain
                          fully qualified destination folder.

    Returns:
      None           Failure before attempt to move file
      False          Failed to move file to targer direcotry
      True           Successfully moved file to target directory
    """
    self.logger.info('entering: _move_file()')

    result = None
    if(file and target and (type(target) == str)):
      target = target.strip().lower()
      try:
        file_name = file.name
        if(file_name):
          new_path = (self.paths.get_path(target) + 
                      self.paths.divider +
                      file_name)
          try:
            file.rename(new_path)
            result = True
          except Exception as e:
            self.logger.error(f'6 Could not move file: {file_name} to ' +
                              f'{target}. Exception: {e}')
            result = False
        else:
          self.logger.error('7 Could not extract file name from pathlib ' +
                            'object')
      except Exception as e:
        self.logger.error('8 Bad argument passed to _move_file(). ' +
                          f'Exceptions {e}')
    return(result)


  def _call_alarm_api(self, path):
    """
    Args:
      path (pathlib)      represent a single alarm file

    Returns:
      None                Error before invoking API Gwy endpoint
                              could be down network / ISP
      True                API Gwy endpoint successfully called
      False               Error invoking API Gwy endpoint
    """
    self.logger.info('entering: _call_alarm_api()')

    result      = None
    try:
      if(path.exists() and path.is_file()):
        file_name = path.name
        if(file_name):
          try:
            with path.open('r') as fd:
              credentials = self.boto3.get_credentials()
              auth        = self.AWS4Auth(credentials.access_key, 
                                          credentials.secret_key,
                                          self.config['region'], 
                                          'execute-api')
              endpoint    = self.config['end_point']
              method      = 'GET'
              headers     = {}
              body        = fd.read()
            
              try:
                response    = self.requests.request(method, endpoint, auth=auth, 
                                                    data=body, headers=headers)
                status_code = response.status_code
                #print(f'99 response: {response.text}')   #handy for debugging
                if(status_code == 200):
                  result = True
                else:
                  result = False
                  self.logger.error('9 API returned bad status code: ' +
                                    f'{str(status_code)}')
              except Exception as e:
                self.logger.error(f'10 Error invoking API. Exception: {e}')
          except Exception as e:
            self.logger.error(f'11 Couldnt open alarm file. Exception: {e}')
        else:
          self.logger.error('12 Could not extract name of file from pathlib' +
                            ' object.')
    except Exception as e:
      self.logger.error('13 Bad argument passed into _call_alarm_api(). ' +
                        f'Exception: {e}')
    return(result)


  def _clean_up_local_files(self):
    """
    Delete alarm files for which an API Gateway API call was made
    regardless of success or failure of the call.  They are no 
    longer needed as comms was successful or file is suspect and
    has been moved to special folder for upload
    """
    self.logger.info('entering: _clean_up_local_files()')

    for file_name in self.trans_good:
      file_path = (self.paths.get_path('alarms') + 
                   self.paths.divider + file_name)
      try:
        file = self.Path(file_path)
        file.unlink()
        self.clean_good.append(file_name)
      except Exception as e:
        self.clean_bad.append(file_name)
        self.logger.error(f'14 Could not delete file: {file_name}. ' +
                          f'Exception: {e}')

    for file_name in self.trans_bad:
      file_path = (self.paths.get_path('alarms') + 
                   self.paths.divider + file_name)
      try:
        file = self.Path(file_path)
        if(self._move_file(file, 'bad_comms')):
          self.move_good.append(file_name)
        else:
          self.logger.error(f'15 Could not move file: {file_name}')
          self.move_bad.append(file_name)
      except:
        self.move_bad.append(file_name)
        self.logger.error(f'16 Could not move file: {file_name}. ' +
                          f'Exception : {e}')


  def send(self):
    """
    Attempt to send all alarm flat files, in special directory, to the 
    AWS backend.  Alarms across all alarm types are housed in this
    single directory. Each file represents an individual alarm state. 
    Alarm files are named using a standard that encodes the alarm name,
    date, irrigation schedule id, and irrigation event sequence number.

    Returns:
      None          issue arose before attempt to send alarm data 
                      or no alarm files needed to be sent
      False         one or more communication attemps failed
      True          all communication attemptes succeeded
    """
    self.logger.info('entering: send()')

    self._reset_status()
    path = self.paths.get_path('alarms')
    if(path):
      try:
        directory = self.Path(path)
        if(directory.exists() and directory.is_dir()):
          for file in directory.iterdir():
            file_name = file.name
            if(self._file_valid(file)):
              result = self._call_alarm_api(file)
              if(result):
                self.trans_good.append(file_name)

              # API invoked w bad result, file suspect, do not retry 
              elif(result == False):
                self.logger.error('17 Error invoking API Gwy using file: ' +
                                  f'{file_name}')
                self.trans_bad.append(file_name)
              
              # API not invoked. Could be network / ISP down.  Leave file
              # for retry by subsequent alarms() object provided not too old
              else:
                self.logger.error('18 Execution did not reach API Gwy ' +
                                  'endpoint invocation')
                if(self._alarm_file_aged_past_limit(file_name)):
                  if(self._move_file(file,'bad_comms')):
                    self.move_good.append(file_name)
                  else:
                    self.move_bad.append(file_name)
                    self.logger.error('19 Could not move expired alarm file:' +
                                      f' {file_name}.')
            else:
              self.logger.error(f'20 Detected corrupt file: {file_name}')
              if(self._move_file(file,'corrupt_files')):
                self.move_good.append(file_name)
              else:
                self.move_bad.append(file_name)
                self.logger.error('21 Could not move corrupt file: ' +
                                  f'{file_name}.')
          self._clean_up_local_files()
        else:
          self.logger.error('22 Nonexistant directory path supplied for ' +
                            'alarms folder')
      except Exception as e:
        self.logger.error(f'23 Exception: {e}')
    else:
      self.logger.error('24 Couldnt retrieve path to alarms directory.')
  
    if(self.trans_bad or self.clean_bad or self.move_bad):
      self.comms_ok = False
    elif(self.trans_good):  # no errors and at least one good transmission
      self.comms_ok = True

    return(self.comms_ok)
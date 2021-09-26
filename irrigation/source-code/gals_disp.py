"""
Jaye Hicks 2021

Deployment check list: set gals_disp.env to 'debug' or 'prod'

Obligatory legal disclaimer:
  You are free to use this source code (this file and all other files 
  referenced in this file) "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER 
  EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. 
  THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THIS SOURCE CODE
  IS WITH YOU.  SHOULD THE SOURCE CODE PROVE DEFECTIVE, YOU ASSUME THE
  COST OF ALL NECESSARY SERVICING, REPAIR OR CORRECTION. See the GNU 
  GENERAL PUBLIC LICENSE Version 3, 29 June 2007 for more details.

All gals_disp messages waiting to be sent to the AWS backend exist in
the comms/gals_disp directory.  Each individual gallons dispensed
messages is contained in its a separate file.  These files are
augmented by dura_file functionality.  

When this module is invoked an attempt is made to transmit all 
gals_disp messages.  Issues / failures with the transmission of
a single message will not stop the attempt to transmit all messages.

The flat files holding gallons dispensed data are named following a 
strict naming standard. Specifically, the gals_disp's creation year
must follow the second'_' in the name, the creation month must follow
the third '_', and the creation day must follow the fourth '_'.  The
naming scheme is documented below.  For a single irrigation event 
there will be two types of gals_disp messages: 1 gals_disp messages
and non 1 gals_disp messages.  The former have the letter 'a' encoded
in their name and the latter have the letter 'b' encoded in their 
name.

'gals_disp_<year>_<month>_<day>_<sched_id>_<sequence>_<'a' / 'b'>.json'

Example: 'gals_disp_2021_10_31_99_1_a.json'  for a 1 gals disp message
         'gals_disp_2021_10_31_99_1_b.json'  partial / final gals disp

Example gals_disp contents:
{"sched_id": 99, "date": "2021-10-31", "sequence": 3, 
 "gallons": 9, "block": "block_x"}

An irrigation schedule will contain multiple individual irrigation 
events.  An irrigation event represents the real world action of 
distributing water to a vineyard block for a period of time.  For a 
single irrigaiton event an initial gals_disp messages will be sent to
the AWS backend, when the irrigation event commences, so that the AWS
backend becomes aware that an irrigation has actually started.  With
this information, the AWS backend will be able to avoid scheduleing
the block again (for the next 24 hours) in any future irrigation 
schedules that it generates (i.e., an irrigaiton big rule).

The 'a' / 'b' encoding in the gals_disp message flat file names avoids
information loss due to down communication links.  Without the 'a' /
/ 'b' encoding, if comms were down such that the initial 1 gals disp
message were still waiting to be sent to the AWS backend when the 
irrigation event concludes, the final gals_disp message would be
written locally and that woudl overwrite the initial 1 gallon gals
disp message.

Gallons dispensed messages are sent to the AWS backend via the IoT
Core service.  Updates are made to the IoT Core shadow document that
corresponds to the vineyard block being irrigated.  This process
breaks down into two separate, independent steps.  First, data 
is placed on an mqtt queue.  Second, the IoT Core service processes
the queue message and invokes a callback function with the results 
of the shadow document update.  The callback function that the IoT
Core service invokes is defined in this module.  It is specified by
this module when it places data on the queue.  As this module cant
wait indefinitely for IoT to invoke the callback function after it 
places data on the queue, it will wait a limited amount of time 
before moving on with processing. 

It is possible that this module will exit before accept / reject 
results come back, from AWS IoT Core, for each individual shadow 
document update made by this module.  An argument can be passed to 
this module to instruct it to either wait (i.e., a fixed amount of 
time) or not wait for the results of all of the IoT shadow document 
updates that it placed on the queue.  If this module exits before 
learning the result for an individual IoT shadow document update(s), 
the IoT shadow document update(s) will be transmitted again (i.e., the
gals disp message will be sent again) the next time this module is
executed.  This does not pose an issue as the processing of gals_disp
 messages on the AWS backend is idempotent.
"""
class gals_disp():
  import logging
  import json
  import time
  from   AWSIoTPythonSDK.MQTTLib  import AWSIoTMQTTShadowClient
  from   pathlib                  import Path
  from   datetime                 import datetime, timedelta

  import dura_file
  import lv_paths


  def __init__(self):
    """
    """
    self.logger = self.logging.getLogger(__name__)

    self.logger.info('entering: __init__()')
    self.env            = 'debug' # set to 'debug' or 'prod'
    self.df             = self.dura_file.dura_file()
    self.paths          = self.lv_paths.lv_paths()
    self.device_shadows = {}

    if(self.env   == 'debug'):
      self.config = {
        'max_age_days' : 2,  # upload files aged past threshold to S3
        'wait_retries' : 4,
        'wait_secs'    : 10,
        'region'       : 'us-east-1',
        'blocks'       : ['block_a','block_b','block_c','block_d','block_e',
                          'block_f','block_g','block_x'],
        'shadow_sec' : {
          'block_a' : '1111111111111111111111111111111111111111111111111111111111111111',
          'block_b' : '2222222222222222222222222222222222222222222222222222222222222222',
          'block_c' : '3333333333333333333333333333333333333333333333333333333333333333',
          'block_d' : '4444444444444444444444444444444444444444444444444444444444444444',
          'block_e' : '5555555555555555555555555555555555555555555555555555555555555555',
          'block_f' : '6666666666666666666666666666666666666666666666666666666666666666',
          'block_g' : '7777777777777777777777777777777777777777777777777777777777777777',
          'block_x' : '8888888888888888888888888888888888888888888888888888888888888888'}, #for debug
        'mqtt_values': {
          'host_name' : 
            'abcdefghijklmon-ats.iot.us-east-1.amazonaws.com',
          'mqtt_port' : 8883},

        # AWSIoTPythonSDK.MQTTLib throws this exception for network issues:
        #    '[Errno 11001] getaddrinfo failed'
        'comms_down_msg' : 'getaddrinfo failed'}
    else: 
      self.config = {
        'max_age_days' : 2,  # upload files aged past threshold to S3
        'wait_retries' : 4,
        'wait_secs'    : 10,
        'region'       : 'us-east-1',
        'blocks'       : ['block_a','block_b','block_c','block_d','block_e',
                          'block_f','block_g'],
        'shadow_sec' : {
          'block_a' : '1111111111111111111111111111111111111111111111111111111111111111',
          'block_b' : '2222222222222222222222222222222222222222222222222222222222222222',
          'block_c' : '3333333333333333333333333333333333333333333333333333333333333333',
          'block_d' : '4444444444444444444444444444444444444444444444444444444444444444',
          'block_e' : '5555555555555555555555555555555555555555555555555555555555555555',
          'block_f' : '6666666666666666666666666666666666666666666666666666666666666666',
          'block_g' : '7777777777777777777777777777777777777777777777777777777777777777'},
        'mqtt_values': {
          'host_name' : 
            'abcdefghijklmon-ats.iot.us-east-1.amazonaws.com',
          'mqtt_port' : 8883},
          
        # AWSIoTPythonSDK.MQTTLib throws this exception for network issues:
        #    '[Errno 11001] getaddrinfo failed'
        'comms_down_msg' : 'getaddrinfo failed'}

    self._reset_status()


  def _gals_disp_file_aged_past_limit(self, file_name):
    """
    Example gals dispensed flat file:
    'gals_disp_<year>_<month>_<day>_<sched_id>_<sequence>_<'a' / 'b'>.json'

    Args
      file_name(str)      name of gals_disp file.  Must follow strict
                            standard.  See comments top of module
    Returns
      True                the gals_disp files age is beyond threshold
      False               the gals_disp files age is not beyond threshold
    """
    self.logger.info('entering: _gals_disp_file_aged_past_limit()')

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
      date_time_limit += self.timedelta(days=self.config['max_age_days'])
  
      if(date_time_limit < now_date_time):
        result = True
    except Exception as e:
      self.logger.error(f'1 Exception: {e}')
    return(result)


  def _reset_status(self):
    """
    Clear out communication details from last transmission of data in
    prepration of a new communication transmission.
    """
    self.logger.info('entering: _reset_status()')

    self.comms_ok   = None
    self.wait       = False
    self.q_messages = {}   # all messages placed on queue for all shadows
    self.trans_good = []   # all files successfully placed on queue
    self.trans_bad  = []   # all files that couldnt be placed on queue
    self.rec_good   = []   # all files resulting in shadow update success
    self.rec_bad    = []   # all files resulting in shadow update failure
    self.clean_good = []   # all files successfully deleted
    self.clean_bad  = []   # all files that couldnt be deleted
    self.move_good  = []   # all suspect files successfully moved
    self.move_bad   = []   # all suspect files that couldnt be moved

 
  def good_comms(self):
    self.logger.info('entering: good_comms()')    
    return(self.comms_ok)


  def good_trans_cnt(self):
    self.logger.info('entering: good_trans_cnt()')    
    return(len(self.trans_good))


  def bad_trans_cnt(self):
    self.logger.info('entering: bad_trans_cnt()')    
    return(len(self.trans_bad))


  def good_rec_cnt(self):
    self.logger.info('entering: good_rec_cnt()')    
    return(len(self.rec_good))


  def bad_rec_cnt(self):
    self.logger.info('entering: bad_rec_cnt()')    
    return(len(self.rec_bad))


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


  def _get_device_shadow_client(self, block):
    """
    This function will either return a preexisting shadow document
    client for the specified block or create one for it.  Reuse of
    shadow document clients is enabled due to the 'True' flag in the 
    createShadowHandlerWithName() call.

    Args:
      block(str)       the block which received the irrigation

    Returns:
      None             Error before attempted creation of shadow client
      False            Shadow client creation attempt failed
      device_shadow    Shadow client successfully created / retrieved
    """
    self.logger.info('entering: _get_device_shadow_client()')

    a_device_shadow = None
    if(block and (type(block) == str)):
      block = (block.strip()).lower()
      if(block in self.config['blocks']):
        if(block in self.device_shadows):
          a_device_shadow = self.device_shadows[block]
        else:
          a_client = self.AWSIoTMQTTShadowClient(block)
          a_client.configureEndpoint(self.config['mqtt_values']['host_name'],
                                     self.config['mqtt_values']['mqtt_port'])
      
          dir_prefix   = (self.paths.get_path('shadow_sec') +
                          self.paths.divider)
          block_prefix = dir_prefix + self.config['shadow_sec'][block]
          a_client.configureCredentials(dir_prefix + 'Amazon_root_CA_1.pem',
                                        block_prefix +'-private.pem.key',
                                        block_prefix + '-certificate.pem.crt')

          a_client.configureConnectDisconnectTimeout(10)
          a_client.configureMQTTOperationTimeout(5)
          
          if(not a_client.connect()):
            self.logger.error(f'2 Couldnt connect to block: {block} ' + 
                               'shadow document')
          else:
            a_device_shadow = a_client.createShadowHandlerWithName(block, True)
            self.device_shadows[block] = a_device_shadow
      else:
        self.logger.error(f'3 Block specification: {block} is invalid.')

    return(a_device_shadow)
  
  
  def _update_shadow_document(self, block, update, filename):
    """
    This function updates the IoT Core shadow document for a given 
    vineyard block.  This happens using two distinct processes.  
    The process running this Python module synchronously places the
    update data on a mqtt queue dedicated to the given block's
    IoT Core shadow document using teh shadowUpdate() call.  When the
    call to place data on the queue returns, the process continues 
    with processing without waiting on feedback for the ultimate 
    success / failure of updating the shadow document.

    The SDK that this modules uses (AWSIoTPythonSDK.MQTTLib) takes
    over after the call to shadowUpdate() with a separate asynchronous 
    process thread that communicates with the AWS backend.  The AWS 
    backend will eventually provide a success / failure for the shadow
    document update request. It will communicate the result by calling
    the callback function that was designated as an argument in the 
    syncrhous call to shadowUpdate() made by this function.
    
    Note, the local SDK will call the callback function on its own if 
    the communications link to the AWS backend is down.
    
    Args:
      block(str)               the block that received the irrigaiton
      update(Python dict)      the irrigation data

    Returns
      None    issue before call to shadowUpdate()
      True    successfully called shadowUpdate()
      False   issue with call to shadowUpdate()
    """
    self.logger.info('entering: _update_shadow_document()')

    result = None
    if(block and update and (type(block) == str)):
      block = (block.strip()).lower()
      if(block in self.config['blocks']):
        a_device_shadow = self._get_device_shadow_client(block)
        if(a_device_shadow):
          update_string = '{"state":{"reported":'
          update_string += self.json.dumps(update)
          update_string += '}}'

          try:
            # synchronously place shadow update on mqtt queue. The
            # update to the IoT shadow document occurs asynchronously
            # and '_my_shadow_update_callback()' as specified below
            # will be called by IoT Core, with success / failure of
            # the shadow update, whenever IoT Core decides to do so.
            token = a_device_shadow.shadowUpdate(update_string, 
                                        self._my_shadow_update_callback, 5)
            self.q_messages[str(token)] = filename
            result = True
          except Exception as e:
            result = False
            self.logger.error(f'4 Exception updating shadow for: {block}. ' +
                              f'Exception: {e}')
        else:
          self.logger.error('5 Couldnt obtain shadow client for block: ' +
                            f'{block}')
      else:
        self.logger.error(f'6 Block specification: {block} is invalid.')
    else:
      self.logger.error('7 Bad parameter(s) sent to _update_shadow_document')
    return(result)


  def _my_shadow_update_callback(self, payload, response_status, token):
    """
    This function will be called by one of two entities.  It will be
    called by the AWS backend (IoT service) if a communicaiton link is
    up to the AWS backend.  In this case it will return success / fail
    status as well as the data that it originally recieved for the
    shadow document update.  This original data will allow this
    function to contruct the name of local file that was used to store
    the gallons dispensed data send throw the shadow document update.
    
    If this function is not called by teh AWS backend it will be called
    by a locally running process (AWSIoTPythonSDK.MQTTLib) and this
    will be due to an error condition (e.g., comms link to AWS backend
    is down).

    Args:
      payload(str)           data sent in for update plus extra stuff 
      response_status(str)   can be 'accepted' or 'rejected'
      token(str)             client token; also contained in payload 
    """
    self.logger.info('entering: _my_shadow_update_callback()')

    token = str(token).strip()
    if(token in self.q_messages):
      if(payload):
        try:
          data = (self.json.loads(payload))['state']['reported']
          block = data['block']
        except Exception as e:
          self.logger.error('8 recieved invalid payload from IoT Core. ' +
                            f'Exception: {e}')
        if(response_status == 'accepted'): 
          self.rec_good.append(self.q_messages[token])
        else:
          self.rec_bad.append(self.q_messages[token])
          self.logger.error('9 Failure updating shadow document for block:' +
                            f' {block}')
      else:
        self.logger.error('10 Empty payload received from IoT Core service.')
        if(response_status == 'accepted'): 
          self.rec_good.append(self.q_messages[token])
        else:
          self.rec_bad.append(self.q_messages[token])
          self.logger.error('40 Failure updating shadow document for file: ' +
                           f'{self.q_messages[token]}')
    else:
      self.logger.error('11 No record of sending this gals_disp message. Cant' +
                        'find and automatically delete local gals_disp file.')


  def _data_valid(self, path):
    """
    Gallons dispensed files are augmented by the class dura_file()

    Args:
      path       pathlib object to gallons dispensed file

    Returns:
      None       issue occured before file could be opened
      False      data contained in file missing or malformed
      True       data contained in file is valid
    """
    self.logger.info('entering: _data_valid()')

    result = None
    if(path):
      file = self.Path(path)
      if(file.exists() and file.is_file()):
        try:
          with path.open('r') as fd:
            contents = fd.read()
            try:
              data = (self.json.loads(contents))['data']
              if(((type(data['date']) == str) and (len(data['date']) == 10)) and
                 (data['sched_id'] >= 0) and
                 (data['sequence'] > 0) and 
                 (data['block'] in self.config['blocks']) and
                 (data['gallons'] > 0) and
                 (data['flow'] > 0)):
                result = True
              else:
                result = False
                self.logger.error('12 Malformed / missing data in:' +
                                  f' {file.name}')
            except Exception as e:
              result = False
              self.logger.error(f'13 Invalide JSON in: {file.name}. ' +
                                f'Exception: {e}')
        except Exception as e:
          self.logger.error(f'14 Could not open file. Exception: {e}')
      else:
        self.logger.error(f'15 Item: {path} does not exist or is not a file.')
    else:
      self.logger.error('16 Bad parameter sent to _data_valid()')
    return(result)


  def _file_valid(self, path):
    """
    Uses dura_file() class functionality to determine validity
  
    Args:
      path(pathlib)   pathlib object represents single gals_disp file

    Returns:
      None            Could not open file; damaged or nonexistant
      True            The file's contents are valid
      False           The file's contents are invalid
    """
    self.logger.info('entering: _file_valid()')

    result = None
    if(path):
      file = self.Path(path)
      if(file.exists() and file.is_file()):
        try:
          with path.open('r') as fd:
            contents = fd.read()
            if(self.df.check_object(json_object=contents)):
              result = True
            else:
              result = False
        except Exception as e:
          self.logger.error(f'17 Could not open file. Exception: {e}')
      else:
        self.logger.error(f'18 Item: {path} does not exist or is not a file.')
    return(result)


  def _move_file(self, file, target):
    """
    Move a problematic file (i.e., corrupt or cause comms failure) from
    one file system directory to another.
 
    Args:
      file(pathlib)     pathlib object represents corrupted file
      targert(str)      specifies destination folder. used to
                          index into lv_paths object.  Will be
                          'corrupt_files' or 'bad_comms'

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
            self.logger.error(f'19 Could not move file: {file_name} to ' +
                              f'{target}. Exception: {e}')
            result = False
        else:
          self.logger.error('20 Could not extract file name from pathlib' +
                            ' object')
      except Exception as e:
        self.logger.error('21 Bad argument passed to _move_file(). ' +
                          f'Exception: {e}')
    return(result)
  

  def _set_wait_mode(self, wait):
    """
    """
    self.logger.info('entering: _set_wait_mode()')

    if(wait and (type(wait) == str)):
      wait = wait.strip().lower()
      if(wait == 'true'):
        self.wait = True
      else:
        self.wait = False
    else:
      self.wait = False

 
  def _wait_for_response(self):
    """
    Delay for a set amount of time waiting for IoT Core to provide the
    status for all of the IoT Core shadow document updates that were
    made by this module.
    """
    self.logger.info('entering: _wait_for_response()')

    retries = self.config['wait_retries']
    
    while(retries > 0):
      if(len(self.trans_good) <= (len(self.rec_good) + len(self.rec_bad))):
        retries = 0
      else:
        retries -= 1
        self.time.sleep(self.config['wait_secs'])


  def _send_gals_disp(self, path):
    """
    Assumption that the argument has been validated (i.e., dura_file
    check for completness / corruption) and custom check on the gals 
    disp data prior to calling this function.

    Args:
      path (pathlib)      represent a single gals_disp file

    Returns:
      None                issue before updating shadow
      True                successfuly called shadowUpdate
      False               issue calling shadowUpdate
    """
    self.logger.info('entering: _send_gals_disp()')

    result = None
    if(path):
      try:
        name = path.name
        if(name):
          try:
            with path.open('r') as fd:
              try:
                update = (self.json.loads(fd.read()))['data']
                return_code = self._update_shadow_document(update['block'], 
                                                           update, name)
                if(return_code):
                  self.trans_good.append(name)
                elif(return_code == None):
                  if(self._gals_disp_file_aged_past_limit(name)):
                    if(self._move_file(path,'bad_comms')):
                      self.move_good.append(name)
                    else:
                      self.move_bad.append(name)
                      self.logger.error('24 Could not move expired gals_disp' +
                                        f' file: {name}.')
                  #else (leave file, subsequent call to this mod will retry)
                else:
                  self.trans_bad.append(name)

              except Exception as e:
                exception_text = str(e)
                if(self.config['comms_down_msg'] in exception_text):
                  if(self._gals_disp_file_aged_past_limit(name)):
                    if(self._move_file(path,'bad_comms')):
                      self.move_good.append(name)
                    else:
                      self.move_bad.append(name)
                      self.logger.error('27 Could not move expired gals_disp' +
                                        f' file: {name}.')
                  #else (leave file, subsequent call to this mod will retry)
                else:
                  self.logger.error(f'28 Exception: {e}')
                  self.trans_bad.append(name)
          except Exception as e:
            self.logger.error(f'29 Couldnt open gals_disp file. Exception: {e}')
            self.trans_bad.append(name)
        else:
          self.logger.error('30 Could not extract name of file from pathlib' +
                            ' object.')
          self.trans_bad.append('bad_name')
      except Exception as e:
        self.logger.error(f'31 Exception: {e}')
    else:
      self.logger.error('32 Null argument passed to _send_gals_disp')
    return(result)

  
  def _clean_up_local_files(self):
    """
    Delete gals_disp message files that were successfully placed on the
    queue and for which 'success 'feedback was received from IoT Core.  
    Move files that could not be placed onto queue or that were
    successfully placed on queue and for which 'failure' feedback was 
    received from IoT Core.  Files that are moved go to a special
    folder that is accessed by a separate Python module that uploades
    them to S3 for future forensic analysis.
    """
    self.logger.info('entering: _clean_up_local_files()')

    # attempt to place gals_disp data on q failed
    for index in range(len(self.trans_bad)):
      file_name = self.trans_bad[index]
      file_path = (self.paths.get_path('gals_disp') + 
                   self.paths.divider + file_name)
      try:
        file = self.Path(file_path)
        if(self._move_file(file, 'bad_comms')):
          self.move_good.append(file_name)
        else:
          self.move_bad.append(file_name)
      except Exception as e:
        self.logger.error(f'33 Could not move file: {file_path} to bad comms' +
                          f' folder. Exception: {e}')

    # IoT responsed with success shadow document update
    for index in range(len(self.rec_good)):
      file_name = self.rec_good[index]
      file_path = (self.paths.get_path('gals_disp') + 
                    self.paths.divider + file_name)
      try:
        self.Path(file_path).unlink()
        self.clean_good.append(file_name)
      except Exception as e:
        self.clean_bad.append(file_name)
        self.logger.error(f'34 Could not delete file: {file_name}. ' +
                          f'Exception: {e}')
  
    # IoT responded with failed shadow document update
    for index in range(len(self.rec_bad)):
      file_name = self.rec_bad[index]
      file_path = (self.paths.get_path('gals_disp') + 
                    self.paths.divider + file_name)
      try:
        file = self.Path(file_path)
        if(self._move_file(file, 'bad_comms')):
          self.move_good.append(file_name)
        else:
          self.move_bad.append(file_name)
      except:
        self.logger.error(f'35 Could not move file: {file_path} to bad ' +
                          f'comms folder. Exception: {e}')
    """
    NOTE: gals_disp files in trans_good[] but not in rec_good[] or 
    rec_bad[] will be left in place so that future exeuctions of this 
    module can access them and attempt to transmit them again.  If a 
    gal_disp file remains untransmitted for too long its age will 
    exceed a threshold and it will be moved to a special directory for 
    problematic files that get uploaded to an S3 bucket for future 
    forensic analysis
    """


  def send(self, wait='False'):
    """
    It is possible for this module to exit without receiving feedback
    on all of the IoT Core shadow document updates.  In such a case
    the _clean_up_local_files() function will not delete the gals_disp
    flat files for those unknown shadow document updates.  At the next 
    execution of this module those files will be sent again.  This does
    not pose an issue as the gals_disp data message processing on the
    AWS backend was desigtned to be idempotent relative to gals_disp
    messages.  
    
    Args:
      wait(str)    wait for IoT Core service to report status on the
                     IoT Core shadow document update
    Returns:
      None        issue occured before communication attempt or no
                    gals_disp files existed to be sent
      True        all communication attempts succeeded
      False       one or more communicaiton attempt failed
    """
    self.logger.info('entering: send()')

    self._reset_status()
    self._set_wait_mode(wait)
    
    path = self.paths.get_path('gals_disp')
    if(path):
      try:
        directory = self.Path(path)
        if(directory.exists() and directory.is_dir()):
          for item in directory.iterdir():
            if((self._file_valid(item)) and 
               (self._data_valid(item))):
              self._send_gals_disp(item)
            else:
              self.logger.error('36 Corrupt file or gals_disp data:' +
                                f' {item.name}')
              if(self._move_file(item, 'corrupt_files')):
                self.move_good.append(item.name)
              else:
                self.move_bad.append(item.name)
          
          if(len(self.trans_good) > (len(self.rec_good) + len(self.rec_bad))):
            if(self.wait):
              self._wait_for_response()
          
          self._clean_up_local_files()
        else:
          self.logger.error('37 Nonexistant directory path supplied for ' +
                            ' gals_disp folder')
      except Exception as e:
        self.logger.error(f'38 Exception: {e}')
    else:
      self.logger.error('39 Couldnt retrieve path to gals_disp folder.')

    if(self.trans_bad or self.rec_bad or self.clean_bad or self.move_bad):
      self.comms_ok = False
    elif(self.trans_good): # no errors and at least 1 good transmission
      self.comms_ok = True

    return(self.comms_ok)
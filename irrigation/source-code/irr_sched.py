"""
Jaye Hicks 2021

Deployment check list: set irr_sched.env to 'debug' or 'prod'

Obligatory legal disclaimer:
  You are free to use this source code (this file and all other files 
  referenced in this file) "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER 
  EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. 
  THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THIS SOURCE CODE
  IS WITH YOU.  SHOULD THE SOURCE CODE PROVE DEFECTIVE, YOU ASSUME THE
  COST OF ALL NECESSARY SERVICING, REPAIR OR CORRECTION. See the GNU 
  GENERAL PUBLIC LICENSE Version 3, 29 June 2007 for more details.

Objects of type irr_sched() can perform a number of operations in 
support of irrigation schedules.
-For irrigation schedules
  - retrieve the most current irrigation schedule from the AWS backend
  - validate that a irrigation schedule file is complete and accurate
  - place a schedule into force / use
     - removing the one in place; possibly stopping an irrigation event
     - possibly stopping an existing irrigation event
       - possibly through graceful shut down
       - possibly through hard take down
  - inform the AWS backend what schedule is currently in force on PI 4
-For irrigation events
  - return irr ev that is curently being executed
  - return irr ev that should be currently executed
  - return all available details for an irr ev

NOTE: the identifier 'boto3' is overidden in the class irr_sched()

irr_sched() requests the most current irrigation schedule by invoking
an API Gateway API.  In this call the PI 4 supplies the id of the 
irrigation schedule that is currently in force on the PI 4 as well as
the PI 4's setting for current date / time.  The AWS backend will
supply either a newer, more up-to-date, irrigation schedule or the
message 'no-newer-sched-available'

When the PI 4 receives a new irrigation schedule it will validate the
schedule and put it into force only after successful validation.

irr_sched(), as with all other modules, relies on adherance to a strict
naming standard for the flat files that it uses.  The irrigation
schedule naming standard:

<year>_<month>_<day)_<'intel' or 'fixed'>_<sched id>.json

There are two types of irrigation schedules.
-Fixed schedules
  -have a schedule id that is between 1 and 20
  -never expire as they schedule irrigation based on days of week
-Intelligent schedules
  -have a schedule id > 30
  -expire if schedule dates occur in the past
  -provide irrigation scheduling for three consecutive calendar days

Example intelligent schedule
{"whatami": "irrigation-schedule-intelligent", 
 "created_ts": 1234567890, 
 "created_date": "2021-10-31", 
 "id": 31, 
 "day1": {"date": "2021-07-31", 
          "events": [{"sequence": 1, 
                      "block": "a", 
                      "start": "8:00", 
                      "duration": "4:00", 
                      "exp_flow": 15, 
                      "over_flow_tol": 10, 
                      "under_flow_tol": 25}, 
                     {"sequence": 2, 
                      "block": "b", 
                      "start": "13:30", 
                      "duration": "4:00", 
                      "exp_flow": 15, 
                      "over_flow_tol": 10, 
                      "under_flow_tol": 25}]}, 
"day2": {"date": "2021-08-01", 
         "events": "no-irrigation"}, 
"day3": {"date": "2021-08-02", 
         "events": [{"sequence": 1, 
                     "block": "e", 
                     "start": "8:00", 
                     "duration": "4:00", 
                     "exp_flow": 15, 
                     "over_flow_tol": 10, 
                     "under_flow_tol": 25}, 
                    {"sequence": 2, 
                     "block": "f", 
                     "start": "13:30", 
                     "duration": "4:00", 
                     "exp_flow": 10, 
                     "over_flow_tol": 10, 
                     "under_flow_tol": 25}]}}

"""
class irr_sched():
  import logging
  import time
  from   datetime          import datetime, timedelta
  import json
  import boto3
  import boto3.session
  import requests
  from   requests_aws4auth import AWS4Auth
  from   pathlib           import Path

  import dura_file
  import lv_paths
  import process_cntrl


  def __init__(self):
    """
    NOTE: identifier 'boto3' overidden to refer to a boto3 session

    The fixed schedule with id == 0 is reserved as the default
    irrigation schedule.
    """
    self.logger = self.logging.getLogger(__name__)

    self.logger.info('entering: __init__()')
    self.df            = self.dura_file.dura_file()
    self.paths         = self.lv_paths.lv_paths()
    self.proc_cntrl    = self.process_cntrl.process_cntrl()
    self.default_sched = {"whatami": "irrigation-schedule-fixed", 
                          "created_ts": 1626847260, 
                          "created_date": "2021-07-21", 
                          "id": 0, 
                          "name" : "default", 
                          "sun" : "no-irrigation",
                          "mon" : "no-irrigation",
                          "tue" : "no-irrigation",
                          "wed" : "no-irrigation",
                          "thu" : "no-irrigation",
                          "fri" : "no-irrigation",
                          "sat" : "no-irrigation"}

    self.config                   = {}
    self.config['env']            = 'debug'  #set to 'debug' or 'prod'
    self.config['blocks']         = ['a','b','c','d','e','f','g']
    self.config['irr_ev_hrs_max'] = 4
    self.config['fixed_days']     = ['mon','tue','wed','thu','fri','sat','sun']
    self.config['intel_days']     = ['day1','day2','day3']

    if(self.config['env'] == 'debug'):
      self.config['get_sched']= (
        'https://abcdefghij.execute-api.us-east-1.amazonaws.com/prod') 
      self.config['region'] = 'us-east-1'

      #overide identifier 'boto3' (session using IAM credentials below)
      try:
        self.boto3 = self.boto3.Session(
          aws_access_key_id     = '12345678901234567890', 
          aws_secret_access_key = '123456789012345678901234567890123456789012',
          region_name           = self.config['region'])
      except Exception as e:
        self.logger.error(f'1 Could not create boto3 session.  Exception: {e}')
    else: 
      self.config['get_sched'] = ( 
        'https://abcdefghij.execute-api.us-east-1.amazonaws.com/prod') 
      self.config['region'] = 'us-east-1'

      #overide identifier 'boto3' (session using AWS CLI default profile)
      try:
        self.boto3 = self.boto3.session.Session()
      except Exception as e:
        self.logger.error(f'2 Could not create boto3 session. Exception: {e}')


  def _number_files_in_dir(self, path):
    """
    Return the number of files contained in the specified directory.  
    Counts only files, not direoctories, symbolic links, etc.  See
    Caution in function _describe_sched() 

    Args:
      path(str)  fully qualified path to directory

    Retrns:
      None       error occured before attempt to count files in dir
      False      error occured during file counting
      int        number of files in directory
    """
    self.logger.info('entering: _number_files_in_dir()')

    file_count = None
    if(path):
      try:
        directory  = self.Path(path)
        dir_name   = directory.name
        file_count = 0
        for item in directory.iterdir():
          if(item.is_file()):
            file_count += 1
      except Exception as e:
        file_count = False
        self.logger.error(f'3 Issue accessing dir: {dir_name} Exception: {e}')
    else:
      self.logger.error('4 Bad argument passed to _number_files_in_dir')
    return(file_count)


  def _software_reset(self):
    """
    Called in extreme cases where it is unclear how to proceed and the
    best course forward is to clear everythign out and start over.
    Performs a series of disjointed tasks, continuing through the list
    of all tasks regardless of success or failure of any individual task.

    Returns:
      True       reset successful
      False      issue encountered during reset attempt
    """
    self.logger.info('entering: _software_reset()')

    result = True
    try:
      # stop any OS process currently running / managing an irrigation event
      if(self.proc_cntrl.clear_entire_register() == False): # True or None are ok
        self.logger.error('5 couldnt stop all irr ev OS processes and or ' +
                          'delete register file')
        result = False

      # clear out all control files
      path = self.paths.get_path('cur_irr_sched')
      directory = self.Path(path)
      if(directory.is_dir()):
        for item in directory.iterdir():
          if(item.is_file()):
            item.unlink()
      else:
        self.logger.error('6 Couldnt access "cur_irr_sched" directory')
        result = False

      path = self.paths.get_path('new_irr_sched')
      directory = self.Path(path)
      if(directory.is_dir()):
        for item in directory.iterdir():
          if(item.is_file()):
            item.unlink()
      else:
        self.logger.error('7 Couldnt access "new_irr_sched" directory')
        result = False

      path = self.paths.get_path('irr_event')
      directory = self.Path(path)
      if(directory.is_dir()):
        for item in directory.iterdir():
          if(item.is_file()):
            item.unlink()
      else:
        self.logger.error('8 Couldnt access "irr_event" directory')
        result = False

      # if gallons dispensed data exits, preserve it
      path = self.paths.get_path('irr_ev_in_progress')
      directory = self.Path(path)
      if(directory.is_dir()):
        for item in directory.iterdir():
          if(item.is_file()):
            if((item.name.split('_')[0] == 'pulse') and
              (item.name.split('_')[1] == 'count')):
              if(not self._move_file(item, 'orphans')):
                result = False
                self.logger.error('9 Couldnt move pulse count file: ' +
                                  f'{item.name}')
            else:
              item.unlink()
      else:
        self.logger.error('10 Couldnt access "irr_ev_in_progress" directory')
        result = False

      # put the default irrigation schedule in force
      now_date_time = self.datetime.today() 
      now_date_str  = ( str(now_date_time.year) + '-' +
                        str(now_date_time.month).zfill(2) + '-' +
                        str(now_date_time.day).zfill(2) )
      path = self.paths.get_path('cur_irr_sched')
      if(path):
        file_name = path + self.paths.divider + now_date_str + '_fixed_0.json'
        if(not self.df.write_data(file_name, self.default_sched)):
          self.logger.error('11 could not write default irr sched flat file')
          result = False
      else:
        result = False
        self.logger.error('12 Couldnt access "cur_irr_sched" directory')
    except Exception as e:
      result = False
      self.logger.error(f'13 Exception: {e}')
    return(result)


  def _clean_a_schedule_dir(self, path):
    """
    Irrigation schedule directories should only contain a single
    irrigation schedule file.  Access an irrigation schedule 
    directory (i.e., new or current) and remove any files that don't 
    belong there.  If a fixed schedule coexists with intel schedules 
    then leave the fixed schedule.  If no fixed sched exists and 
    multiple intel schedules exist then leave the newest intel sched.  
    If multiple fixed scheds exist then punt - call the software 
    reset function.

    Only call this function when no OS process is active and 
    managing an irrigation event because it can remove an 
    irrigation file that might be the basis for the running OS
    process engaged in irrigating a vineyard block.

     Args:
      path(str)       the fully qualified directory path to clean

    Returns:
      None            issue occured before attempt to clean directory
      False           failed in attempt to clean directory
      True            successfully cleaned direcotry
    """
    self.logger.info('entering: _clean_a_schedule_dir()')

    result       = None
    fixed_scheds = []
    intel_scheds = []

    if(path and (type(path) == str)):
      try:
        directory = self.Path(path)
        if(directory.is_dir()):
          for item in directory.iterdir():
            if(item.is_file()):
              full_path = path + self.paths.divider + item.name
              if(not self.df.check_object(file_name=full_path)):
                if(not self._move_file(item, 'corrupt_files')):
                  self.logger.error('14 couldnt move corrupt file: ' +
                                    f'{item.name}')
              else:
                item_type = item.name.split('_')[3]
                if(item_type == 'fixed'):
                  fixed_scheds.append(item.name)
                elif(item_type == 'intel'):
                  intel_scheds.append(item.name)
                else:
                  item.unlink()       # not a valid sched file name so delete it
        if(len(fixed_scheds) > 1):
          result = self._software_reset()    # punt as we dont know which has precedence
        elif(len(fixed_scheds) == 1):
          for sched in intel_scheds:
              full_path = path + self.paths.divider + sched
              item = self.Path(full_path)
              item.unlink() 
          result = True
        elif(len(intel_scheds) > 1):
          newest_intel_sched = intel_scheds[0]
          for sched in intel_scheds:
            if(int(newest_intel_sched.split('_')[4].split('.')[0]) < 
               int(sched.split('_')[4].split('.')[0])):
              newest_intel_sched = sched
          for sched in intel_scheds:
            if(sched != newest_intel_sched):
              full_path = path + self.paths.divider + sched
              item = self.Path(full_path)
              item.unlink() 
          result = True
        else:
          result = True
      except Exception as e:
        result = False
        self.logger.error(f'15 Exception: {e}')
    return(result)


  def _clear_directory(self, path):
    """
    Delete any files that might exist in the specified direcotry.
    If something else exists in directory (e.g., symbolic link,
    directory) it will not be deleted

    Args:
      path(str)        fully qualifed path of the directory to clear

    Returns:
      None             issue occured before attempt to clear out dir
      True             succeeded in clearing directory
      False            exception occured or could not clear directory
    """
    self.logger.info('entering: _clear_directory()')

    result = None
    if(path and (type(path) == str)):
      try:
        directory = self.Path(path)
        dir_name = directory.name
        if(directory.is_dir()):
          result = True
          for item in directory.iterdir():
            if(item.is_file()):
              item.unlink()
      except Exception as e:
        result = False
        self.logger.error(f'16 Could not clear a directory: {dir_name}. ' +
                          f'Exception: {e}')
    else:
      self.logger.error('17 Bad argument sent to _clear_directory()')
    return(result)


  def _move_file(self, file, target):
    """
    Move a single file from one directory to another directory
 
    Args:
      file(pathlib)     pathlib object representing file to move
      targert(str)      string arg passed to lv_paths to get dest dir
  
    Returns:
      None           issue before attempt to move file
      False          failed to move file to target direcotry
      True           successfully moved file to target directory
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
            self.logger.error(f'18 Could not move file: {file_name} to ' +
                              f'{target}. Exception: {e}')
            result = False
        else:
          self.logger.error('19 Could not extract file name from pathlib '
                            'object')
      except Exception as e:
        self.logger.error('20 Bad argument passed to _move_file(). ' +
                          f'Exceptions {e}')
    return(result)


  def _irr_ev_good(self, event):
    """
    Validate an irrigation event

    Args:
      an irrigation event

    Returns:
      None         issue before check could be made
      False        one or more problems with the irr ev
      True         irr ev checks out
    """
    self.logger.info('entering: _irr_ev_good()')

    result = None
    if(event):
      try:
        if((type(event['sequence']) != int) or
           (not event['sequence'] > 0)):
          result = False
          self.logger.error('21 Invalid value for "sequence"')

        if((type(event['block']) != str) or
           (not event['block'] in self.config['blocks'])):
          result = False
          self.logger.error('22 Invalid value for "block"')

        if((type(event['start']) != str) or
           (not (0 <= int(event['start'].split(':')[0]) < 24)) or
           (not (0 <= int(event['start'].split(':')[1]) < 60))):
          result = False
          self.logger.error('23 Invalid value for "start"')

        if((type(event['duration']) != str) or
           (not (0 <= int(event['duration'].split(':')[0]) <= 
             self.config['irr_ev_hrs_max'])) or
           (not (0 <= int(event['duration'].split(':')[1]) < 60))):
          result = False
          self.logger.error('24 Invalid value for "duration"')

        if((type(event['exp_flow']) != int) or
           (not event['exp_flow'] > 0)):
          result = False
          self.logger.error('25 Invalid value for "exp_flow"')
        
        if((type(event['over_flow_tol']) != int) or
           (not event['over_flow_tol'] > 0)):
          result = False
          self.logger.error('26 Invalid value for "over_flow_tol"')

        if((type(event['under_flow_tol']) != int) or
           (not event['under_flow_tol'] > 0)):
          result = False
          self.logger.error('27 Invalid value for "under_flow_tol"')

        if(result == None):
          result = True

      except Exception as e:
        result = False
        self.logger.error(f'28 Issue with err ev. Exception: {e}')

    else:
      self.logger.error('29 Bad parameter to _irr_ev_good()')
    return(result)


  def _irr_ev_for_day(self, schedule, day):
    """
    With the schedule provided, extract all of the irrigation events
    for the specified specified day.  As this function supports
    both fixed schedules and intelligent schedules the day parameter
    could be a day of the week or the value 'day1', 'day2', or 
    'day3'.

    Returns
      None       issue encountered
      False      no irrigation events for day/date
      ()         tuple of {irr_ev_seq:<>, 
                           block:<>,
                           start:<>,
                           duration:<>,
                           exp_flow:<>,
                           over_flow_tol:<>,
                           under_flow_tol:<>}
    """
    self.logger.info('entering: _irr_ev_for_day()')

    irr_events = None
    try:
      day  = day.strip().lower()
      sched_type = schedule['whatami']
      if(not sched_type in ('irrigation-schedule-intelligent',
                            'irrigation-schedule-fixed')):
        self.logger.error('30 Unknonwn schedule type')
      elif((sched_type == 'irrigation-schedule-intelligent') and
           (not day in self.config['intel_days'])):
        self.logger.error('31 Invalid day specified for intelligent schedule')
      elif((sched_type == 'irrigation-schedule-fixed') and
          (not day in self.config['fixed_days'])):
        self.logger.error('32 Invalid day specified for fixed schedule')
      else:
        if(sched_type == 'irrigation-schedule-fixed'):
          if(schedule[day] == 'no-irrigation'):
            irr_events = ()
          else:
            irr_events = schedule[day]['events']
        else: # irrigation-schedule-intelligent
          if(schedule[day]['events']  == 'no-irrigation'):
            irr_events = ()
          else:
            irr_events = schedule[day]['events']
    except Exception as e:
      self.logger.error('33 JSON issues w schedule passed to function.' +
                        f' Exception: {e}')
    return(irr_events)


  def get_irr_ev_details(self, irr_event_description):
    """
    Access the current irrigation schedule and return the full set of
    detail available for the irrigation event specified by the input
    parameter.

    Args: (enough detail to uniquely identify an irrigation event)
      dict      {'irr_ev_date' :   '<yyyy-mm-dd>' or 'any',
                 'irr_ev_day'  :    <'day1','day2','day3','mon','tue',
                                     'wed','thu','fri', 'sat'>,
                 'irr_ev_seq'  :    <int>,
                 'irr_sched_id':    <int> }
    Returns: (all detail available for an irrigation event)
      None   something went wrong before attempt to retrieve detail
      False  detail retrieval failed
      dict   {sched_id       : <int>, 
              date           : '<yyyy-mm-dd>',
              day            : <'day1', 'day2', 'day3', 'sun', 'mon',
                                'tue', 'wed', 'thu', 'fri', or 'sat'> 
              sequence       : <int>, 
              start          : '<hh:mm>', 
              duration       : '<hh:mm>', 
              block          : <'a', 'b', 'c', 'd', 'e', 'f', or 'g'>, 
              exp_flow       : <int>,   #gpm 
              under_flow_tol : <int>, #percentage
              over_flow_tol  : <int>  #percentage}
    """
    self.logger.info('entering: get_irr_ev_details()')

    event_detail = None
    valid_days = ('day1','day2','day3','sun','mon','tue','wed','thu','fri','sat')
    try:
      irr_ev_date  = irr_event_description['irr_ev_date']
      irr_ev_day   = irr_event_description['irr_ev_day']
      irr_ev_seq   = irr_event_description['irr_ev_seq']
      irr_sched_id = irr_event_description['irr_sched_id']
      if( ((not irr_ev_date) or (type(irr_ev_date) != str)) or 
          ((not irr_ev_day) or (type(irr_ev_day) != str) or 
            (irr_ev_day not in valid_days)) or 
          ((not irr_ev_seq) or (type(irr_ev_seq) != int) or
            (irr_ev_seq <= 0)) or 
          ((not irr_sched_id) or (type(irr_sched_id) != int) or
            (irr_sched_id < 0)) ):
        self.logger.error('34 Bad parameter(s) passed to irr_ev_details()')
      else:
        the_sched = self._read_curr_sched()
        if(the_sched):
          try:
            if(irr_sched_id == the_sched['id']):
              irr_events = the_sched[irr_ev_day]['events']
              for irr_ev in irr_events:
                if(irr_ev['sequence'] == irr_ev_seq):
                  event_detail = irr_ev
            else:
              event_detail = False
              self.logger.error('35 The schedule id passed in doesnt equal ' +
                                'id of current schedule')
          except Exception as e:
            event_detail = False
            self.logger.error(f'36 Exception: {e}')
        else:
          self.logger.error('37 Could not obtain the current schedule')
    except Exception as e:
      self.logger.error(f'38 Exception: {e}')
    return(event_detail)


  def irr_ev_underway(self):
    """
    Is an irrigation event currently underway?  If so return a dict
    containing detail on that irrigation event

    Returns:
      None         issue occured before determination could be made
      False        no irrigation event is currently executing
      dict         the irrigation event that should be occuring now
                      {irr_ev_date:  <yyyy-mm-dd> or 'any', 
                       irr_ev_day:   <'day1', 'day2', 'day3'> or
                                     <'sun', 'mon', 'tue', wed', 'thu',
                                      'fri', 'sat'>,
                       irr_ev_seq:   <int > 0>, 
                       irr_sched_id: <int >= 0>} 
    """
    self.logger.info('entering: irr_ev_underway()')

    irr_ev_detail = None
    self.proc_cntrl.refresh_register() 
    register = self.proc_cntrl.get_register()

    if(register):
      if(len(register) == 1):
        register_entry = register.popitem()
        irr_ev_detail = register_entry[1]  # [0] == key, [1] == value
      else:
        self.logger.error('39 More than 1 pid in the process register')
    elif(register == None):
      irr_ev_detail = False
    
    return(irr_ev_detail)


  def irr_ev_should_be_underway(self):
    """
    Per the irrigation schedule currently in force, what irr ev, if
    any, should be currently executing.

    Returns:
      None         issue occured before determination could be made

      False        an irrigation event should not be occuring
      dict         the irrigation event that should be occuring now
                      {irr_ev_date:  <yyyy-mm-dd> or 'any', 
                       irr_ev_day:   <'day1', 'day2', 'day3'> or
                                     <'sun', 'mon', 'tue', wed', 'thu',
                                      'fri', 'sat'>,
                       irr_ev_seq:   <int > 0>, 
                       irr_sched_id: <int >= 0>}
    """
    self.logger.info('entering: irr_ev_should_be_underway()')

    result    = None
    the_sched = self._read_curr_sched()

    if(the_sched):
      try:
        the_sched_id = the_sched['id']
        now_date_time = self.datetime.today()
        now_day       = self.config['fixed_days'][now_date_time.weekday()]
        now_date_str  = ( str(now_date_time.year) + '-' +
                          str(now_date_time.month).zfill(2) + '-' +
                          str(now_date_time.day).zfill(2) )
        irr_events = []
        if(the_sched['whatami'] == 'irrigation-schedule-fixed'):
          irr_events = the_sched[now_day]['events']
          irr_ev_day = now_day
          the_irr_ev_date = 'any'
          if((not irr_events) or (irr_events == 'no-irrigation')):
            result = False

        elif(the_sched['whatami'] == 'irrigation-schedule-intelligent'):
          for day in self.config['intel_days']:
            if(the_sched[day]['date'] == now_date_str):
              irr_ev_day = day
              the_irr_ev_date = the_sched[day]['date']
              irr_events = the_sched[day]['events']
              break
          if((not irr_events) or (irr_events == 'no-irrigation')):
            result = False
        else:
          result = False
          self.logger.error('40 Invalid irrigation schedule type encountered')
        
        #cycle through all irr evs, see if current time in their time window
        if(result == None):
          for event in irr_events:
            ev_date_time_start = (
              self.datetime(year=now_date_time.year,
                            month=now_date_time.month,
                            day=now_date_time.day,
                            hour=int(event['start'].split(':')[0]),
                            minute=int(event['start'].split(':')[1])))
            ev_date_time_stop = ev_date_time_start + (
              self.timedelta(hours=int(event['duration'].split(':')[0]),
                             minutes=int(event['duration'].split(':')[1])))

            ev_seq = event['sequence']

            if(ev_date_time_start < now_date_time < ev_date_time_stop):
              result = {'irr_ev_seq'   : ev_seq,
                        'irr_sched_id' : the_sched_id,
                        'irr_ev_day'   : irr_ev_day,
                        'irr_ev_date'  : the_irr_ev_date}

              break # no need to consider any other irr events

          # current time isnt within any irr ev time window 
          if(result == None):
            result = False
      except Exception as e:
        self.logger.error(f'41 Exception: {e}')
    return(result)


  def _move_new_irr_sched(self):
    """
    Called after a new irrigation schedule has been retrieved from the 
    AWS backend, validated, and stored in the special purpose directory 
    dedicated to holding newly downloaded irrigation scheds.  
 
    Returns:
      None           issue arose before file move attempt
      False          file move failed
      True           file move succeeded
    """
    self.logger.info('entering: _move_new_irr_sched()')

    result = None
    new_path = self.paths.get_path('new_irr_sched')
    if(new_path):
      file_count = self._number_files_in_dir(new_path)
      if(file_count < 1):
        self.logger.error('42 No files present in new irrigation ' +
                          'schedule directory.')
      elif(file_count > 1):
        self.logger.error('43 More than 1 file present in new irr sched' +
                          ' directory.')
      else:
        try:
          directory = self.Path(new_path)
          if(directory.is_dir()):
            for item in directory.iterdir():
              new_sched = item
              break # should only be 1 file in dir
            if(self._clear_directory(self.paths.get_path('cur_irr_sched'))):
              if(self._move_file(new_sched, 'cur_irr_sched')):
                  result = True
              else:
                self.logger.error('44 Couldnt move new irr sched to curr' +
                                  ' irr sched dir.')
                result = False
            else:
              result = False
              self.logger.error('45 Couldnt clear out dir of curr irr ' +
                                'sched that in force.')
          else:
            self.logger.error('46 Issue accessing directory for new ' +
                              'irrigation sched')
        except Exception as e:
          self.logger.error('47 Issue moving new irrigation schedule. ' +
                            f'Exception: {e}')
    else:
      self.logger.error('48 Could not extract directory path holding new' +
                        ' irr schedule.')
    return(result)


  def _new_sched_diff_from_curr_sched(self):
    """
    Determine if the newly downloaded irrigation schedule is identical
    to the irrigaiton schedule that is currently in force on the PI 4 
    platform.

    Returns
      None         issue arose before comparing two schedules
      True         the two schedules are different
      False        the two schedules are the same
    """
    self.logger.info('entering: _new_sched_diff_from_curr_sched()')

    result = None
    new_path = self.paths.get_path('new_irr_sched')
    cur_path = self.paths.get_path('cur_irr_sched')
    if(new_path and cur_path):
      new_file_count = self._number_files_in_dir(new_path)
      cur_file_count = self._number_files_in_dir(cur_path)
      if((new_file_count == 1) and (cur_file_count == 1)):
        new_sched_info = self._describe_sched(new_path)
        cur_sched_info = self._describe_sched(cur_path)
        
        if(new_sched_info and cur_sched_info):
          if((new_sched_info['sched_type']   == cur_sched_info['sched_type']) and
             (new_sched_info['created_ts']   == cur_sched_info['created_ts']) and
             (new_sched_info['created_date'] == cur_sched_info['created_date']) and
             (new_sched_info['sched_id']     == cur_sched_info['sched_id'])):
            result = False
            self.logger.error('49 The new sched and the current sched are' + 
                              ' identical')
          else:
            result = True
        else:
          self.logger.error('50 Couldnt extract new and or curr sched info.')
      else:
        self.logger.error('51 Invalid number files in new and or current' +
                          ' irr sched dir.')
    else:
      self.logger.error('52 Couldnt retrieve one or more directory paths')
    return(result)


  def _put_new_sched_in_force(self):
    """
    Called after a new irrigaiton schedule has been obtained from the 
    AWS backend, validated, and placed in a special purpose folder 
    dedicated to holding newly downloaded irr scheds.  
      
    Returns:
      True      successfully put irr sched, located in the special
                  purpose directory for new irr scheds, in force
      False     failed putting irr shced, located in the special
                  purpose direcotr for new irr scheds, in force
    """
    self.logger.info('entering: _put_new_sched_in_force()')

    result = True
    if(self._new_sched_diff_from_curr_sched()):
      pass #clean/clear/reset env then move sched file

    return(result)


  def _new_schedule_good(self, schedule):
    """
    Validate the contents of an irrigation schedule.  Irrigation
    schedules are augmented by dura_file functionality

    Args
      schedule(dict)    An irrigation schedule

    Returns:
      None      issue before schedule validation
      True      schedule determined valid
      False     schedule determined invalid

    """
    self.logger.info('entering: _new_schedule_good()')

    result = None

    if(schedule and (type(schedule) == dict)):
      if(self.df.check_object(json_object=schedule)):
        try:
          data = schedule['data']
          if(type(data['created_ts']) != int):
            self.logger.error('53 Invalid value for "created_ts"')
            result = False
          if(type(data['created_date']) != str):
            self.logger.error('54 Invalid value for "created_date"')
            result = False
          if(type(data['id']) != int):
            self.logger.error('55 Invalid value for "id"')
            result = False
          if(data['whatami'] == 'irrigation-schedule-fixed'):
            if(type(data['name']) != str):
              result = False
              self.logger.error('56 Invalid value for "name"')
            for day in self.config['fixed_days']:
              if(not day in data):
                result = False
                self.logger.error(f'57 Fixed schedule missing day: {day}')
              else:
                if(data[day] != 'no-irrigation'):
                  for event in data[day]['events']:
                    if(not self._irr_ev_good(event)):
                      result = False
                      self.logger.error('58 Bad irr ev detected for' +
                                        f' day: {day}')
                      break
            if(result == None): # no issues detected
              result = True
          elif(data['whatami'] == 'irrigation-schedule-intelligent'):
            for day in self.config['intel_days']:
              if(not day in data):
                result = False
                self.logger.error(f'59 Intel schedule missing day: {day}')
              else:
                if(type(data[day]['date']) != str):
                  result = False
                  self.logger.error('60 Invalid value for "date"')
                if(data[day]['events'] != 'no-irrigation'):
                  for event in data[day]['events']:
                    if(not self._irr_ev_good(event)):
                      result = False
                      self.logger.error(f'61 Bad irr even for day: {day}')
                      break
                if(result == None):  # no issues uncovered
                  result = True
          else:
            result = False
            self.logger.error('62 Invalid schedule type specified for ' + 
                              '"whatami" value')
        except Exception as e:
          result = False
          self.logger.error('63 JSON issue with irrigation schedule. ' +
                            f' Exception: {e}')
      else:
        result = False
        self.logger.error('64 dura_file() check indicates corrupt schedule' +
                          ' file')
    else:
      self.logger.error('65 Bad parameter to _new_schedule_good()')
    return(result)


  def _extract_sched(self, path):
    """
    Extract the entire irrigation schedule from the flat file that 
    contains it.  Irrigation schedules augmented with dura_file
    functionalty.

    Args:
     path(str)      fully qualified path to schedule directory
    Retrns:
      None          issue occured before file read attempt
      False         flat file read failed 
      sched (dict)  the irrigation schedule
    """
    self.logger.info('entering: _extract_sched()')

    the_sched = None
    if(path):
      try:
        directory  = self.Path(path)
        for item in directory.iterdir():
          if(item.is_file()):
            with item.open('r') as fd:
              contents = fd.read()
              if(self.df.check_object(json_object=contents)):
                the_dict = self.json.loads(contents)
                the_sched = the_dict['data']
              else:
                the_sched = False
                self.logger.error('66 Current irrigation schedule flat file ' +
                                  'is corrupt')
            break # should only be 1 file in dir; so why not break?
      except Exception as e:
        the_sched_info = False
        self.logger.error('67 Issue accessing irrigation schedule flat file.' +
                          f' Exception: {e}')
    else:
      self.logger.error('68 Bad parameter passed to _extract_sched')
    return(the_sched)


  def _read_curr_sched(self):
    """
    Read the irrigation schedule that is currently in force.

    Returns:
      None              issue arose before attempt to read schedule
      False             failed to read the irrigation schedule
      schedule (dict)   the irrigation schedule
    """ 
    self.logger.info('entering: _read_curr_sched()')

    result = None
    path = self.paths.get_path('cur_irr_sched')
    if(path):
      file_count = self._number_files_in_dir(path)
      if(file_count):
        if(file_count == 1):
          result = self._extract_sched(path)
        elif(file_count > 1):
          self.logger.error('69 More than one file in curr irr sched dir')
        else:
          self.logger.error('70 Couldnt count num of files in curr irr ' +
                            'sched dir')
      else:
        self.logger.error('71 No files exist in curr irr sched dir')
    else:
      self.logger.error('72 Couldnt retrieve dir path for curr irr schedule')
  
    return(result)


  def _describe_sched(self, directory_path):
    """
    Extract high-level info from an irr sched flat file.  As this 
    function will only read the first flat file contained in the
    specified directory, it is assumed that this function is only
    invoked on directories containing a single file.  You must take
    care when calling this function to assure that your direcotry is
    clean.  Note that if you view a directory in a high-level tool 
    like MS File Explorer you might not "see" all the files contained 
    by the directory.  Some files (e.g., temporary files created MS 
    Word when it opens a document) might exist yet not display due 
    to settings/options for MS Explorer.

    Args:
      directory_path(str)   fully qualified directory path containing
                              the irrigation schedule
      
    Returns:
      None        issue before extracting schedule info
      False       issue accessing file or bad JSON in file
      dict          {'sched_type' : '', 
                     'created_ts' : '', 
                     'created_date' : '',
                     'sched_id' : ''}
    """
    self.logger.info('entering: _describe_sched()')

    the_sched_info = None
    the_sched      = {}

    directory = self.Path(directory_path)
    if((directory_path) and (directory.is_dir())):
      for file in directory.iterdir():
        try:
          with file.open('r') as fd:
            contents = fd.read()
            if(self.df.check_object(json_object=contents)):
              the_dict = self.json.loads(contents)
              the_sched = the_dict['data']
            else:
              the_sched = False
              self.logger.error('73 Irrigation schedule in file is corrupt')
        except Exception as e:
          self.logger.error(f'74 Exception: :{e}')
        break  # should only be 1 file in dir; so why not break?
      if(the_sched):
        try:
          the_sched_info                 = {}
          the_sched_info['sched_type']   = the_sched['whatami']
          the_sched_info['created_ts']   = the_sched['created_ts']
          the_sched_info['created_date'] = the_sched['created_date']
          the_sched_info['sched_id']     = the_sched['id']
        except Exception as e:
          the_sched_info = False
          self.logger.error(f'76 JSON in file is damaged. Exception: {e}')
    else:
      self.logger.error('77 Bad parameter passed to _describe_sched()')

    return(the_sched_info)


  def _save_new_sched(self, schedule):
    """
     Before this call a newly obtained irrigation schedule has been
     validated.  Now, it is time to contruct the file name for the
     new irrigation schedule and store the file in a special purpose
     directory for newly arrived schedules.  This does not place the
     new irrigation schedule in force.  An extra step is required to
     place the irrigation schedule into force.
    
    Args:
      schedule(dict)       an irrigation schedule 

    Returns:
      None          issue occured before attempt to store irr sched
      True          successfully store new irr sched locally
      False         failed to store new irr sched locally
    """
    self.logger.info('entering: _save_new_sched()')

    result    = None
    file_name = ''

    if(schedule and (type(schedule) == dict)):
      try:
        sched_type = schedule['data']['whatami']
        date = (schedule['data']['created_date']).replace('-','_')
        id = str(schedule['data']['id'])
        path = self.paths.get_path('new_irr_sched') + self.paths.divider
        if(sched_type == 'irrigation-schedule-intelligent'):
          file_name = date + '_intel_' + id + '.json'
        elif(sched_type == 'irrigation-schedule-fixed'):
          file_name = date + '_fixed_' + id + '.json'
        else:
          self.logger.error('78 Unrecognized schedule type')
        if(file_name):
          full_name = path + file_name
          result = self.df.write_data(full_name, schedule)
      except Exception as e:
        result = False
        self.logger.error(f'79 Couldnt save new sched to file.  Exception: {e}')
    else:
      self.logger.error('80 Bad parameter passed to _save_new_sched()')
    return(result)


  def _store_new_schedule(self, schedule):
    """
    Store a newly downloaded and validated irrigation schedule locally.
    Storing a new irr sched locally does not put it in force; that
    requires another step (i.e., function within this module)

    Args:
      schedule(dict)           an irrigation schedule

    Returns:
      None          issue before attempt to store
      True          successfully stored new irr sched
      False         failed to store new irr sched
    """
    self.logger.info('entering: _store_new_schedule()')

    result = None
    if(schedule and (type(schedule) == dict)):
      if(self._new_schedule_good(schedule)):
        if(self._clear_directory(self.paths.get_path('new_irr_sched'))):
          if(self._save_new_sched(schedule)):
            result = True
          else:
            result = False
            self.logger.error('81 Couldnt save downloaded sched to flat file.')
        else:
          result = False
          self.logger.error('82 Couldnt clear dir for new incoming schedule.')
      else:
        result = False
        self.logger.error('83 downloaded schedule invalid.')
    else:
      self.logger.error('84 Bad parameter sent to _store_new_schedule().')
    return(result)


  def curr_sched_usable(self):
    """
    Determine if the current irrigation schedule (i.e., currently in
    force) is valid and not expired.  Note, only intelligent irrigation
    schedules expire (i.e., prescribe irrigation events that occur in 
    the past).

    Returns:
      None            issue occured before determination could be made
      True            irrigation schedule is valid and not out of date
      False           irrigaiton schedule invalid or out of date
    """
    self.logger.info('entering: curr_sched_usable()')

    result    = None
    the_sched = self._read_curr_sched()

    if(the_sched):
      try:
        if(the_sched['whatami'] == 'irrigation-schedule-fixed'):
          result = True
        elif(the_sched['whatami'] == 'irrigation-schedule-intelligent'):
          now_date_time  = self.datetime.today()
          last_day = self.config['intel_days'][len(self.config['intel_days']) - 1]
          last_date_str  = the_sched[last_day]['date']
          last_date      = self.datetime(year=int(last_date_str.split('-')[0]),
                                         month=int(last_date_str.split('-')[1]),
                                         day=int(last_date_str.split('-')[2]))
          last_date_time = self.datetime(year=last_date.year,
                                         month=last_date.month,
                                         day=last_date.day,
                                         hour=23,
                                         minute=59)
          if(last_date_time < now_date_time):
            result = False
          else:
            result = True
        else:
          self.logger.error('85 Invalid schedule type specified')
      except Exception as e:
        self.logger.error(f'86 Exception: {e}')
    else:
      self.logger.error('87 Could not obtain the current schedule')
    return(result)


  def get_schedule(self):
    """
    Gather data to send to AWS (i.e., info on the irr sched currently
    in force and the PI 4's settings for current date / time) and call
    the AWS backend, passing in the gathered data, in order to obtain
    the most up to date irrigation schedule.  The AWS backend will 
    respond with either a more up-to-date irrigation schedule than the
    on currently in force on the PI 4 or the message 
    'no-newer-sched-available'.  Newly downloaded irrigation schedules
    will be stored locally, validated, and then placed in force.
 
    Returns:
      None                Issue before invoking API Gwy endpoint
                            could be down network / ISP
      True                New irr sched retrieved, validated, stored
                            -OR- 'no-newer-sched-available'
      False               Issue invoking API Gwy endpoint
    """
    self.logger.info('entering: get_schedule()')

    result = None
    try:
      credentials = self.boto3.get_credentials()
      auth        = self.AWS4Auth(credentials.access_key, 
                                  credentials.secret_key,
                                  self.config['region'], 
                                  'execute-api')
      endpoint          = self.config['get_sched']
      method            = 'GET'
      headers           = {}
      curr_sched_path   = self.paths.get_path('cur_irr_sched')
      sched_description = self._describe_sched(curr_sched_path)
      payload           = {'sched' : sched_description,
                          'ts'    : int(self.time.time())}

      if(payload['sched'] and payload['ts']):
        try:
          response = self.requests.request(method, endpoint, auth=auth, 
                                           data=self.json.dumps(payload), 
                                           headers=headers)
          status_code = response.status_code
          if(status_code == 200):
            try:
              schedule = self.json.loads(response.text)
              if(schedule['data']['whatami'] == 'no-newer-sched-available'):
                result = True
              else:                
                if(self._store_new_schedule(schedule)):
                  self._put_new_sched_in_force()
                  result = True
                else:
                  self.logger.error('88 Newly downloaded sched couldnt' +
                                    ' be stored locally.')
                  result = False
            except Exception as e:
              self.logger.error('89 Issue with API Gateway response data.' +
                                f' Exception: {e}')
          else:
            result = False
            self.logger.error('90 API returned bad status code:' +
                              f' {status_code}')
        except Exception as e:
          self.logger.error(f'91 Issue before invoking API. Exception: {e}')
          result = None
      else:
        self.logger.error('92 Couldnt access curr irr sched and or curr ' +
                          'date time.')
    except Exception as e:
      self.logger.error(f'93 Exception: {e}')
    return(result)
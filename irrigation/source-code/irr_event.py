"""
Jaye Hicks 2021

Fix me:
- need more research on PI 4 GPIO pin module; there are alternatives
- d

Obligatory legal disclaimer:
  You are free to use this source code (this file and all other files 
  referenced in this file) "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER 
  EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. 
  THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THIS SOURCE CODE
  IS WITH YOU.  SHOULD THE SOURCE CODE PROVE DEFECTIVE, YOU ASSUME THE
  COST OF ALL NECESSARY SERVICING, REPAIR OR CORRECTION. See the GNU 
  GENERAL PUBLIC LICENSE Version 3, 29 June 2007 for more details.

Relavant Real World Detail:
- The Lonesome vine's irrigation pump is rated for a maximum of 20
  gallons per minute.  
- Lonesome Vines's flow meter pulses once for every 1 gallon of flow 
  (i.e., one rotation of an internal paddle.)  
- Executive decision made to work in whole seconds vs. fractional 
  seconds.
- An irrigation event involves dispensing water to a single vineyard
  block for a elapsed period of time (e.g., 2 hours)
- As irrigaiton events are long running, a small chance exists that
  they may be interrupted by an unexpected reboot of teh PI 4 
- Tracking the amount of water dispensed to a vineyard block is the
  vitrally important.
    - Run time data structures are periodically check pointed to flat
      files, augmented by dura_file functionality
    - Updating an existing file involves renaming the existng file to
      a backup (i.e., <filename>.old) and the creation of a new file

GPIO Mapping
  17  input, water flow sensor
  output, block valves {'a': 1, 'b': 8, 'c': 12, 'd': 16, 
                        'e': 18, 'f': 23, 'g': 24}

The class irr_event() uses a data structure to keep track of the
number of pulses that have been received from a water flow sensor 
during a single irrigation event.  This is an array of individual 
elements in which each element contains a key value pair.  The key is 
the epoch time stamp for the datapoint and the value is the cumulative 
number of water sensor pulses received since the irrigation event 
started or since the last unexpected reboot of the PI 4 platform.  If 
an exepcted reboot of the PI 4 occurs during an irrigation event then
multiple OS pids (i.e., one for each OS process that was involved in
managing the irrigation event) will exist in the pulse count data 
structure.  The data across all OS pids in the data structure will
be combined to determine the total gallons dispensed and the average 
flow rate for the entire irrigation event.

The pulse count data structure is illustrated below.  Note, in this 
example illustration, a single unexpected reboot occuring during the 
irrigation event which led to two elements in the top level array to 
the right of the key named 'pulses.'

{'whatami'  : 'pulse_count',
  'sched_id' : <int>,
  'date'     : '<yyyy-mm-dd>',   #or 'any'
  'day'      : '<str>',          #allowed values include 'day1',
                                 #'day2', 'day3', 'sun', 'mon',
                                 #'tue', 'wed', 'thu', 'fri', 'sat'
  'sequence' : <int>,
  'block'    : '<letter>',
  'pulses'   : [{'<int - OS pid>' : [{'<int - epoch ts>' : 
                                       <int - pulse data>},
                                     {'<int>' : <int>}]},
                {'<int - OS pid>' : [{'<int - epoch ts>' : 
                                      <int - pulse data>},
                                     {'<int>' : <int>}]} ]}
"""
class irr_event():
  import logging
  import time
  import json
  from   pathlib   import Path
  from   datetime  import datetime, timedelta

  #import RPi.GPIO as GPIO   (gonna go with this one????)

  import process_cntrl
  import dura_file
  import lv_paths


  def __init__(self):
    """
    Preliminary initialization.  Final initialization, using input
    parameters, occurs in irr_event.start_irr_ev()

    Objects of type irr_event() are created and used in two fashions.
    First, in support of normal irrigation management, they are created
    and the full complement of their features and functions are used.
    Second, they are created as utility objects and a portion of their
    features and functions are used to tactically salvage/process 
    orphaned pulse count files to hopefully prevent loss of data.
    """
    self.logger = self.logging.getLogger(__name__)
    self.logger.info('entering: __init__()')

    self.df      = self.dura_file.dura_file()
    self.paths   = self.lv_paths.lv_paths()
    self.process = self.process_cntrl.process_cntrl()

    #initializations not requiring input parameterss 
    self.pulse_count              = 0
    self.all_pulse_data           = []
    self.config                   = {}
    self.config['num_sleep_secs'] = 300
    self.config['blocks']         = ['a','b','c','d','e','f','g']
    self.config['flow_sensor']    = 17

    #following ints match physical wiring of valve controller to valves
    self.config['valves']         = {'a' : 7,
                                     'b' : 8,
                                     'c' : 12,
                                     'd' : 16,
                                     'e' : 18,
                                     'f' : 12,
                                     'g' : 24}

    self.config['fixed_days']     = ['mon','tue','wed','thu','fri','sat','sun']
    self.config['intel_days']     = ['day1','day2','day3']
    self.config['flow_alarms']    = ['under','over']

    #flow rate for Hunter Industries HC100FLOW Hydrawise 1" HC Flow Meter
    self.config['gals_per_pulse'] = 1

    #obtain the linux PID of the OS process executing this script
    process_info = self.process.get_process_info()
    if(process_info):
      self.config['pid'] = process_info['pid']
    else:
      self.config['pid'] = ''
      self.logger.error('1 Couldnt obtain pid of OS process running this code')
 
  def _set_up_GPIO(self):
    """
    Set up the Raspberry PI 4's GPIO pins to accept input from water
    flow sensor and to control the valve actuator.  Currently using
    the Hunter HC100 Flow Hydrawise 1" and the Elegco 8 Channel Relay
    Module
    """
    self.logger.info('entering: _set_up_GPIO()')

    self.GPIO.setmode(self.GPIO.BCM)
    self.GPIO.setwarnings(False)

    #Set single GPIO as input for water flow sensor pulses.  pull high
    self.GPIO.setup(self.config['flow_sensor'], 
                    self.GPIO.IN, 
                    pull_up_down=self.GPIO.PUD_UP)
    self.GPIO.add_event_detect(17, 
                               self.GPIO.BOTH, 
                               callback=self._sensor_pulse_callback, 
                               bouncetime=200)

    #Set 7 GPIO as outputs to control low voltage to hi voltage relay
    self.GPIO.setup(self.config['valves']['a'], self.GPIO.OUT)  #block A
    self.GPIO.setup(self.config['valves']['b'], self.GPIO.OUT)  #block B
    self.GPIO.setup(self.config['valves']['c'], self.GPIO.OUT)  #block C
    self.GPIO.setup(self.config['valves']['d'], self.GPIO.OUT)  #block D
    self.GPIO.setup(self.config['valves']['e'], self.GPIO.OUT)  #block E
    self.GPIO.setup(self.config['valves']['f'], self.GPIO.OUT)  #block F
    self.GPIO.setup(self.config['valves']['g'], self.GPIO.OUT)  #block G


  def _curr_date_as_string(self):
    """
    """
    self.logger.info('entering: _curr_date_as_string()')

    try:
      now_date_time = self.datetime.today()
      now_date_str  = (str(now_date_time.year) + '_' +
                       str(now_date_time.month).zfill(2) + '_' +
                       str(now_date_time.day).zfill(2) )
    except Exception as e:
      self.logger.error(f'datetime Exception: {e}')
    return(now_date_str)


  def _move_file(self, file, target):
    """
    Move a problematic file (i.e., corrupt or cause comms failure)
 
    Args:
      file(pathlib)     pathlib object represents corrupted file
      targert(str)      specifies destination folder.
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
            self.logger.error(f'2 Could not move file: {file_name} to ' +
                              f'{target}. Exception: {e}')
            result = False
        else:
          self.logger.error('3 Couldnt extract file name from pathlib object')
      except Exception as e:
        self.logger.error('4 Bad argument passed to _move_file(). ' +
                          f' Exceptions {e}')
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
        self.logger.error(f'5 Could not clear a directory: {dir_name}.' +
                          f' Exception: {e}')
    else:
      self.logger.error('6 Bad argument sent to _clear_directory()')
    return(result)


  def _clear_extraneous_files(self):
    """
    Detect and move any extraneous files to special purpose directory.
    A separate process will upload them to an S3 bucket for future 
    forensic analysis.

    These function exists out of an abundance of caution to salvage 
    pulse count orphaned pulse count files and avoid loss of this data.
    """
    self.logger.info('entering: _clear_extraneous_files()')

    result = True
    curr_pulse_file    = self.config['pulse_file']
    old_pulse_file     = curr_pulse_file + '.old'
    acceptable_files   = (curr_pulse_file, old_pulse_file)
    unacceptable_files = []
    path               = self.paths.get_path('irr_ev_in_progress')
    if(path):
      try:
        directory = self.Path(path)
        if(directory.exists() and directory.is_dir()):

          #scan directory special purpose directory that holds files
          for item in directory.iterdir():
            if(not item.name in acceptable_files):
              unacceptable_files.append(item)

          #move extraneous files out of special directory
          for file in unacceptable_files:
            if(not self._move_file(file, 'orphans')):
              self.logger.error(f'7 couldnt move exraneous file: {file.name} ' +
                                 'to orphans directory')
              result = False
        else:
          result = False
          self.logger.error('8 Could not access directory for curr irr ev')
      except Exception as e:
        result = False
        self.logger.error(f'9 Exception: {e}')
    else:
      result = False
      self.logger.error('10 Could not obtain directory path for curr irr ev')
    return(result)


  def _create_backup_file(self):
    """
    when updating a pulse count file, save existing one to
    <filename.json>.old and then create a 'new' file named 
    <filename>.json
    """
    self.logger.info('entering: _create_backup_file()')

    result = True
    curr_pulse_file = self.config['pulse_file']
    old_pulse_file  = curr_pulse_file + '.old'
    path            = self.paths.get_path('irr_ev_in_progress')
    if(path):
      old_pulse_file_path = path + self.paths.divider + old_pulse_file
      try:
        directory = self.Path(path)
        if(directory.exists() and directory.is_dir()):

          #scan special purpose directory that holds the files
          current = None
          old     = None
          for item in directory.iterdir():
            if(item.name == curr_pulse_file):
              current = item
            if(item.name == old_pulse_file):
              old = item
          if (old):
            old.unlink()
          if(current):
            current.rename(old_pulse_file_path)
        else:
          result = False
          self.logger.error('11 Could not access directory curr irr event' +
                            ' in progress')
      except Exception as e:
        result = False
        self.logger.error(f'12 Exception: {e}')
    else:
      result = False
      self.logger.error('13 Could not obtain directory path for irr event' +
                        ' in progress')
    return(result)


  def _close_all_valves(self):
    """
    Use Raspberry PI 4 GPIO pin to send low voltage signal to the
    relay module which will in turn send high voltage signal to the
    correct irrigation valve actuator.
    Currently in use: Elegco 8 Channel Relay Module
    """
    self.logger.info('entering: _close_all_valves()')
  
    self.GPIO.output(self.config['valves']['a'], self.GPIO.LOW)  #block A
    self.GPIO.output(self.config['valves']['b'], self.GPIO.LOW)  #block B
    self.GPIO.output(self.config['valves']['c'], self.GPIO.LOW)  #block C
    self.GPIO.output(self.config['valves']['d'], self.GPIO.LOW)  #block D
    self.GPIO.output(self.config['valves']['e'], self.GPIO.LOW)  #block E
    self.GPIO.output(self.config['valves']['f'], self.GPIO.LOW)  #block F
    self.GPIO.output(self.config['valves']['g'], self.GPIO.LOW)  #block G


  def _open_value(self, block):
    """
    Use Raspberry PI 4 GPIO pin to send low voltage signal to the
    relay module which will in turn send high voltage signal to the
    correct irrigation valve actuator.
    Currently in use: Elegco 8 Channel Relay Module
    """
    self.logger.info('entering: _open_value()')

    block = block.strip().lower()
    self._close_all_valves()

    if(block in self.config['valves']):
      self._close_all_valves()
      self.GPIO.output(self.config['valves'][block], self.GPIO.HIGH)
    else:
      self.logger.error(f'73 Cant open block: {block}. It doesnt exist.')


  def _calculate_average_flow_rate(self):
    """
    Post irrigation event.  Access all pulse count data for the
    irrigation event.  In this sample a single unexpected reboot
    of PI 4 platform.

    Illustrative pulse count data structure:
      {'whatami'  : 'pulse_count',
        'sched_id' : <int>,
        'date'     : '<yyyy-mm-dd>',  #or 'any'
        'day'      : '<str>',         #allowed values include 'day1',
                                      #'day2', 'day3', 'sun', 'mon',
                                      #'tue', 'wed', 'thu', 'fri','sat'
        'sequence' : <int>,
        'block'    : '<letter>',
        'pulses'   : [{'<int - OS pid>' : [{'<int - epoch ts>' : 
                                            <int - pulse data>},
                                          {'<int>' : <int>}]},
                      {'<int - OS pid>' : [{'<int - epoch ts>' : 
                                            <int - pulse data>},
                                          {'<int>' : <int>}]} ]}
    Returns:
      None             calculation was not attempted
      <int>            average flow rate in gallons per minute
    """
    self.logger.info('entering: _calculate_average_flow_rate()')

    average_flow = None
    factor       = self.config['gals_per_pulse']
    if(self.all_pulse_data):
      all_rates_for_all_pids = []
      for cnt1, all_pid_data in enumerate(self.all_pulse_data):
        for single_pid_data in all_pid_data:
          for cnt2, single_pid_rec in enumerate(self.all_pulse_data[cnt1][single_pid_data]):
            if(cnt2 != 0): #flow calculation requires a previous data point
              for ts in single_pid_rec: #not a loop; used to set variable 'ts'
                current_ts        = int(ts)
                current_pulse_cnt = single_pid_rec[ts]
              elapsed_mins  = int((current_ts - previous_ts) / 60)
              pulse_count   = current_pulse_cnt - previous_pulse_cnt
              gallons_flow  = pulse_count * factor
              if(elapsed_mins > 0):
                flow_rate = int(gallons_flow / elapsed_mins)
                all_rates_for_all_pids.append(flow_rate)
              else:
                all_rates_for_all_pids.append(0)
            
            for ts in single_pid_rec: #not a loop; used to set variable 'ts'
              previous_ts        = int(ts)
              previous_pulse_cnt = single_pid_rec[ts]
      if(len(all_rates_for_all_pids)):
        average_flow = int(sum(all_rates_for_all_pids) / len(all_rates_for_all_pids))
      else:
        average_flow = 0
    else:
      self.logger.error('14 No pulse count data; couldnt calculate ' +
                        'average flow rate')
    return(average_flow)


  def _calculate_total_gals_disp(self):
    """
    Calculate total gallons dispensed during single irrigation event.
    If unexpected PI 4 reboot occuring during event, data from multiple
    OS processes (i.e., that managed a portion of event) has to be
    combined.
    
    Returns:
      None             calculation was not attempted
      <int>            total gallons dispensed
    """
    self.logger.info('entering: _calculate_total_gals_disp()')

    total_gals_disp = None
    factor          = self.config['gals_per_pulse']

    if(self.all_pulse_data):
      total_pulses_for_all_pids = 0
      for cnt1, all_pid_data in enumerate(self.all_pulse_data):
        for single_pid in all_pid_data:
          max_count_for_pid = 0
          for single_pid_rec in self.all_pulse_data[cnt1][single_pid]:
            for ts in single_pid_rec: #nt a loop; used to set variable 'key'
              key = ts
            value = single_pid_rec[key]
            if(value > max_count_for_pid):
              max_count_for_pid = value
          total_pulses_for_all_pids += max_count_for_pid
      total_gals_disp = total_pulses_for_all_pids * factor
    else:
      self.logger.error('15 No pulse records. couldnt calculate total' +
                        ' gals disp')
    return(total_gals_disp)


  def _pulse_data_valid(self, pulse_data):
    """
    Args:
      pulse_data      pulse count data structure

    Returns:
      True         no issues found with data structure
      False        issue found with data structure
      None         issue occured before validation attempt
    """
    self.logger.info('entering: _pulse_data_valid()')

    result = None
    try:
      if((type(pulse_data['whatami']) != str) or
         (pulse_data['whatami'] != 'pulse_count')):
        self.logger.error('16 Invalid value for "whatami"')
        result = False
      if((type(pulse_data['sched_id']) != int) or
         (pulse_data['sched_id'] < 0)):
        self.logger.error('17 Invalid value for "sched_id"')
        result = False
      if(type(pulse_data['date']) != str):
        self.logger.error('18 Invalid value for "date"')
        result = False
      if((type(pulse_data['day']) != str) or
         ((not pulse_data['day'] in self.config['intel_days']) and
          (not pulse_data['day'] in self.config['fixed_days']))):
        self.logger.error('19 Invalid value for "day"')
        result = False
      if((type(pulse_data['sequence']) != int) or
         (pulse_data['sequence'] <= 0)):
        self.logger.error('20 Invalid value for "sequence"')
        result = False
      if((type(pulse_data['block']) != str) or
         (not pulse_data['block'] in self.config['blocks'])):
        self.logger.error('21 Invalid value for "block"')
        result = False    
      for pid_data in pulse_data['pulses']:
        for pid_entry in pid_data:
          if((type(pid_entry) != str) or (int(pid_entry) <= 0)):
            result = False
            self.logger.error('22 Invalid value for a pulse pid')
          for single_pulse in pid_data[pid_entry]:
            for ts in single_pulse:
              if((type(ts) != str) or (int(ts) <= 0)):
                result = False
                self.logger.error('23 Invalid time stamp for single pulse')
              if((type(single_pulse[ts]) != int) or (single_pulse[ts] <= 0)):
                result = False
                self.logger.error('24 Invalid pulse count for single pulse')
    except Exception as e:
      result = False
      self.logger.error(f'25 Exception: {e}')
    if(result == None):  # no issues uncovered
      result = True
    else:
      self.logger.error('26 Encountered some bad pulse data')
    return(result)


  def _write_pulse_count_file(self):
    """
    Write the pulse count data held in the runtime data structure used
    by an irr_event() object to a flat file.  Pulse count flat files 
    are named using a naming convention: 
  
    pulse_count_<yyyy_mm_dd>_<sched_id>_<sequence>.json  
  
    Returns:
      True             pulse count file successfully written
      False            error writing pulse count file
    """
    self.logger.info('entering: _write_pulse_count_file()')

    result = False
    if(self._clear_extraneous_files()):
      if(self._create_backup_file()):
        pulse_data = { 'whatami'  : 'pulse_count',
                       'sched_id' : self.config['sched_id'],
                       'date'     : self.config['date'], 
                       'day'      : self.config['day'],
                       'sequence' : self.config['sequence'],
                       'block'    : self.config['block'],
                       'pulses'   : self.all_pulse_data}
        result = self.df.write_data(self.config['pulse_file_path'], pulse_data)
      else:
        self.logger.error('27 Issue encountered preserving old pulse ' +
                          'count file')
    else:
      self.logger.error('28 Couldnt clear extraneous pulse count files')
    return(result)


  def _read_pulse_count_file(self, file_path=None):
    """
    If a pulse count flat file exists for an irrigation event, load
    the data in that flat file into the irr_event() object's runtime
    data structure.  Look for both a pulse count file and a backup
    file with preference for the non backup if both exist.
    
    Args:
      file(str)  this parameter will be supplied a value only when
                 an irr_event() object is being used as a utility 
                 to process orphaned pulse count files
    Returns:
      None      neither pulse file nor old pulse file exists or issue
                  occured before detection attempt to access files
      True      successfully read pulse file or old pulse file
      False     issue / error during pulse file read
    """
    self.logger.info('entering: _read_pulse_count_file()')

    result     = None
    try:
      if(file_path):
        pulse_file = self.Path(file_path)
      else:
        pulse_file = self.Path(self.config['pulse_file_path'])
        if(not pulse_file.is_file()):
          pulse_file = self.Path(self.config['old_pulse_file_path'])

      if(pulse_file.is_file()):
        try:
          with pulse_file.open('r') as fd:
            contents = fd.read()
            if(self.df.check_object(json_object=contents)):
              contents_as_a_dict = self.json.loads(contents)
              data_section = contents_as_a_dict['data']
              if(self._pulse_data_valid(data_section)):
                pulses = data_section['pulses']
                self.all_pulse_data = []
                for pid_data in pulses:
                  self.all_pulse_data.append(pid_data)
                result = True
              else:
                self.logger.error('29 Pulse data flat file contains data' +
                                  ' errors')
                result = False
                fd.close()
                if(not self._move_file(pulse_file, 'corrupt_files')):
                  self.logger.error('30 cant move corrupt file: ' +
                                    f'{pulse_file.name} to corrupt files' +
                                    ' directory')
            else:
              self.logger.error('31 Pulse data flat file is corrupt')
              result = False
              fd.close()
              if(not self._move_file(pulse_file, 'corrupt_files')):
                self.logger.error('32 cant move corrupt file: ' +
                                  f'{pulse_file.name} to corrupt files' +
                                  ' directory')
        except Exception as e:
          result = False
          self.logger.error(f'33 Exception: {e}')
    except Exception as e:
      self.logger.error(f'35 Exception: {e}')
    return(result)


  def _check_for_shutdown_request(self):
    """
    Other OS processes (i.e., executing irr_cntrl.py) may send the OS
    processing executing this script a request to shutdown.  Detect
    and prescence of such reqeust.

    Returns:
      True       another OS process requests a shutdown
      False      no OS process is requesting a shutdown
      None       an issue occured before check could be made
    """
    self.logger.info('entering: _check_for_shutdown_request()')

    result = None
    semaphore_file_name = 'stop_irr.json'
    path                = self.paths.get_path('irr_event')
    if(path):
      try:
        directory = self.Path(path)
        if(directory.exists() and directory.is_dir()):
          for item in directory.iterdir():
            if((item.is_file()) and (item.name == semaphore_file_name)):
              result = True
              break
          if(result == None):
            result = False
        else:
          self.logger.error('36 Could not access directory irr ev')
      except Exception as e:
        self.logger.error(f'37 Exception: {e}')
    else:
      self.logger.error('38 Could not retrieve directory path for irr' +
                        ' event directory')
    return(result)


  def _send_gals_disp(self, av_flow_rate=0, total_gals_disp=0, date_str=None):
    """
    Create a gallons dispensed message flat file and place in special 
    directory.  A separate module will transmit the data to the AWS
    backend.  There are two kinds of gallons dispensed messages.  If 
    this is the '1 Gallon' variety the input parameter av_flow_rate 
    will not be set and the input parameter total_gals_disp parameter 
    will be set to 1.  The parameter date_str will only be supplied if 
    an irr_event() object is being used as a utility object to process 
    orphaned pulse count files
    
    Args:
      av_flow_rate(int)      the average flow rate for the entire 
                               irr ev (includes all OS processes)
      total_gals_disp(int)   the total number of gallons dispensed
                               for the entire irr ev (all OS processes)
      date_str(str)          only supplied for utility irr_event() 
                               objects
    Returns:
      False                  could not send the gallons dispensed file
      True                   successfully sent gallons dispensed file
    """
    self.logger.info('entering: _send_gals_disp()')

    result = None
    if((type(av_flow_rate) == int) and (av_flow_rate >=0) and
       (type(total_gals_disp) == int) and (total_gals_disp >= 0)):
      try:
        if(date_str):
          date_string = date_str
        else:
          date_string = self._curr_date_as_string()
        message = {'whatami' : 'gallons-dispensed',
                    'date'     : date_string,
                    'sched_id' : self.config['sched_id'],
                    'sequence' : self.config['sequence'],
                    'block'    : self.config['block'],
                    'gallons'  : total_gals_disp,
                    'flow'     : av_flow_rate}

        #build the file name
        file_name = 'gals_disp_' + date_string + '_' + str(self.config['sched_id'])
        file_name += '_' + str(self.config['sequence']) + '_'
        if((av_flow_rate == 0) and (total_gals_disp == 1)):
          file_name += 'a.json'
        else:
          file_name += 'b.json'

        #write the file
        path = self.paths.get_path('gals_disp')
        if(path):
          path_and_file_name = path + self.paths.divider + file_name
          if(self.df.write_data(path_and_file_name, message)):
            result = True
          else:
            result = False
            self.logger.error('39 could not write the gals disp file for' +
                              f' file: {file_name}')
        else:
          result = False
          self.logger.error('40 Couldnt get directory path for gals_disp' +
                            ' directory')
      except Exception as e:
        result = False
        self.logger.error(f'41 Exception: {e}')
    else:
      self.logger.error('42 Bad parameters passed into ' +
                        '_send_final_gals_disp()')

    return(result)


  def _send_flow_alarm(self, alarm_type, percent):
    """
    Create a flat file containing the alarm condition information. A
    separate module will pick up this file and transmit it to the
    AWS backend.  The flat file will be named using the standard:
    'alarm_<'under' or 'over'>_<yyyy-mm-dd>_<sched_id>_<seq>.json

    The flat file will employ dura_file functionality and will contain
    'date'     : 'yyyy-mm-dd'
    'sched_id' : int
    'sequence' : int
    'block'    : <'a'-'g'>
    'percent'  : int

    Args:
      str       the type of flow alarm.  currently can be 'under' or 'over'
      int       the percent (0 - 100) the flow rate is under the limit
    """
    self.logger.info('entering: _send_flow_alarm()')

    if((type(percent) == int) and (percent > 0) and
        (type(alarm_type) == str) and 
        (alarm_type in self.config['flow_alarms'])):
      
      #construct file name
      date_string = self._curr_date_as_string()
      file_name   = 'alarm_' + alarm_type + '_' + date_string + '_' 
      file_name   += str(self.config['sched_id']) + '_' 
      file_name   += str(self.config['sequence']) + '.json'
      
      try:
        #construct file contents
        message = {'whatami'  : alarm_type + 'flow-alarm',
                   'date'     : date_string,
                   'sched_id' : self.config['sched_id'],
                   'sequence' : self.config['sequence'],
                   'block'    : self.config['block'],
                   'percent'  : percent}
        
        #write file to special purpose comms dir
        path = self.paths.get_path('alarms')
        if(path):
          path_and_file_name = path + self.paths.divider + file_name
          if(not self.Path(path_and_file_name).is_file()):
            self.df.write_data(path_and_file_name, message)
        else:
          self.logger.error('43 Couldnt get directory path for alarms dir')
      except Exception as e:
        self.logger.error(f'44 Exception: {e}')
    else:
      self.logger.error('45 Bad parameters to _send_flow_alarm')


  def _check_flow_rate(self):
    """
    Determine the flow rate, based on the last two data points.  Check
    for both under flow and over flow.  If either condition is detected
    place an alarm file in special directory; a separate module will 
    transmit the alarm data to the AWS backend.
    """
    self.logger.info('entering: _check_flow_rate()')

    the_pid       = self.config['pid']
    factor        = self.config['gals_per_pulse']
    expected_flow = self.config['exp_flow'] 
    upper_tol     = self.config['over_flow_tol']
    lower_tol     = self.config['under_flow_tol']

    the_pids_data     = []
    pen_ts            = ''
    pen_pulse_count   = 0
    last_ts           = ''
    last_pulse_count  = 0 
    if(the_pid):
      #access all pulse count data points for the pid
      for single_pid in self.all_pulse_data:
        for pid in single_pid:  #not loop; setting 'pid'
          if(pid == the_pid):
            the_pids_data = single_pid[the_pid]
      if(len(the_pids_data) >= 2):
        penultimate_data_point = the_pids_data[len(the_pids_data) - 2]
        last_data_point        = the_pids_data[len(the_pids_data) - 1]

        #dig out last two data points for the pid
        for key in penultimate_data_point:  #not loop; setting 'key'
          pen_ts          = key
          pen_pulse_count = penultimate_data_point[key]
        for key in last_data_point: #not loop; setting 'key'
          last_ts          = key
          last_pulse_count = last_data_point[key]

        #determine the flow rate for last two data points for pid
        if(pen_ts and pen_pulse_count and last_ts and last_pulse_count):
          elapsed_mins  = int((last_ts - pen_ts) / 60)
          pulse_count   = last_pulse_count - pen_pulse_count
          gallons_flow  = pulse_count * factor
          if(elapsed_mins > 0):
            flow_rate = int(gallons_flow / elapsed_mins)
          else:
            flow_rate = 0

          #send alarm if flow rate exceeds over / under flow tolerance
          if((expected_flow > 0) and (upper_tol > 0) and (lower_tol > 0)):
            upper_limit = expected_flow + int(expected_flow * (upper_tol * .01))
            lower_limit = expected_flow - int(expected_flow * (lower_tol * .01))

            if(flow_rate > upper_limit):
              percent_over = int((flow_rate - expected_flow) / expected_flow * 100)
              self._send_over_alarm(self.config['flow_alarms'][1], percent_over)
            elif(flow_rate < lower_limit):
              percent_under = int((expected_flow - flow_rate) / expected_flow * 100)
              self._send_under_alarm(self.config['flow_alarms'][0], percent_under)
        else:
          self.logger.error('46 Invalid time stamp  and / or pulse count data')
    else:
      self.logger.error('47 pid of OS proc executing this script not recorded')


  def _irr_ev_should_continue(self):
    """
    Determine if the time has run out on this irrigation event

    Returns:
      None      something went wrong before check
      True      time has not expired on irrigation event
      False     time has expired on irrigation event
    """
    self.logger.info('entering: _irr_ev_should_continue()')

    result = None
    try:
      now_date_time = self.datetime.today()
      date_string   = self.config['date']
      time_string   = self.config['start']
      dur_string    = self.config['duration']
      irr_ev_start  = (self.datetime(year=int(date_string.split('-')[0]),
                                     month=int(date_string.split('-')[1]),
                                     day=int(date_string.split('-')[2]),
                                     hour=int(time_string.split(':')[0]),
                                     minute=int(time_string.split(':')[1])))
      irr_ev_stop = irr_ev_start + (self.timedelta(
                                      hours=int(dur_string.split(':')[0]),
                                      minutes=int(dur_string.split(':')[1])))
      if(irr_ev_stop < now_date_time):
        result = False
      else:
        result = True
    except Exception as e:
      self.logger.error(f'48 Exception: {e}')
    return(result)


  def _sensor_pulse_callback(self, channel):
    """
    This function is called each time a pulse signal is received on the
    PI 4 GPIO pin connected to the water flow sensor.  The function is
    called whether the OS process managing the irrigation event is 
    actively running or in a sleep state.  This function adds a new   
    datapoint to the pulse count data structure.
    """
    self.logger.info('entering: _sensor_pulse_callback()')

    time_stamp            = str(int(self.time.time()))
    self.pulse_count      += 1
    the_pid               = self.config['pid']
    pid_in_all_pulse_data = False

    for pid_data in self.all_pulse_data:
      if(the_pid in pid_data):
        pid_in_all_pulse_data = True
        pid_data[the_pid].append({time_stamp : self.pulse_count})
        self._check_flow_rate()
    
    if(not pid_in_all_pulse_data):
      self.all_pulse_data.append({the_pid : [{time_stamp : self.pulse_count}]})
      #since its first data point, dont call _check_flow_rate()
      
    if(self._check_for_shutdown_request()):
      self._stop_irr_event()


  def _manage_irr_event(self):
    """
    This function is the business end of a long running (e.g., 4 
    hour) OS process tha manages an irrigation event.  Managing 
    an irrigation event involves counting pulses from the water 
    flow sensor, scanning for under / over flow conditions, and 
    scanning for requests from other OS processes to shut down.
    
    This function will run tasks, sleep, run tasks, sleep, etc. 
    until time runs out for the duration of the irrigation event
    which can be cut short by a shutdown request from another OS 
    process.
    """
    self.logger.info('entering: _manage_irr_event()')

    should_be_irrigating = True

    while(should_be_irrigating):
      self.time.sleep(self.config['num_sleep_secs'])

      #save the pulse count data structure
      if(not self._write_pulse_count_file()):
        self.logger.error('49 could not checkpoint pulse count data structure')

      #check for semaphore signal
      if(self._check_for_shutdown_request()):
        should_be_irrigating = False

      #check to see if time expired for irr even
      if(self._irr_ev_should_continue()):
        should_be_irrigating = False

      if(not should_be_irrigating):
        self._stop_irr_event()


  def _set_config_data(self, file_path):
    """
    A helper function that is only invoked by an irr_event() utility
    object.  These utility objects are created to process orphaned 
    pulse count files.  The input parameter is used to set  the 
    <utility object>.config[] structure inside the irr_event() 
    utility object.  Refer to send_orphaned_data() for more 
    information.
     
    Args:
      file_path(str)     fully qualified direcotry path to orphan file 

    Returns:
      True               successfully loaded self.cofig[] data
      False              failed to load one or more self.config[] data items
    """
    self.logger.info('entering: _set_config_data()')

    result = False
    if(file_path):
      try:
        the_file = self.Path(file_path)
        if(the_file.is_file()):
            with the_file.open('r') as fd:
              contents = fd.read()
              if(self.df.check_object(json_object=contents)):
                contents_as_a_dict = self.json.loads(contents)
                data_section = contents_as_a_dict['data']
                self.config['date']     = data_section['date']
                self.config['sched_id'] = data_section['sched_id']
                self.config['sequence'] = data_section['sequence']
                self.config['block']    = data_section['block']
                result = True
        else:
          self.logger.error('50 could not open orphan pulse count file:' +
                            f' {file_path}')
      except Exception as e:
        self.logger.error(f'51 Exception: {e}')
    else:
      self.logger.error('52 bad argument passed to _set_config_data()')
    return(result)


  def _process_orphan_file(self, path, main_file, backup_file):
    """
    A helper function that is only invoked by an irr_event() utility
    object.  These utility objects are created to process orphaned 
    pulse count files.  The input parameters hold the orphaned pulse 
    count file name as well as the backup file name, if one exists.
    The runtime pulse count data structure, within the utility object,
    is cleared and then loaded with data from one of the flat files 
    specified by the input parameters.  Refer to send_orphaned_data() 
    for more information.

    Args:
      path(str)           fully qualified path of directory containing
                            pulse count files (and orphaned ones too)
      main_file(str)      file name of a pulse count file
      backuip_file(str)   fiel name for the backup to arg main_file

    Returns:
      False        could not process the orphaned pulse count file
      True         successfully processed the orphaned pulse count file
    """
    self.logger.info('entering: _process_orphan_file()')

    result = False
    try:
      self.all_pulse_data = []     #clear the data structure

      # process the pulse count file
      if(main_file):
        the_file_path = path + self.paths.divider + main_file
        
        if(self._read_pulse_count_file(file_path=the_file_path)):
          if(self._set_config_data(the_file_path)): 
            flow_rate = self._calculate_average_flow_rate()
            gals_disp = self._calculate_total_gals_disp()
            if(flow_rate and gals_disp):
              if(self._send_gals_disp(av_flow_rate=flow_rate, 
                                      total_gals_disp=gals_disp,
                                      date_str=self.config['date'].replace('-','_'))):
                result = True
                self.Path(the_file_path).unlink()
                if(backup_file):
                  the_file_path = path + self.paths.divider + backup_file
                  self.Path(the_file_path).unlink()
              else:
                self.logger.error('53 could not send gals_disp file for ' +
                                  f' orphan pulse file: {main_file}')
            else:
              self.logger.error('54 Could not calculate flow rate and/' +
                                'or total gallons dispensed for orphan' +
                                f' pulse file: {main_file}')
          else:
            self.logger.error('55 couldnt set irr_event.config[] ' +
                              'values needed to process orphan ' +
                              f'pulse file: {main_file}')
        else:
          self.logger.error('56 couldnt read orphaned pulse file ' +
                            'data into the irr_event().all_pulse_data[] ' +
                            'data structure for orphaned pulse count ' +
                            f'file: {main_file}')

      # handle case where processing pulse count file failed but a backup file
      # exists.  also case where no pulse count file exists but backup does
      if(not result and backup_file):
        the_file_path = path + self.paths.divider + backup_file
        if(self._read_pulse_count_file(file_path=the_file_path)):
          if(self._set_config_data(the_file_path)): 
            flow_rate = self._calculate_average_flow_rate()
            gals_disp = self._calculate_total_gals_disp()
            if(flow_rate and gals_disp):
              if(self._send_gals_disp(av_flow_rate=flow_rate, 
                                      total_gals_disp=gals_disp,
                                      date_str = self.config['date'].replace('-','_'))):
                result = True
                self.Path(the_file_path).unlink()
                if(main_file):
                  the_file_path = path + self.paths.divider + main_file
                  self.Path(the_file_path).unlink()
              else:
                self.logger.error('57 could not send gals_disp file for' +
                                  f' orphan pulse file: {backup_file}')
            else:
              self.logger.error('58 coult not calculate flow rate and /' +
                                ' or total gallons dispensed for orphan ' +
                                f'pulse file: {backup_file}')
          else:
            self.logger.error('59 couldnt set irr_event.config[] values ' +
                              'needed to process orphan pulse file:' +
                              f' {backup_file}')
        else:
          self.logger.error('60 couldnt read orphaned pulse file data' +
                            f' into the irr_event().all_pulse_data[] data' +
                            ' structure for orphaned pulse count file: ' +
                            f'{backup_file}')
    except Exception as e:
      self.logger.error(f'61 Exception: {e}')
    return(result)


  def send_orphaned_data(self):
    """
    This function was created to process one or more orphaned pulse
    count files.  Orphaned pulse count files are created when an 
    irr_event() object is taken down hard, not being allowed to
    shutdown gracefully and process pulse count data into data files
    destined for transmission to the AWS backend.  An attempt is made
    to process all orphaned pulse count files.  If an orphan cannot be
    processed it will be uploaed to an S3 bucket for future forensic 
    analysis.

    This function, as well as irr_event() processing in general, relies
    on strict naming standards for pulse count file (and all other 
    files.)  Example naming standard:

    pulse_count_<yyyy_mm_dd>_<sched_id>_<sequence>.json  
    pulse_count_<yyyy_mm_dd>_<sched_id>_<sequence>.json.old

    Returns:
      None           Issue before beginning
      True           Successfully processed and/or uploaded all orphans
      False          Failed to process / upload one or more orphans
    """
    self.logger.info('entering: send_orphaned_data()')

    result = None
    # build dict inventory of all orphaned pulse files and backup files
    path = self.paths.get_path('irr_ev_in_progress')
    if(path):
      directory  = self.Path(path)
      pulse_files = {}
      for item in directory.iterdir():
        if(item.is_file()):
          file_name_root = item.name.split('.')[0]
          if(not file_name_root in pulse_files):
            pulse_files[file_name_root] = {'file': None, 'backup':None}
          if(len(item.name.split('.')) > 2):
            pulse_files[file_name_root]['backup'] = item.name
          else:
            pulse_files[file_name_root]['file'] = item.name

      # process all orphans. successfully processed files are deleted.  
      # error(s) during processing results in upload to S3
      for file_name_root in pulse_files:
        main_file   = pulse_files[file_name_root]['file']
        backup_file = pulse_files[file_name_root]['backup']
        process_success = self._process_orphan_file(path, main_file, backup_file)
        
        # processing failed; clean up pulse files if they are stil present
        if(not process_success):
          if(main_file):
            the_file_path = path + self.paths.divider + main_file
            file = self.Path(the_file_path)
            if(file.is_file()):
              self._move_file(file, 'orphans')
              if(backup_file):
                the_file_path = path + self.paths.divider + backup_file
                file = self.Path(the_file_path)
                if(file.is_file()):
                  file.unlink()
          elif(backup_file):
            the_file_path = path + self.paths.divider + backup_file
            file = self.Path(the_file_path)
            if(file.is_file()):
              self._move_file(file, 'orphans')
    else:
      self.logger.error('62 Couldnt get directory for irrigation event' +
                        ' in progress')
    return(result)


  def _stop_irr_event(self):
    """
    Perform orderly shut down of the management of an irrigation event.
    This involves closing the valve, calculating the total gallons 
    dispensed during the irrigation event, calculating the average flow
    rate for the irrigation event and sending this data to the AWS
    backend.
    """
    self.logger.info('entering: _stop_irr_event()')

    # set two variables used in multiple places below
    main_file = self.config.get('pulse_file_path', None)
    backup_file = self.config.get('old_pulse_file_path', None)
    if(main_file):
      main_file = self.Path(main_file)
    if(backup_file):
      backup_file = self.Path(backup_file)

    self._close_all_valves()

    # flush data struct of any newly arrive data since last save to file
    if(not self._write_pulse_count_file()):
      self.logger.error('63 could not checkpoint the pulse count data' +
                        ' structure')
    
    # process pulse count data structure, creating a gals_disp file
    flow_rate = self._calculate_average_flow_rate()
    gals_disp = self._calculate_total_gals_disp()
    if((flow_rate == None) or (gals_disp == None)):
      self.logger.error('64 issue calculating total gals disp or avg flow rate')
      if(main_file):
        if(self._move_file(main_file, 'corrupt_files')):
          if(backup_file):
            backup_file.unlink()
        else:
          if(backup_file):
            if(not self._move_file(backup_file, 'corrupt_files')):
              self.logger.error('65 could not move damanged main file or ' +
                                'backup file to corrupt_files for pulse ' +
                                f'count file: {main_file.name}')
          else:
            self.logger.error('66 could not move damanged main file to ' +
                              'corrupt_files for the pulse count file: ' +
                              f'{main_file.name}')
    else:
      if(not self._send_gals_disp(av_flow_rate=flow_rate, 
                                  total_gals_disp=gals_disp)):
        self.logger.error('67 could not send gals disp message to AWS backend')
        if(main_file):
          if(self._move_file(main_file, 'orphans')):
            if(backup_file):
              backup_file.unlink()
          else:
            if(backup_file):
              if(not self._move_file(backup_file, 'orphans')):
                self.logger.error('68 could not move unsendable main file ' +
                                  'or backup file to orphans for pulse count ' +
                                  f'file: {main_file.name}')
            else:
              self.logger.error('69 could not move unsendable main file to ' +
                                'orphans for the pulse count file: ' +
                                f'{main_file.name}')
        elif(backup_file):
          if(not self._move_file(backup_file, 'orphans')):
            self.logger.error('70 could not move unsendable backup file to' +
                              ' orphans for pulse count file: ' +
                              f'{main_file.name}')
      else:
        if(main_file):
          main_file.unlink()
        if(backup_file):
          backup_file.unlink()
    
    # clean up irrigation event directories
    self.send_orphaned_data()
    self._clear_directory(self.paths.get_path('irr_event'))
    self._clear_directory(self.paths.get_path('irr_ev_in_progress'))


  def start_irr_ev(self, irr_ev_detail):
    """
    When an OS process, that is executing irr_cntrl.py, makes the
    decision to become a long running OS process so that it can 
    supervise an irrigaiton event, it will create an irr_event() 
    object.  The constructor will initialize a portion of the object
    and this function, which will be called at the start or irrigation
    event managment,  will finish off the initialization of the 
    irr_event object.
    
    Args:
      {}        contains all available details for the irr ev
    """
    self.logger.info('entering: start_irr_ev()')

    self._close_all_valves()     #to be bullet proof
    try:
      self.config['start']          = irr_ev_detail['start']          #'hh:mm'
      self.config['duration']       = irr_ev_detail['duration']       #'hh:mm'
      self.config['exp_flow']       = irr_ev_detail['exp_flow']       #int(gpm)
      self.config['under_flow_tol'] = irr_ev_detail['under_flow_tol'] #int(%)
      self.config['over_flow_tol']  = irr_ev_detail['over_flow_tol']  #int(%)
      self.config['sched_id']       = irr_ev_detail['sched_id']       #int
      self.config['sequence']       = irr_ev_detail['sequence']       #int
      self.config['block']          = irr_ev_detail['block']          #'a'-'g'
      self.config['date']           = irr_ev_detail['date']  #'yyyy-mm-dd'
      self.config['day']            = irr_ev_detail['day']   #'day1'-'day3' or
                                                             #  'sun'-'sat'
      #build name of pulse file and old pulse file
      self.config['date'] = self.config['date'].replace('-','_')
      pulse_file = 'pulse_count_' + self.config['date'] + '_'
      pulse_file += str(self.config['sched_id']) + '_'
      pulse_file += str(self.config['sequence']) + '.json'
      self.config['pulse_file'] = pulse_file

      #build full path to pulse file and old pulse file
      pulse_file_path = self.paths.get_path('irr_ev_in_progress')
      pulse_file_path += self.paths.divider + pulse_file
      self.config['pulse_file_path'] = pulse_file_path
      self.config['old_pulse_file_path'] = pulse_file_path + '.old'

      self._read_pulse_count_file() #load any prior pulse data that might exist

      self._open_valve(self.config['block'])

      if(not self._send_gals_disp(total_gals_disp=1)):
        self.logger.error('71 could not write 1 gal disp file for block: ' +
                          f'{self.config["block"]}')

      self._manage_irr_event()  #become a long running OS process
    except Exception as e:
      self.logger.error('72 Could not start irrigation event process. ' +
                        f'Exception: {e}')
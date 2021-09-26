"""
Jaye Hicks 2021

Obligatory legal disclaimer:
  You are free to use this source code (this file and all other files 
  referenced in this file) "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER 
  EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. 
  THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THIS SOURCE CODE
  IS WITH YOU.  SHOULD THE SOURCE CODE PROVE DEFECTIVE, YOU ASSUME THE
  COST OF ALL NECESSARY SERVICING, REPAIR OR CORRECTION. See the GNU 
  GENERAL PUBLIC LICENSE Version 3, 29 June 2007 for more details.

This Python module will be executed on the PI 4 platform on a regular
schedule (i.e., a linux cron job).  In most cases it will perform some
basic activities and exit relatively quickly.  However, in some cases
the execution of this script will result in a long running OS process
that manages an irrigatin event.  It is quite normal for two versions
of this script to run at the same time on the PI 4.  In such a case 
the older OS process is managing an irrigation event (i.e., receiving 
data from the water flow sensor) and the newer OS process is checking
in on things.  In one possible scenario, the newer process may 
obtain an irrigation schedule update from the AWS backend and then, 
by following the new irrigation schedule, request that the older OS 
process shutdown so that the newer OS process can begin to manage the 
irrigation of a different vineyard block.

At the conclusion of an irrigation event the AWS backend will be
informed as to the total gallons distributed to the block as well and
the average flow rate observed across the entire irrigation event.

Gallons Dispened Files
  There is a 1 gallon message that is sent once an irrigation event
  has commenced.  This keeps the intelligent scheduling algorithm
  running on the AWS backend from scheduling the same block more
  than once within a 24 hour period.  BIG RULE: if a block has recieved
  any amount of irrigation it cannot be scheduled again for irrigation
  for the next 24 hours.  A 1 gallon message sent to the AWS backend
  at the initiation of an irrigation event provides the necessary data
  to enable adherence to this rule.

  There is a second type of gallons dispensed message sent to the AWS
  backend - to total number of gallons dispensed to the vineyard block
  as a result of an irrigation event.  

  With respect to the AWS backend processing, all galons dispensed 
  messages are idempotent (i.e., messages can be processed multiple
  times without any adverse side effects)

Pulse Count Files
  Pulse count files track the  number of pulses received from the water
  flow sensor.  The flow sensor has been physically plumbed in-line
  between the the water inlet and the valve controller.  The valve
  controller takes signals from the GPIO pins of the PI 4 to open and
  close valves and by doing so, start irrigation / stop irrigation to 
  the selected vineyard block.

  Unexpected reboots can occur during an irrigaiton event as the
  irrigaiton events can run for a relatively long period of time 
  (e.g., 4 hours).  In such a case, a single irrigaiton event will
  have multiple pulse count files associated with it.

"""
import                 logging
import                 time
import                 sys
from   datetime import datetime, timedelta
from   pathlib  import Path

import dura_file
import process_cntrl
import comms_check
import check_date_time
import alarms
import gals_disp
import upload_files
import irr_sched
import irr_event
import lv_paths


def gen_log_file_name():
  """
  This irr_cntrl.py script executes on a regular, scheduled basis.  A 
  new system logging file is used for each invocation.  This function 
  generates a unique file name for each new system log file.  The
  encoding of date and time allow the system log files to be curated
  by an automated process.

  NOTE: hard-coded directory paths exist as the system log file has to
        be named before a call can be made to the class that avoids
        hard-coded directory paths.  Kind of a 'chicken before the egg'
  
  Example system logging file:
  2021_08_21_23_05_irr_cntrl.log
  """
  if(sys.platform == 'win32'):   # development
    prefix =  'C:\\Development\\LonesomeVine\\Irrigation\\lonesome\\sys_logs\\'
  else:                          # production
    prefix = '/home/pi/lonesome/sys_logs/'

  module_name = 'irr_cntrl'
  try:
    now          = datetime.today()
    file_name    = (str(now.year) + '_' +
                    str(now.month).zfill(2) + '_' +
                    str(now.day).zfill(2) + '_' +
                    str(now.hour).zfill(2) + '_' +
                    str(now.minute).zfill(2) + '_' +
                        module_name + '.log')
    the_log_file_name = prefix + file_name
  except Exception as e:
    print(f'datetime() exception: {e}')
  return(the_log_file_name)


"""
Set up system logging
"""
logging.basicConfig(level=logging.INFO, 
                    filename=gen_log_file_name(), 
                    format='%(asctime)s %(name)s %(levelname)s:%(message)s')


def _curr_date_as_string():
  """self explanatory
  """
  logging.info('entering: _curr_date_as_string()')
  try:
    now_date_time = datetime.today()
    now_date_str  = ( str(now_date_time.year) + '_' +
                      str(now_date_time.month).zfill(2) + '_' +
                      str(now_date_time.day).zfill(2) )
  except Exception as e:
    logging.error(f'36 datetime Exception: {e}')
  return(now_date_str)


def _curr_date_time_as_string():
  """self explanatory
  """
  logging.info('entering: _curr_date_time_as_string()')
  try:
    now_date_time      = datetime.today()
    now_date_time_str  = (str(now_date_time.year) + '_' +
                          str(now_date_time.month).zfill(2) + '_' +
                          str(now_date_time.day).zfill(2) + '_' +
                          str(now_date_time.hour).zfill(2) + '_' +
                          str(now_date_time.minute).zfill(2))
  except Exception as e:
    logging.error(f'37 datetime Exception: {e}')
  return(now_date_time_str)


def _number_files_in_dir(path):
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
  logging.info('entering: _number_files_in_dir()')
  file_count = None

  if(path):
    try:
      directory  = Path(path)
      dir_name   = directory.name
      file_count = 0
      for item in directory.iterdir():
        if(item.is_file()):
          file_count += 1
    except Exception as e:
      file_count = False
      logging.error(f'1 Issue accessing dir: {dir_name} Exception: {e}')
  else:
    logging.error('2 Bad argument passed to _number_files_in_dir')
  return(file_count)


def _clear_directory(path):
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
  logging.info('entering: _clear_directory()')
  result = None
  
  if(path and (type(path) == str)):
    try:
      directory = Path(path)
      dir_name = directory.name
      if(directory.is_dir()):
        result = True
        for item in directory.iterdir():
          if(item.is_file()):
            item.unlink()
    except Exception as e:
      result = False
      logging.error(f'3 Could not clear a directory: {dir_name}.' + 
                    f' Exception: {e}')
  else:
    logging.error('4 Bad argument sent to _clear_directory()')
  return(result)


def _write_semaphore(semaphore_file_name):
  """
  Place the semaphore file in the directory that contains the process
  register.  Long running processes that manage irrigation events are 
  designed to regularly check this direcotry for this semaphore flat 
  file.  If a long running process detects the semaphore file it should  
  exit as quickly as possible.

  Note the dura_file.write_data() function contains a
  sys.stdout.flush() call after the file write command
  """
  logging.info('entering: _write_semaphore()')

  _delete_semaphore(semaphore_file_name)  #clear out potential residual
  try:
    an_lv_paths_obj = lv_paths.lv_paths()
    directory = an_lv_paths_obj.get_path('irr_event')
    if(directory):
      a_dura_file_obj = dura_file.dura_file()
      file_contents = {'action' : 'stop-an-irrigation-event'}
      file_name = (directory + an_lv_paths_obj.divider + semaphore_file_name)
      a_dura_file_obj.write_data(file_name, file_contents)
    else:
      logging.error('5 Could not access directory for the irr ev.')
  except Exception as e:
    logging.error(f'6 Issue accessing irr ev folder. Exception: {e}')


def _delete_semaphore(semaphore_file_name='stop_irr.json'):
  """
  Place the semaphore file in the directory that contains the process
  register.  Long running processes that manage irrigation events are 
  designed to regularly check this direcotry for this semaphore flat 
  file.  If a long running process detects the semaphore file it should  
  exit as quickly as possible.
  """
  logging.info('entering: _delete_semaphore()')
  try:
    an_lv_paths_obj = lv_paths.lv_paths()
    directory = an_lv_paths_obj.get_path('irr_event')
    if(directory):
      file_name = (directory + an_lv_paths_obj.divider + 
                   semaphore_file_name)
      file = Path(file_name)
      if(file.is_file()):
        file.unlink()
    else:
      logging.error('8 Could not access directory for irr ev.')
  except Exception as e:
    logging.error(f'9 Issue accessing irr ev folder. Exception: {e}')


def _stop_process_using_semaphore(semaphore_file_name, 
                                  num_wait_cycles, wait_time):
  """
  Give the process supervising the irrigation process the chance to
  such itself down. This function will  wait for a set number of wait 
  time cycles, giving the targeted OS process sufficient time to 
  recognize the semaphore and voluntarily exit.
  """
  logging.info('entering: _stop_process_using_semaphore()')
  result = False
  a_proc_cntrl_obj = process_cntrl.process_cntrl()
  if(a_proc_cntrl_obj.get_register() != None):
    _write_semaphore(semaphore_file_name)
    while(num_wait_cycles != 0):
      time.sleep(wait_time)
      if(a_proc_cntrl_obj.get_register() == None):
        result = True
        break
      else:
        num_wait_cycles -= 1
  else:
    result = True
  return(result)


def _stop_process_using_kill():
  """
  This function will be called if the process that needs to be taken
  down (i.e., one managing an irrigation event) did not respond
  to the semaphore placed in the process register directory by 
  exiting voluntarily.  This function will issue use the OS kill
  command to take stop all processing that exist in the process
  register.  At this time (i.e., lonesome vine solution design 
  perspective) there should only be a singel process in the process
  register.
  """
  logging.info('entering: _stop_process_using_kill()')
  result = False
  a_proc_cntrl_obj = process_cntrl.process_cntrl()
  a_proc_cntrl_obj.refresh_register()
  if(a_proc_cntrl_obj.get_register() != None):
    if(not a_proc_cntrl_obj.clear_entire_register()):
      logging.error('10 Could not kill the process with OK kill command.')
    else:
      result = True
  else:
    result = True
  return(result)


def _stop_current_irr_ev():
  """
  Stop an OS process that is currently running and managing an
  irrigation event.  First attempt to signal the OS process and
  ask it to shut itself down in an orderly fashion.  If that fails
  then take it down hard with an OS level 'kill' command

  Returns:
    None          issue arose before attempt to stop OS process
    False         could not stop OS process
    True          successfully stopped OS process
  """
  logging.info('entering: _stop_current_irr_ev()')
  num_wait_cycles     = 3
  wait_time_secs      = 5
  semaphore_file_name = 'stop_irr.json'

  if(not _stop_process_using_semaphore(semaphore_file_name,
                                       num_wait_cycles, wait_time_secs)):
    if(not _stop_process_using_kill()):
      result = False
      logging.error('11 Couldnt stop current irr ev OS process')
    else:
      result = True
  else:
      result = True
  _delete_semaphore(semaphore_file_name)
  return(result)


def _get_new_irr_sched():
  """
  The PI4 regularly requests the most up to date irrigation schedule
  from the AWS backend.  Data updates or vineyard operator GUI action
  can, at any time, cause the current irrigation schedule to become
  obsolete in favor or a new irrigation schedule.  For a given point in
  time an irrigaiton schdule can dictate irrigaiton of a vineyard block
  or no irrigation of any vineyard block.
  
  When this function requests the most up to date schedule from the AWS
  backend it, it sends the id of the irrigation schedule that is
  currently in force on the PI 4.  It also send in the PI 4's perception
  of the current date and time.  With this input the AWS backend can 
  track what irrigation schedule is in force on the PI 4 and respond to
  request for new schedules with either the most up to date irrigation
  schedule or the message message 'no newer schedule avail.')  The AWS
  backend will also be able to detect the contition where the PI 4's 
  setting for current date / time is inaccurate beyond acceptable
  tolerance.
  """
  logging.info('entering: _get_new_irr_sched()')
  irr_sched.irr_sched().get_schedule()


def _execute_schedule():
  """
  Accessing the most up-to-date irrigaiton schedule available (i.e., 
  the irrigation schedule that is currently in force) and execute
  it provided it is valid an not expired.

  Note: the execution of the irrigation schedule will involve using 
  irr_event objects.  An irr_event can do two things that should be
  clearly understood here.  First, it might stop a running PI 4 OS
  process (e.g., stop a process that is supervising an irrigation 
  event).  Second, the call to irr_evetn.start_irr_ev() will turn 
  the PI 4 OS process that is running this function _execute_schedule()
  to morph into a long running process, perhaps running for 4 hours, 
  that manages (i.e., opens a valve and tracks water flow) an 
  individual irrigation event.
  """
  logging.info('entering: _execute_schedule()')
  a_sched_obj   = irr_sched.irr_sched()
  an_irr_ev_obj = irr_event.irr_event()

  _delete_semaphore()   # clear out any prior shutdown signal

  curr_sched_usable = a_sched_obj.curr_sched_usable()
  if(curr_sched_usable):
    curr_irr_ev = a_sched_obj.irr_ev_underway()
    scheduled_irr_ev = a_sched_obj.irr_ev_should_be_underway()

    if(scheduled_irr_ev == False):
      if(curr_irr_ev):
        if(not _stop_current_irr_ev()):
          logging.error('12 Could not stop current irrigation event: ' +
                        f'{curr_irr_ev}')
    elif(scheduled_irr_ev):
      if(curr_irr_ev == scheduled_irr_ev):
        pass
      else:
        if(curr_irr_ev):
          if(not _stop_current_irr_ev()):
            logging.error('13 Could not stop current irrigation event: ' +
                          f'{curr_irr_ev}')
          else:
            irr_ev_detail = a_sched_obj.get_irr_ev_details(scheduled_irr_ev)
            if(irr_ev_detail):
              an_irr_ev_obj.start_irr_ev(irr_ev_detail) #becomes long running 
            else:
              logging.error('14 Could retrieve irrigation event details')
        elif(curr_irr_ev == False):
          irr_ev_detail = a_sched_obj.get_irr_ev_details(scheduled_irr_ev)
          if(irr_ev_detail):
            an_irr_ev_obj.start_irr_ev(irr_ev_detail) #becomes long running
          else:
            logging.error('15 Could not retrieve irrigation event details')
        else:
          logging.error('16 Couldnt determine irrigation event currently in' +
                        ' process')
  elif(curr_sched_usable == None):
    logging.error('17 Couldnt determine if current schedule is usable or not')
    a_sched_obj.software_reset()
  else:
    logging.error('18 Curr sched out of date or no irr sched retrievable')
    a_sched_obj.software_reset()


def _send_outbound_data():
  """
  Send gallons dispsensed data, alarm conditions detected, and upload
  suspect files that require forensic analysis.
  """
  logging.info('entering: _send_outbound_data()')

  if(gals_disp.gals_disp().send(wait=True) == False):
    logging.error('19 Failed attempt sending gals disp data to AWS')

  if(alarms.alarms().send == False):
    logging.error('20 Failed attempt sending alarms data to AWS')

  if(upload_files.upload_files().send == False):
    logging.error('21 Failed attempt uploading files to AWS')


def _date_time_check_current():
  """
  This function is called to catch the alarm condition of ongoing
  inability to corroborate the PI 4s perception of current date and
  time with a trusted, external Internet endpoint.  The 
  _date_time_sanity_check() call does not have to succeed everytime.  
  However, if the amount of elapsed time since the last successfull 
  contact with a trusrted, external Internet endpoint exceeds the 
  threshold then raise an alarm.
  """
  logging.info('entering: _date_time_check_current()')
  days_threshold = 5
  an_lv_paths_obj = lv_paths.lv_paths()
  
  path = an_lv_paths_obj.get_path('time_synch')
  if(path):
    num_files = _number_files_in_dir(path)

    if(num_files == 1):
      directory  = Path(path)
      for item in directory.iterdir():
        if(item.is_file()):
          file_name = item.name
          lc_year  = int(file_name.split('_')[3])
          lc_month = int(file_name.split('_')[4])
          lc_day   = int(file_name.split('_')[5])
          lc_hour  = int(file_name.split('_')[6])
          lc_min   = int(file_name.split('_')[7].split('.')[0]) 
        
          now        = datetime.today()
          last_check = datetime(year=lc_year, month=lc_month, day=lc_day,
                                hour=lc_hour, minute=lc_min)

          if(not (last_check + timedelta(days=days_threshold)) > now):

            #construct file name
            date_string = _curr_date_as_string()
            file_name   = 'alarm_pi4_datetimecurr_' + date_string + '.json'

            #construct file contents
            message = {'whatami'  : 'pi4-datetime-alarm-not-current',
                        'date'     : date_string}
            
            #write file to special purpose comms dir
            path = an_lv_paths_obj.get_path('alarms')
            if(path):
              try:
                path_and_file_name = path + an_lv_paths_obj.divider + file_name
                if(not Path(path_and_file_name).is_file()):
                  dura_file.dura_file().write_data(path_and_file_name, message)
              except Exception as e:
                logging.error(f'22 Exception: {e}')
            else:
              logging.error('23 Couldnt get directory path for alarms ' +
                            'directory')

          break # should only be 1 file in dir; so why not break?
    elif(num_files == 0):
      #send warn/error message to system log but not to AWS backend
      logging.error('24 Error: no control file present in date time synch ' +
                    'directory')
    elif(num_files > 1):
      #send warn/error message to system log but not to AWS backend
      logging.error('25 Error: only one file is allowed in date time synch ' +
                    'directory. All existing files will be deleted.')
      _clear_directory(path)
    else:
      logging.error('26 Could not determine how many files in date time ' +
                    'synch directory')
  else:
    logging.error('27 could not access the date time synch directory')


def _date_time_sanity_check():
  """
  Using a publicly accessable Internet endpoint, obtain the current
  date and time.  If the PI 4 verson of date and time varies beyond
  the acceptable threshold, raise an alarm.

  The PI 4's default behaviour is to use a public Internet endpoint
  to set its local version of date and time.  It does this every 5
  mins.  So, in the event that this script is being run directly 
  following ann unexpected reboot, a delay > 5 mins is executed
  before the check in order to give the PI 4 a chance to set its
  local date and time from a public Internet endpoint.
  """
  logging.info('entering: _date_time_sanity_check()')

  an_lv_paths_obj = lv_paths.lv_paths()
  secs_delay_before_check = 420      #'2' is a good debug setting 
  file_name_prefix        = 'date_time_synch_'

  time.sleep(secs_delay_before_check)
  result = check_date_time.check_date_time().check()
  if(result == False):
    logging.error('28 PI 4 date / time value out of acceptable variance')

    #construct file name
    date_string = _curr_date_as_string()
    file_name   = 'alarm_pi4_datetimesynch_' + date_string + '.json'

    #construct file contents
    message = {'whatami'  : 'pi4-datetime-alarm-not-synched',
                'date'     : date_string}
    
    #write file to special purpose comms dir
    path = an_lv_paths_obj.get_path('alarms')
    if(path):
      try:
        path_and_file_name = path + an_lv_paths_obj.divider + file_name
        if(not Path(path_and_file_name).is_file()):
          dura_file.dura_file().write_data(path_and_file_name, message)
      except Exception as e:
        logging.error(f'29 Exception: {e}')
    else:
      logging.error('30 Couldnt get directory path for alarms directory')

  elif(result == None):
    logging.error('31 Some issue prevented execution of date / time check')

  #place file in directory to document last successful date/time
  #synch with an external source.  File name contains date and time  
  else:
    try:
      directory = an_lv_paths_obj.get_path('time_synch')
      if(directory):
         #clear out directory
        _clear_directory(directory)   

        #construct the file name
        now_date_time_str  = _curr_date_time_as_string()
        file_name     = file_name_prefix + now_date_time_str + '.json'
        file_contents = {'action' : 'successful-date-time-synch'}
        file_name     = (directory + an_lv_paths_obj.divider + file_name)
        dura_file.dura_file().write_data(file_name, file_contents)
      else:
        logging.error('32 Could not access date time synch directory.')
    except Exception as e:
      logging.error(f'33 Exception: {e}')  


def task_list():
  """
  These are the major tasks which must be performed regularly (e.g., 
  every 5 mins) on an ongoing basis in order to automatically manage
  vineyard irrigation
  """
  logging.info('entering: task_list()')

  an_lv_object = lv_paths.lv_paths()
  an_alarm = alarms.alarms()
  cd = check_date_time.check_date_time()
  cc = comms_check.comms_check()
  df = dura_file.dura_file()
  gd = gals_disp.gals_disp()
  ie = irr_event.irr_event()
  irs = irr_sched.irr_sched()
  pc = process_cntrl.process_cntrl()
  uf = upload_files.upload_files()


if(__name__ == '__main__'):
  task_list()
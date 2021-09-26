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

Objects of type process_cntrl() are used to manage the exeuction of
concurrent Python scripts that oversee the irrigation of a vineyard
block.  A process_cntrl() object can create, manage, and provide access
to a process register (i.e., flat file residing in a specific
directory.)  This file tracks the OS pids of Python scripts that are
executing and supervising the irrigation of a vineyard block.  As
irrigaiton is a relatively long running event (e.g., 4 hour) it is
sometimes necessary to stop irrigation before the scheduled completion
time.  Object's of type process_cntrl() can assist in stopping
irrigation should it become necessary to use  an OS kill command.

The process register flat file is augmented with dura_file
functionality to help detect any potential issues with the file.
Objects of type process_cntrl() work equally well across Windows
10 and Linux.  The class auto detects the platform that is is 
running on.  

The process_cntrl class is simple / single-threaded; it makes no 
provisions to support multithreaded accesses to the process register 
flat file.  This functionality can be added easiy enough in the future 
if the need arises to warrant the inclusion of the extra libraries.

The structure of the flat file:
{<'pid'>:{'irr_ev_date': <'date'>, 'irr_ev_seq': <sequence number>,
'irr_sch_id': <schedule id>}}

An example: {"12345": {"irr_ev_date": "2021-10-31", "irr_ev_seq": 2, 
                       "irr_sch_id": 65}}
"""
class process_cntrl():
  """
  Create an object of class process_cntrl() and use it to:
  1) determine if any active processes are supervising irrigation
  2) register newly created process that will supervise irrigation
  3) exeucte a hard shutdown of a processess (e.g., one supervising an
     irrigation event) that will not respond to semaphoric requests to
     terminite (i.e., request occurs outside of process_cntrl() class)

  """
  import logging
  import os
  import sys
  import psutil

  import lv_paths       # directory paths to locations used in irr man
  import dura_file      # file complete/correct guaranteed w hash value


  def __init__(self):
    """
    """
    self.logger = self.logging.getLogger(__name__)

    self.logger.info('entering: __init__()')    
    self.paths = self.lv_paths.lv_paths()
    self.reg_file_path = self.paths.get_path('process_reg')
    if(self.reg_file_path):
      self.reg_file_path += self.paths.divider
      self.reg_file_path += 'lv_irr_proc_reg.json'
    else:
      self.logger.error('1 Couldnt establish dir path to process register.')

    self.df = self.dura_file.dura_file()


  def get_register(self):
    """
    Read the Irrigation Process Register from its flat file. Normal
    processing includes the case in which the process register flat
    file does not exist; there are no active OS processes running and
    supervising an irrigation event (i.e., register is empty)

    Return:
      None      No Irrigation Process Register file exists
      False     Could not access Irrigation Process Register file
      <dict>    Python dictionary containing Irrigation Register. From
                a dura_file() perspective, this is just the value side
                of the 'data' key in the special dura_file format.

      The Python dict (i.e., a single entry in register)
        {<'pid'>:{'irr_ev_date': <'date'>, 
                  'irr_ev_seq': <sequence number>,
                  'irr_sch_id': <scheduel id>}}
        --example-- 
          {"12345": {"irr_ev_date": "2021-10-31", 
                     "irr_ev_seq": 2, 
                     "irr_sch_id": 65}}
    """
    self.logger.info('entering: get_register()')  

    register = None 
    if(self.reg_file_path):
      register = self.df.read_data(self.reg_file_path)
      if(register == None):
        self.logger.info('2 No irrigation process register file exists.')
      elif(register == False):
        self.logger.error('3 Could not read irrigaton process register file.')
    else:
      self.logger.error('4 Directory path to process register not set.')
    return(register)


  def put_register(self, register):
    """
    Write the Irrigation Process Register to a flat file

    Args:
      register     Python dict containing the complete register.

        Format of a single entry of the register:
          {<'pid'>:{'irr_ev_date': <'date'>, 
                    'irr_ev_seq': <sequence number>,
                    'irr_sch_id': <scheduel id>}}
        Example of a single entry of the register:
            {"12345": {"irr_ev_date": "2021-10-31", 
                      "irr_ev_seq": 2, 
                      "irr_sch_id": 65}}
    Returns:
      None      issue before write attempt
      True      successfully wrote dict to register file
      False     failed to write dict to register file
    """
    self.logger.info('entering: put_register()')  

    result = None
    if(self.reg_file_path):
      result = self.df.write_data(self.reg_file_path, register)
    else:
      self.logger.error('5 Directory path to process register not set.')
    return(result)
  

  def _delete_reg_file(self):
    """
    Delete the flat file used to house the Irrigation Process Register.
    CAUTION: before calling this function, ensure that regiser is empty
    or that you no longer need to track any of the OS processes that 
    the registery tracks.
    """
    self.logger.info('entering: _delete_reg_file()')  

    result = False
    if(self.reg_file_path):
      try:
        self.os.remove(self.reg_file_path)
        result = True
      except Exception as e:
        self.logger.error(f'6 Could not delete register file. Exception: {e}')
    else:
      self.logger.error('7 Directory path to process register not set.')
    return(result)
    

  def clear_entire_register(self):
    """
    Run through all entries contained in the Irrigation Process
    Register, stopping any processes that are running, and delete
    all entries.
    
    Returns:
      None    no action taken (likely no reg flat file exists)
      False   could not read register flat file
                or failed to kill one or more processes
                or failed to delete register flat file
      True    killed all processes and deleted flat file
    """
    self.logger.info('entering: clear_entire_register()')  

    result = True
    register = self.get_register()
    if(register):
      for pid, data in register.items():
        if(self.is_pid_running(pid)):
          if(not self.kill_pid(pid)):
            result = False
            break
      if(result):
        result = self._delete_reg_file()
    else:
      result = register

    return(result)
    

  def refresh_register(self):
    """
    Cycle through Irrigation Process Register removing entries
    beloning to OS processes that are no longer executing.  If the last
    entry in the register is removed (i.e., it is now empty) then the
    flat file used to store the register will be deleted
    
    Returns:
      None    no register exists so no action taken
      True    update successful or no Irrigation Process Register
      False   update unsuccessful
    """
    self.logger.info('entering: refresh_register()')  

    result = None
    register = self.get_register()
    if(register):
      for pid, data in register.items():
        if(self.is_pid_running(pid) == False):
          if(self.delete_pid_from_reg(pid) == False):
            result = False
            break
      result = True
    else:
      result = register
    return(result)


  def kill_pid(self, pid):
    """
    Terminate a running OS process that has a pid equal to the argument
    'pid.'  Direclty after executing this function a call should be 
    made to refresh_register()

    Args:
      pid(str)

    Returns:
      None    bad input parameter or no OS process with pid
      True    successfully killed pid
      False   could not kill pid
    """
    self.logger.info('entering: kill_pid()')  

    result = None
    if((pid) and (type(pid) == str)):
      try:
        pid = pid.strip()
        if(int(pid) > 0):
          if(self.is_pid_running(pid)):
            try:
              process = self.psutil.Process(int(pid))
              process.terminate()
              result = True
            except Exception as e:
              result = False
              self.logger.error(f'8 Could not kill pid: {pid}. Exception: {e}')
          elif(self.is_pid_running(pid) == False):
            self.logger.error(f'9 No process exists with pid: {pid}')
        else:
          self.logger.error('10 Invalid pid: pids must be positive integers.')
      except Exception as e:
        self.logger.error(f'11 Bad argument.  Exception: {e}')
    return(result) 


  def is_pid_running(self, pid):
    """
    Determine if any running OS process has a pid that is equal to the
    argument 'pid'

    Args:
      pid(str)    OS process id of OS process to stop/kill

    Returns
      None    bad input parameter
      True    An OS process wiht pid is currently running
      False   No OS process with pid
    """
    self.logger.info('entering: is_pid_running()')  

    result = None
    if((pid) and (type(pid) == str)):
      try:
        pid = pid.strip()
        if(int(pid) > 0):
          if(self.psutil.pid_exists(int(pid))):
            result = True
          else:
            result = False
        else:
          self.logger.error('12 Invalid pid: pids must be positive integers.')
      except Exception as e:
        self.logger.error(f'13 Bad argument.  Exception: {e}')
    return(result) 


  def kill_pid_and_delete_pid_from_reg(self, pid):
    """
    Kill the OS process specified by argument 'pid' and then remove
    pid from the register

    Note: leading / trailing whitespace trimmed from argument

    Returns:
      None    bad argument or pid not in register or no register exists
      False   could not kill the process or delete pid from register
      True    successfully killed the process and deleted pid from reg
    """
    self.logger.info('entering: kill_pid_and_delete_pid_from_reg()')  

    result = None
    if((pid) and (type(pid) == str)):
      try:
        pid = pid.strip()
        if(int(pid) > 0):
          if(self.is_pid_running(pid)):
            result = self.kill_pid(pid)
            if(result == True):
              result = self.delete_pid_from_reg(pid)
        else:
          self.logger.error('14 Invalid pid: pids must be positive integers.')
      except Exception as e:
        self.logger.error(f'15 Bad argument.  Exception: {e}')
    return(result)


  def is_pid_in_reg(self, pid):
    """
    Determine if the Irrigation Process Register contains an entry with
    a pid equal to the input parameter 'pid'

    Args:
      pid (str)   process id of entry to remove from register

    Returns:
      None     bad pid input parameter
                 or damaged / corrupt register
                 or register nonexistant
               currently no register file exists
      True     <pid> exists in register
      False    <pid> does not exist in register
    """
    self.logger.info('entering: is_pid_in_reg()')  

    result = None
    if((pid) and (type(pid) == str)):
      try:
        pid = pid.strip()
        if(int(pid) > 0):
          register = self.get_register()
          if(register == None):
            self.logger.info('16 Irrigation Process Register file ' +
                              'does not exist.')
          elif(register == False):
            self.logger.error('17 Couldnt retrieve register info ' +
                              'from flat file')
          else:
            if(pid in register):
              result = True
            else:
              result = False
        else:
          self.logger.error('18 Invalid pid: pids must be positive integers.')
      except Exception as e:
        self.logger.error(f'19 Bad argument.  Exception: {e}')
    return(result)


  def delete_pid_from_reg(self, pid):
    """
    Delete a single OS process entry from Irrigation Process Register.
    If the last pid entry is deleted from  the register, the flat file
    used to store the register will be deleted.  
    
    CAUTION: do not delete a pid from the register unless it is
    no longer running or you no longer need to track it with the 
    register.  If you regularly orphan processes you will eventually
    have to reboot the PI4 to remove them.

    Args:
      pid (str)   process id of entry to remove from register

    Returns:
      None     bad pid input parameter
                 or no register file exists
                 or register does not contain pid entry
      True     successfully removed pid
                 or register file does not exist
      False    failed to remove pid
    """
    self.logger.info('entering: delete_pid_from_reg()')  

    result = None
    if((pid) and (type(pid) == str)):
      try:
        pid = pid.strip()
        if(int(pid) > 0):
          register = self.get_register()
          if(register == None):
            self.logger.info('20 Irrigation Process Register file' + 
                              ' doesnt exist.')
          elif(register == False):
            self.logger.error('21 Couldnt retrieve register information' +
                              ' from flat file')
          else:
            if(pid in register):
              del register[pid]
              if(len(register) > 0):
                if(self.put_register(register)):
                  result = True
                else:
                  result = False # error condition logged in dura_file.write_data()
              else: # just deleted last pid from register, so delete the file
                result = self._delete_reg_file()
            else:
              self.logger.error(f'22 Register doesnt contain pid: {pid} ' +
                                'so it cant be deleted')
        else:
          self.logger.error('23 Invalid pid: pids must be positive integers.')
      except Exception as e:
        self.logger.error(f'24 Bad argument.  Exception: {e}')
    return(result)


  def add_pid_to_reg(self, irr_ev_info):
    """
    Obtain the Linux pid for the process running this Python code and
    combine it with the argument irr_ev_info in order to create a new 
    entry in the Irrigation Process Register.  

    Future: tighten up validity checks on sch seq and sch id
    
    Args:
      irr_ev_info(dict)   {'irr_ev_date':<'date'>, #day of irrigation
                           'irr_ev_seq':<int>,     #seq order for date
                           'irr_sch_id':<int>}     #id of irr schedule
    Returns:
      None    bad parameter
      True    successfully added entry to Irrigation Process Register
      False   failed to add new entry to Irrigation Process Register
    """
    self.logger.info('entering: add_pid_to_reg()')  

    result = None
    try:
      irr_ev_info['irr_ev_date'] = irr_ev_info['irr_ev_date'].strip()

      ev_date    = irr_ev_info['irr_ev_date']
      ev_sch_seq = irr_ev_info['irr_ev_seq']
      ev_sch_id  = irr_ev_info['irr_sch_id']

      if((ev_date and (type(ev_date) == str) and (len(ev_date) > 0)) and
         (ev_sch_seq and ((type(ev_sch_seq) == int ) and (ev_sch_seq > 0))) and
         (ev_sch_id and ((type(ev_sch_id) == int) and (ev_sch_id > 0)))):
        
        process_info = self.get_process_info()
        if(process_info):
          new_pid = process_info['pid'].strip()
          register = self.get_register()

          # register file exists so read it and add to it
          if(register):
            if(new_pid in register):
              self.logger.error('25 Irrigation Process Register already has' +
                                f' an entry with the pid: {new_pid}')
              result = False
            else:
              register[new_pid] = irr_ev_info
              if(self.put_register(register)):
                result = True
              else:
                result = False

          # no register file exists so create a new one
          elif(register == None):
            register = {}
            register[new_pid] = irr_ev_info
            if(self.put_register(register)):
              result = True
            else:
              result = False

          # could not retrieve register data from flat file
          else:  # (register == False)
            result = False
      else:
        self.logger.error('26 One or more arguments invalid.')
    except Exception as e:
      self.logger.error(f'27 Bad argument.  Exception: {e}')
    return(result)


  def get_process_info(self):
    """
    Get the pid of the process running this Python code, handling
    differences across platforms (e.g., Windows/Linux).  The only
    process attributes that exist on both Windows and Linux are:
    'pid' and 'parent_id'. 

    Returns:
      None                something went wrong
      process_info(dict)  Python dictionary with process information

    """
    self.logger.info('entering: get_process_info()')  

    process_info = None
    try:
      if(self.sys.platform == 'win32'):   # development; Windows
        process_info = {'pid'                : str(self.os.getpid()),
                        'parent_pid'         : str(self.os.getppid())}
      else:                              # production; Linux
        process_info = {'pid'                : str(self.os.getpid()),
                        'process_group_id'   : str(self.os.getpgrp()),
                        'session_id'         : str(self.os.getsid(0)),
                        'user_id'            : str(self.os.getuid()),
                        'effective_user_id'  : str(self.os.geteuid()),
                        'real_group_id'      : str(self.os.getgid()),
                        'effective_group_id' : str(self.os.getegid()),
                        'parent_pid'         : str(self.os.getppid())}
    except Exception as e:
      process_info = None
      self.logger.error('28 Could not retrieve process information. ' +
                        f'Exception: {e}')
    return(process_info)
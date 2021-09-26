"""
Jaye Hicks 2020

Obligatory legal disclaimer:
  You are free to use this source code (this file and all other files 
  referenced in this file) "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER 
  EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. 
  THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THIS SOURCE CODE
  IS WITH YOU.  SHOULD THE SOURCE CODE PROVE DEFECTIVE, YOU ASSUME THE
  COST OF ALL NECESSARY SERVICING, REPAIR OR CORRECTION. See the GNU 
  GENERAL PUBLIC LICENSE Version 3, 29 June 2007 for more details.

Automated irrgation control scripts running on the PI 4 platform
leverage a large number of flat files located across several file
system directories.  Objects of type lv_paths() are used to abstract
file system details away from the control scripts so that they can work
across linux, windows, development environments, production 
environments, etc. requiring minimual code modification (e.g., set
a class variable to 'debug' or 'prod').

Usage:
  >>> import lv_paths
  >>> paths = lv_path.lv_paths()
  >>> paths.get_path('irr_sched')
  /home/pi/lonesome/control/irr_sched
"""

class lv_paths():
  """
  lv_paths() objects are used to retrieve the direcotry path to 
  numerous standardized locations utilized throughout overall 
  irrigation management.
  """
  import logging
  import sys


  def __init__(self):
    """
    """
    self.logger = self.logging.getLogger(__name__)

    self.logger.info('entering: __init__()')
    self.prefix   = None
    self.platform = None
    self.divider  = None
    try:
      if(self.sys.platform == "win32"):   # development
        self.prefix   = 'C:\\Development\\LonesomeVine\\Irrigation\\lonesome'
        self.platform = 'win32'
        self.divider  = '\\'
      else:                               # production
        self.prefix   = '/home/pi/lonesome'
        self.divider  = '/' 
        self.platform = 'unix'
    except Exception as e:
      self.logger.error(f'1 Couldnt detect OS. Exception: {e}')

    else:
      self.win_dirs = {'root'               : '',
                       'control'            : '\\control',
                       'irr_sched'          : '\\control\\irr_sched',
                       'cur_irr_sched'      : '\\control\\irr_sched\\current',
                       'new_irr_sched'      : '\\control\\irr_sched\\new',
                       'process_reg'        : '\\control\\process_reg',
                       'time_synch'         : '\\control\\last_time_synch',
                       'irr_event'          : '\\control\\irr_event',
                       'irr_ev_in_progress' : '\\control\\irr_event\\in_progress',
                       'shadow_sec'         : '\\control\\shadow_sec',
                       'sys_logs'           : '\\sys_logs',
                       'alarms_errors'      : '\\sys_logs\\alarms_errors',
                       'comms'              : '\\comms',
                       'alarms'             : '\\comms\\alarms',
                       'bad_comms'          : '\\comms\\bad_comms',
                       'gals_disp'          : '\\comms\\gals_disp',
                       'orphans'            : '\\comms\\orphans',
                       'corrupt_files'      : '\\comms\\corrupt_files'}
      self.unix_dirs = {'root'              : '',
                       'control'            : '/control',
                       'irr_sched'          : '/control/irr_sched',
                       'cur_irr_sched'      : '/control/irr_sched/current',
                       'new_irr_sched'      : 'control/irr_sched/new',
                       'time_synch'         : '/control/last_time_synch',
                       'irr_event'          : '/control/irr_event',
                       'irr_ev_in_progress' : '/control/irr_event/in_progress',
                       'process_reg'        : '/control/process_reg',
                       'shadow_sec'         : '/control/shadow_sec',
                       'sys_logs'           : '/sys_logs',
                       'alarms_errors'      : '/sys_logs/alarms_errors',
                       'comms'              : '/comms',
                       'alarms'             : 'comms/alarms',
                       'bad_comms'          : '/comms/bad_comms',
                       'gals_disp'          : '/comms/gals_disp',
                       'orphans'            : '/comms/orphans',
                       'corrupt_files'      : '/comms/corrupt_files'}


  def get_path(self, target_dir):
    """
    Args:
      target_dir (str)    the directory that you want the path for

    Regurns:
      None                if bad input parameter
      path(str)           the platform specific path for the directory
    """
    self.logger.info('entering: get_path()')
    path = None
    if((target_dir) and (type(target_dir) == str)):
      try:
        target_dir = (target_dir.strip()).lower()
        if(self.platform == "win32"):
          path = self.prefix + self.win_dirs[target_dir]
        elif(self.platform == "unix"):
          path = self.prefix + self.unix_dirs[target_dir]
        else:
          self.logger.error('2 The platforms OS value not configured.')
      except Exception as e:
        self.logger.error(f'3 Bad argument. Exception: {e}')
    else:
      self.logger.error('4 Bad argument type or empty.')
    return(path)
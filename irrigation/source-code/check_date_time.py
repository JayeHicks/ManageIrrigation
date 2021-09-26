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

A Raspberry PI 4 in default configuratin does not have an onbaord 
Real Time Clock.  When it boots up the date time will be what it was
when it powered down.  The PI 4 will attempt, approximately every
5 mins, to reach out to an external source (i.e., Internet endpoint)
and obtain the current date / time.

The time_synch class will validate the platform's date time setting. 
A list of public API endpoints are accessed to obtain the current
date time.  If the current date time cannot be obtained from any of
the public APIs then a list of publicly accessable serverrs are 
accessed, via a socket connection, to obtain the current date time.

With the current date time value, provided by a trusted external 
source, compare it to the PI 4's value (i.e., what is running this
Python code).  If the two vary beyond the defined threhold, set the
the attribute 'check_date_time.acceptable' to False

Note, it is imperative that you configure the PI 4 to the correct
timezone.  I believe this information is passed into the requests
to time/date servers publicly accessible on the Internet.

Usage: # assuming the platforms date / time value is out of tolerance
  >>> import check_date_time
  >>> if(check_date_time.check_date_time().acceptable != True):
  >>>   print("platforms date / time values vary beyond accpetable tolerance")
  platforms date /time values vary behyond acceptabel tolernace
  >>> cdt = check_date_time.check_date_time()
  >>> cdt.acceptable
  False
  >>> cdt.check()
  False
"""


class check_date_time():
  """
  Check the platform's value for date / time against externally 
  sourced value
  """
  import logging
  import json
  import datetime
  import urllib.request
  
  
  class _source_from_server():
    """
    This class will be used only if the current date / time cannot be
    sourced from a public Internet API endpoint.

    Create socket connection to servers to get current epoch date time.
    
    The epoch date time returned from these servers is correct 
    for CST. As neither a time zone name nor a numeric offset sent in
    with the call, I suspect the server deduces a time zone based on 
    to the origination IP address of the incoming request.  Therefore,
    its super important to configure the PI 4 to the correct timezone.
    """
    import logging
    import socket
    import struct


    def __init__(self):
      """
      """
      self.logger = self.logging.getLogger('_source_from_server')

      self.logger.info('entering: __init__()')
      self.servers        = ['ntp.iitb.ac.in', 'time.nist.gov', 
                             'time.windows.com', 'pool.ntp.org']
      self.sourced_time   = None 
      self.TIME_1970      = 2208988800
      self.SOCKET_TIMEOUT = 5.0
      self.BUFFER_SIZE    = 1024
      
      for server in self.servers:
        if(self._access_server(server)):
          break


    def _access_server(self, server):
      """
      Connect to a single server and attempt to source date time.
      """
      self.logger.info('entering: _access_server()')

      result = False
      try:
        client = self.socket.socket(self.socket.AF_INET, 
                                    self.socket.SOCK_DGRAM )
      except Exception as e:
        self.logger.error(f'1 Could not create socket. Exception: {e}')
      else:
        data = '\x1b' + 47 * '\0'
        data = data.encode()
        try:
          client.settimeout(self.SOCKET_TIMEOUT)
          client.sendto(data, (server, 123))
          data, address = client.recvfrom(self.BUFFER_SIZE)
          if(data):
            self.sourced_time = self.struct.unpack( '!12I', data )[10]
            self.sourced_time -= self.TIME_1970
            result = True
        except Exception as e:
          self.logger.error(f'2 Socket comms error.  Exception: {e}')
      return(result)
      
    
  def __init__(self):
    """
    """
    self.logger = self.logging.getLogger(__name__)

    self.logger.info('entering: __init__()')
    self.api_providers = {
      'world_time' : 
        {'url' : 'http://worldtimeapi.org/api/timezone/America/Chicago', 
         'key' : 'datetime'}, 
      'world_clock' : 
        {'url' : 'http://worldclockapi.com/api/json/cst/now', 
         'key' : 'currentDateTime'}}
    self.ACCEPTABLE_VARIANCE = 3600        # 3600 seconds in 1 hour
    self.sourced_time        = None
    self.acceptable          = None
    self.check()
  

  def check(self):
    """
    Try to source current date time from a public API endpoint.  If
    it cannot be sourced from an API endpoint, try to source via a 
    socket connection to a publicly accessable server.

    If date time can be sourced from an external source, compare it
    to the platforms date time.  Set the attribute
    'check_date_time.acceptable' to True or False reflecting whether
    or not the PI 4's setting for date / time is within acceptable
    tolerance from externally sourced date / time.
    
    Returns:
      None      date / time could not be sourced externally
      False     platforms date / time exceeds acceptable tolerance
      True      platforms date / time within acceptable tolerance
    """
    self.logger.info('entering: check()')

    self.acceptable   = None
    self.sourced_time = None

    #Priority/preference is to source from an API endpoint
    for api_provider in self.api_providers:
      self._source_from_api(api_provider)
      if(self.sourced_time):
        break
    
    #If no API endpoint access succeeded, try socket connection to server
    if(not self.sourced_time):                                
      self.server_synch = self._source_from_server()
      self.sourced_time = self.server_synch.sourced_time

    if(self.sourced_time):
      platform_time = int(self.datetime.datetime.now().timestamp())
      if(abs((platform_time - self.sourced_time)) < self.ACCEPTABLE_VARIANCE):
        self.acceptable = True
      else:
        self.acceptable = False

    return(self.acceptable)


  def _generate_ts(self, date_time_string):
    """
    Generate a epoch time stamp (i.e., 10 digits representing number of
    seconds that have elapsed since Jan 1, 1970) from the human readable
    date time string passed in parameter

    Args:
      date_time_string (str)       example: '2021-04-15T14:49'
    Returns:
      None    if argument could not be converted to a epoch ts
      int     10 digit epoch time stamp representation of argument
    """
    self.logger.info('entering: _generate_ts()')

    epoch_ts = None
    if((date_time_string)  and (type(date_time_string) == str)):
      try:
        date_time_string = date_time_string[:16]
        date_time_string = date_time_string.replace('T', ' ')
        date_time_object = self.datetime.datetime.strptime(date_time_string, 
                                                           '%Y-%m-%d %H:%M')
        epoch_ts = int(date_time_object.timestamp())
      except Exception as e:
        self.logger.error('3 Issue parsing date time from string: ' +
                          f'{date_time_string} Exception: {e}')        
    return(epoch_ts)


  def _source_from_api(self, api_provider):
    """
    Source the current date time from the api provider
    """
    self.logger.info('entering: _source_from_api()')

    if(api_provider):
      if(api_provider in self.api_providers):
        try:
          url = self.api_providers[api_provider]['url']
          key = self.api_providers[api_provider]['key']
          with self.urllib.request.urlopen(url) as response:
            RAW_HTML_BYTES = response.read()
            RAW_HTML_STRING = RAW_HTML_BYTES.decode()
        except Exception as e:
          RAW_HTML_STRING = ''
          self.logger.error('4 Issue arose invoking endpoint: ' +
                            f'{api_provider} Exception: {e}')
        
        if(RAW_HTML_STRING):
          try:
            JSON_RESPONSE = self.json.loads(RAW_HTML_STRING)
            self.sourced_time =  self._generate_ts(JSON_RESPONSE[key])
          except Exception as e:
            self.logger.error('5 Cant extract datetime from response of ' +
                              f'endpoint: {api_provider}. Exception: {e}')
"""
Jaye Hicks 2021

Deployment check list: set upload_files.env to 'debug' or 'prod'

Obligatory legal disclaimer:
  You are free to use this source code (this file and all other files 
  referenced in this file) "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER 
  EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. 
  THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THIS SOURCE CODE
  IS WITH YOU.  SHOULD THE SOURCE CODE PROVE DEFECTIVE, YOU ASSUME THE
  COST OF ALL NECESSARY SERVICING, REPAIR OR CORRECTION. See the GNU 
  GENERAL PUBLIC LICENSE Version 3, 29 June 2007 for more details.

NOTE: the identifier 'boto3' is overidden in the upload_files()

The majority of data transmitted to the AWS backend routes through
the PI 4 file system.  Some are passed in through the an API Gwy API
with the others passing through an IoT Core shadow document (i.e., 
mqtt queue).  In the event that transmission fails for one of these 
files it is uploaded by this module to an S3 bucket for future forensic 
analysis.

This module will attempt to upload all suspect file and will not
stop should the upload of a single file fail.

Currently, there are three special purose S3 buckets set up to
receive thse suspect files.  
- files that, via dura_file() functionality, have been determined 
  to be corrupt
- files that resulted in a failed communication attempt (i.e., bad 
  return value or have aged beyond a threshold waiting, with retries
  to be sent)
- files that have been orphaned or simply misplaced somehow

Most flat files involved in automated irrigation control on the PI 4
platform have their creation date encoded in the file name.  This
enables the detection of troublesome files that resist processing
and / or upload beyond a certain amount of time (i.e., threshold for
processing / uploading).

NOTE: the identifier 'boto3' is overidden in the class upload_files()

"""
class upload_files():
  import logging
  import boto3
  import boto3.session
  from   botocore.exceptions import EndpointConnectionError
  from   botocore.exceptions import ClientError
  from   pathlib             import Path
  import lv_paths


  def __init__(self):
    """
    NOTE: identifier 'boto3' overidden to refer to a boto3 session
    """
    self.logger = self.logging.getLogger(__name__)

    self.logger.info('entering: __init__()')
    self.env     = 'debug'   #set to 'debug' or 'prod'
    self.paths   = self.lv_paths.lv_paths()

    if(self.env   == 'debug'):
      self.config = {'corrupt' : 'lv-irr-man-corrupt-files',
                     'comms'   : 'lv-irr-man-bad-comms',
                     'orphans' : 'lv-irr-man-orphans',
                     'region'  : 'us-east-1'}
      #overide identifier 'boto3' (session using IAM credentials below)
      try:
        self.boto3 = self.boto3.Session(
          aws_access_key_id     = '1234567890123456789012', 
          aws_secret_access_key = '12345678901234567890123456789012345678901',
          region_name           = self.config['region'])
      except Exception as e:
        self.logger.error(f'1 Couldnt create boto3 session.  Exception: {e}')
    else: 
      self.config = {'corrupt' : 'lv-irr-man-corrupt-files',
                     'comms'   : 'lv-irr-man-bad-comms',
                     'region'  : 'us-east-1'}
      #overide identifier 'boto3' (session using AWS CLI default profile)
      try:
        self.boto3 = self.boto3.session.Session()
      except Exception as e:
        self.logger.error(f'2 Couldnt create boto3 session. Exception: {e}')

    self._reset_status()

  def _reset_status(self):
    """
    Clear out communication details from last transmission of corrupted files
    """
    self.logger.info('entering: _reset_status()')

    self.comms_ok   = None
    self.trans_good = []     # successfuly uploaded these listed files
    self.trans_bad  = []     # failed to upload these listed files
    self.clean_good = []     # successfully deleted these listed files
    self.clean_bad  = []     # failed to delete these listed files


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


  def good_comms(self):
    self.logger.info('entering: good_comms()')    
    return(self.comms_ok)


  def _bucket_exists(self, bucket):
    """ 
    Does the destinated S3 bucket exist?

    Args:
      bucket(str)            name of special purpose S3 bucket    
    Returns:
      None                   could not access S3 endpoint, could be
                               down netowrk or down ISP
      False                  bucket does not exist
      True                   bucket owned by this acct or is public
    """
    self.logger.info('entering: _bucket_exists()')

    result = None
    try:
      s3_access = self.boto3.client('s3')
      s3_access.head_bucket(Bucket=bucket)
      result = True 
    except self.ClientError as e:
      error_code = int(e.response['Error']['Code'])
      if(error_code == 404):
        result = False  # does not exist
      else:
        result = False  # permission issue
    except self.EndpointConnectionError as e:
      result = None
      self.logger.error(f'3 Couldnt access S3 endpoint.  Exception: {e}')
    except Exception as e:
      result = None
      self.logger.error(f'4 Boto3 exception: {e}')
    return(result)

 
  def _upload_file_to_s3(self, bucket, path):
    """
    Upload a single file to special purpose S3 bucket.
    Caution: Assumes input parameters are valid and any preexisting
             objects with same name will be overwritten

    Args:
      bucket (str)        name of the S3 bucket to upload to
      path (pathlib)      object refers to a single file to upload

    Returns:
      None                Issue before upload attempt
      False               failed to open file or upload file
      True                upload attempt succeeded
    """
    self.logger.info('entering: _upload_file_to_s3()')

    result = None
    bucket = bucket.strip()
    name   = path.name
    
    try:
      with path.open('rb') as file_contents:
        s3_access  = self.boto3.resource('s3')
        the_bucket = s3_access.Bucket(bucket)
        the_object = the_bucket.put_object(Key=name, Body=file_contents)
        the_object.wait_until_exists()   
        result = True
    except self.EndpointConnectionError as e:
      self.logger.error(f'5 Could not access S3 endpoint. Exception: {e}')
      result = None
    except Exception as e:
      result = False
      self.logger.error(f'6 Open file / S3 upload file error.  Exception: {e}')

    return(result)


  def _upload_and_clean_up(self, bucket, path):
    """
    Files that were successfully uploaded to S3 will be deleted as
    they are no longer needed.  For upload attempts that failed, the
    file will be left in place and subsequent attempts to upload these
    files will be made by future invocation of this module.  A separate
    Python script will detect files that remain (i.e., cant be
    uploaded) beyond a threshold limit and manages them.
      
    Args:
      bucket(str)     name of the bucket to upload file to
      path(pathlib)   directory containing files to uplaod to S3
    """
    self.logger.info('entering: _upload_and_clean_up()')

    if(path and bucket and (type(bucket) == str)):
      bucket.strip()
      try:
        if(path.exists() and path.is_dir()):
          for file in path.iterdir():
            file_name = file.name
            result = self._upload_file_to_s3(bucket, file)
            if(result):
              self.trans_good.append(file_name)
              try:
                file.unlink()
                self.clean_good.append(file_name)
              except Exception as e:
                self.clean_bad.append(file_name)
                self.logger.error('7 Couldnt delete file: {file_name}. ' +
                                  f'Exception: {e}')
           
            #upload attempt made and failed; file is suspect
            elif(result == False):
              self.trans_bad.append(file_name)
        else:
          self.logger.error('8 Bad pathlib argument sent to ' +
                            ' _upload_and_clean_up()')
      except Exception as e:
        self.logger.error(f'9 Exception: {e}')
    else:
      self.logger.error('10 Bad argument(s) passed to _upload_and_clean_up()')


  def send(self):
    """
    Attempt the transfer of all files requiring transfer to special
    purpose S3 buckets for future forensic analysis.  At present two
    categories of files are uploaded to special S3 buckets

    Returns:
      None            something went wrong before transfer attempt
                        or nothing to transfer
      False           one or more erros occured during transfer attempt
      True            all transfer attemps succeed
    """
    self.logger.info('entering: send()')

    self._reset_status()
    corrupt_path  = self.paths.get_path('corrupt_files')
    bad_path      = self.paths.get_path('bad_comms')
    orphans_path  = self.paths.get_path('orphans')

    if(corrupt_path and bad_path):
      try:
        corr_dir       = self.Path(corrupt_path)
        bad_dir        = self.Path(bad_path)
        orphans_dir    = self.Path(orphans_path)
        corr_bucket    = self.config['corrupt']
        comms_bucket   = self.config['comms']
        orphans_bucket = self.config['orphans']

        if(corr_dir.exists() and corr_dir.is_dir() and
           bad_dir.exists() and bad_dir.is_dir()):

          corr_bucket_exists    = self._bucket_exists(corr_bucket)
          comms_bucket_exists   = self._bucket_exists(comms_bucket)
          orphans_bucket_exists = self._bucket_exists(orphans_bucket)
          if(corr_bucket_exists and 
             comms_bucket_exists and 
             orphans_bucket_exists):
            self._upload_and_clean_up(corr_bucket, corr_dir)
            self._upload_and_clean_up(comms_bucket, bad_dir)
            self._upload_and_clean_up(orphans_bucket, orphans_dir)
          elif((corr_bucket_exists == None) or 
               (comms_bucket_exists == None) or
               (orphans_bucket_exists == None)):
            self.logger.error('11 Couldnt access S3 endpoint.  Network /' +
                              ' ISP may be down.')
          else:
            self.logger.error('12 S3 bucket(s) that receives file uploads' +
                              ' does not exist or a specified bucket name ' +
                              'belongs to another AWS account')
        else:
          self.logger.error('13 Cant configure pathlib objects.')
      except Exception as e:
        self.logger.error(f'14 Exception: {e}')
    else:
      self.logger.error('15 Couldnt retrieve corrupt files directory path ' +
                        'and or bad comms directory path from lv_paths object')

    if(self.trans_bad or self.clean_bad):
      self.comms_ok = False
    elif(self.trans_good):     # no errors and at least 1 good transmission
      self.comms_ok = True

    return(self.comms_ok)
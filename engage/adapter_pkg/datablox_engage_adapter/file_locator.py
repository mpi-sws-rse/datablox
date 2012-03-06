import os
import os.path
import sys

def np(p):
  """normalize a path"""
  return os.path.abspath(os.path.expanduser(p))

def check_dir(dirpath):
  if not os.path.isdir(dirpath):
    raise Exception("Could not find directory '%s' - is your Engage environment set up correctly?" % dirpath)

def check_file(filepath):
  if not os.path.exists(filepath):
    raise Exception("Could not find file '%s' - is your Engage environment set up correctly?" % filepath)

class FileLocator(object):
  """This class has methods to return the locations of various files and
  directories used by Datablox and Engage, assuming that Datablox was deployed
  by Engage.
  """
  def __init__(self):
    # First get the deployment home by searching up the directory tree.
    # We need to resolve any symlinks first due to the new virtualenv structure
    # on Ubuntu Linux 11.
    self.dh = np(os.path.os.path.join(os.path.realpath(os.path.dirname(__file__)),
                                      "../../../../../.."))
    self.config_dir = os.path.join(self.dh, "config")
    check_dir(self.config_dir)
    self.engage_dir = os.path.join(self.dh, "engage")
    check_dir(self.engage_dir)
    self.blox_dir = os.path.join(self.dh, "blox")
    check_dir(self.engage_dir)
    self.installed_res_file = os.path.join(self.config_dir,
                                           "installed_resources.json")
    self.svcctl_exe = os.path.join(self.engage_dir,
                                   "bin/svcctl")
    check_file(self.svcctl_exe)
    self.deployer_exe = os.path.join(self.engage_dir,
                                     "bin/deployer")
    check_file(self.deployer_exe)
    self.master_pw_file = os.path.join(self.config_dir, "master.pw")
    check_file(self.master_pw_file)
    log_dir_ref_file = os.path.join(self.config_dir, "log_directory.txt")
    check_file(log_dir_ref_file)
    with open(log_dir_ref_file, "r") as f:
      self.log_directory = f.read().rstrip()
    assert self.log_directory, "%s does not seem to contain a valid directory" %\
           log_dir_ref_file

  def get_dh(self):
    return self.dh
    
  def get_blox_dir(self):
    return self.blox_dir

  def get_config_dir(self):
    return self.config_dir
  
  def get_installed_resources_file(self):
    """Return the path to the installed resources file.
    """
    return self.installed_res_file

  def is_installed_resources_file_present(self):
    return os.path.exists(self.installed_res_file)

  def move_installed_resources_file(self, backup_extn=".prev"):
    """Move the installed resources file to a backup file so that we
    can write a new one.
    """
    check_file(self.installed_res_file)
    backup_name = self.installed_res_file + backup_extn
    os.rename(self.installed_res_file, backup_name)

  def get_svcctl_exe(self):
    return self.svcctl_exe

  def get_deployer_exe(self):
    return self.deployer_exe

  def get_master_pw_file(self):
    return self.master_pw_file

  def get_file_server_key_file(self):
    return os.path.join(self.dh, "datablox_file_server_key")

  def get_djm_server_dir(self):
    return os.path.join(self.dh, "djm")

  def get_log_directory(self):
    return self.log_directory

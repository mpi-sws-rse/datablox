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
    self.dh = np(os.path.os.path.join(os.path.dirname(__file__),
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

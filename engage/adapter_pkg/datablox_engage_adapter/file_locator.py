import os.path
import sys

def np(p):
  """normalize a path"""
  return os.path.abspath(os.path.expanduser(p))

def check_dir(dirpath):
  if not os.path.isdir(dirpath):
    raise Exception("Could not find directory '%s' - is your engage environment set up correctly?" % dirpath)

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

  def get_dh(self):
    return self.dh
    
  def get_blox_dir(self):
    return self.blox_dir




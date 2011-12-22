#
# -*- py-indent-offset:2 -*- 

import os.path
import subprocess

def run_deployer(file_locator, install_spec_file):
  cmd_line = [file_locator.get_deployer_exe(), install_spec_file]
  print "Running deployer: %s" % cmd_line
  subprocess.check_call(cmd_line)

def run_svcctl(file_locator, command_and_args):
  cmd_line = [file_locator.get_svcctl_exe()] + command_and_args
  print "Running svc controller: %s" % cmd_line
  subprocess.check_call(cmd_line)


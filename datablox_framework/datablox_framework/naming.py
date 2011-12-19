"""Element naming and location utilities for the datablox framework.
"""

import os.path

DEFAULT_VERSION = "1.0"

def mangle_string(name):
  """Take a string and make it a valid python module name. This is
  used for element names and versions.
  """
  return name.lower().replace('-','_').replace('.', '_').replace(' ', '')
    
def element_path(bloxpath, element_name, version=DEFAULT_VERSION):
  base_name = mangle_string(element_name)
  file_name = 'e_' + base_name + '.py'
  dir_name = base_name + "__" + mangle_string(version)
  return os.path.join(os.path.join(bloxpath, dir_name),
                      file_name)
  
def element_class_name(element_name):
    return mangle_string(element_name)
  
def element_module(element_name, version=DEFAULT_VERSION):
  base_name = mangle_string(element_name)
  return base_name + "__" + mangle_string(version) + ".e_" + \
         base_name


def element_submodule(element_name):
  return "e_" + mangle_string(element_name)


def get_block_class(element_name, version):
  module_name = element_module(element_name, version)
  module = __import__(module_name)
  submodule = getattr(module, element_submodule(element_name))
  return getattr(submodule, element_class_name(element_name))

def get_block_resource_key(element_name, version):
  return {u"name":unicode(element_name.lower()), u"version":unicode(version)}

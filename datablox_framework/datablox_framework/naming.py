"""Block naming and location utilities for the datablox framework.
"""

import os.path

DEFAULT_VERSION = "1.0"

def mangle_string(name):
  """Take a string and make it a valid python module name. This is
  used for block names and versions.
  """
  return name.lower().replace('-','_').replace('.', '_').replace(' ', '')
    
def block_path(bloxpath, block_name, version=DEFAULT_VERSION):
  base_name = mangle_string(block_name)
  file_name = 'b_' + base_name + '.py'
  dir_name = base_name + "__" + mangle_string(version)
  return os.path.join(os.path.join(bloxpath, dir_name),
                      file_name)
  
def block_class_name(block_name):
    return mangle_string(block_name)
  
def block_module(block_name, version=DEFAULT_VERSION):
  if block_name.find("_")!=(-1) or block_name.find(".")!=(-1):
    raise Exception("Block name '%s' invalid - block names cannot contain underscores or periods" % block_name)
  base_name = mangle_string(block_name)
  return base_name + "__" + mangle_string(version) + ".b_" + \
         base_name

def block_submodule(block_name):
  return "b_" + mangle_string(block_name)

def get_block_class(block_name, version):
  module_name = block_module(block_name, version)
  module = __import__(module_name)
  submodule = getattr(module, block_submodule(block_name))
  return getattr(submodule, block_class_name(block_name))

def get_block_resource_key(block_name, version):
  return {u"name":unicode(block_name.lower()), u"version":unicode(version)}

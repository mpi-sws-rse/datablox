"""Utilties for processing block configuration properties
"""

# -*- py-indent-offset:2 -*-

import os.path
import json
from collections import namedtuple
import re
import logging


class BlockPropertyError(Exception):
  pass


PropDef = namedtuple("PropDef",
                     "name default validator processor help has_none_as_default")

def required_prop(name, validator=None, transformer=None, help="None"):
  """Define a single required configuration property for a block.

  If specified, the validator may either be an instance of type (e.g. int) or a
  function. If a function, it should accept the property name, value, and the
  block instance as properties and throw a BlockPropertyError if the
  value fails validation.

  If specified, the transformer argument should be an argument that takes the
  name, value, and block instance and returns a transformed value. For example the
  transformer might ensure that a file path is absolute.
  """
  default = None
  has_none_as_default = False
  return PropDef(name, default, validator, transformer, help,
                 has_none_as_default)

def optional_prop(name, validator=None, transformer=None,
                  help="None", default=None):
  """Define a single optional configuration property for a block.

  If default is not specified or equal to None, we provide None as
  the default value. If so, we skip the running of the validator in the event
  that the property is not provided in the configuration directory or if a
  value of None is provided.

  If specified, the validator may either be an instance of type (e.g. int) or a
  function. If a function, it should accept the property name, value, and the
  block instance as properties and throw a BlockPropertyError if the
  value fails validation.

  If specified, the transformer argument should be an argument that takes the
  name, value, and block instance and returns a transformed value. For example the
  transformer might ensure that a file path is absolute.
  """
  has_none_as_default = (default==None)
  return PropDef(name, default, validator, transformer, help, has_none_as_default)


def process_config(prop_defs, config, block_inst):
  """Given a list of property definitions, a configuration map,
  and a block instance, process all the configuration properties and
  add them as fields to the block instance.
  """
  for (name, default, validator, transformer, help, has_none_as_default) in prop_defs:
    if config.has_key(name):
      value = config[name]
    elif default!=None or (default==None and has_none_as_default):
      value = default
    else:
      raise BlockPropertyError("%s: Required property %s not specified" %
                               (block_inst.id, name))
    if validator and not (value==None and has_none_as_default):
      if isinstance(validator, type):
        # check that value is of type. includes special case to treat
        # str and unicode the same
        if not (isinstance(value, validator) or
                (validator==str and isinstance(value, unicode)) or
                (validator==unicode and isinstance(value, str))):
          raise BlockPropertyError("%s: Property %s has invalid value '%s'" %
                                   (block_inst.id, name, value))
      else:
        validator(name, value, block_inst) # custom validation function
    if transformer:
      value = transformer(name, value, block_inst)
    setattr(block_inst, name, value)
    block_inst.logger.info("Property '%s' value is %s" % (name, value.__repr__()))


# Validators
def v_dir_exists(name, path, obj_inst):
  path = os.path.abspath(os.path.expanduser(path))
  if not os.path.isdir(path):
    raise BlockPropertyError("%s: Property %s refers to non-existant directory '%s'" %
                             (obj_inst.id, name, path))


# match YYYY-MM-DDTHH:MM:SS*
timestamp_re = re.compile(r"^(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)")

def v_iso_timestamp(name, value, obj_inst):
  if (not isinstance(value, str) and not isinstance(value, unicode)) or \
     timestamp_re.match(value)==None:
    raise BlockPropertyError("%s: Property %s value %s is not in valid iso time/date format" %
                             (obj_inst.id, value.__repr__()))


# Validator constructors
def vc_or_types(*args):
  def v_or_types(name, value, obj_inst):
    for typ in args:
      if isinstance(value, typ): return
    raise BlockPropertyError("%s: Property %s should be one of the types %s, actual value was %s" %
                             (obj_inst.id, name, ', '.join([arg.__name__ for arg in args]),
                              value.__repr__()))
  v_or_types.__doc__ = "Valid types are any one of %s" % \
                       ', '.join([arg.__name__ for arg in args])
  return v_or_types


  # Transformers
def t_fixpath(name, path, obj_inst):
  return os.path.abspath(os.path.expanduser(path))

    

def t_jsondump(name, value, obj_inst):
  return json.dumps(value)

#
# Some common properties that blocks might want to use
#

def _extra_debug_xformer(name, value, obj_inst):
  if value==True:
    if self.log_level>logging.DEBUG:
      self.log_level==logging.DEBUG.
      self.logger.setLevel(logging.DEBUG)
  return value

extra_debug_property = \
    optional_prop('debug', default=False, validator=bool,
                  transformer=_extra_debug_xformer,
                  help="If True or if log level is ALL, then print extra debugging info (at some performance penalty). Will also ensure that log level for this block is at least DEBUG.")

def extra_debug_enabled(block):
  if block.debug or block.log_level<logging.DEBUG:
    return True
  else:
    return False


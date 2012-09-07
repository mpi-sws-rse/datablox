#
# -*- py-indent-offset:2 -*- 
from logging import ERROR, WARN, INFO, DEBUG
import copy
import re


from block import *
from block_cfg_utils import *

query_var_re = re.compile('^\\$\\{(.+)\\}$')

PROPERTIES = [
  required_prop('database', validator=str,
                help='The name of the mongo db database'),
  required_prop('input_collection', validator=str,
                help='The name of the collection on which the map-reduce will be performed'),
  required_prop('output_collection', validator=vc_or_types(unicode, dict),
                help='The name of the collection to store the results of the map-reduce or a dictionary of the form {"reduce": "output_collection_name"}'),
  required_prop('map_function', validator=str,
                help='A string containing the JavaScript map function'),
  required_prop('reduce_function', validator=str,
                help='A string containing the JavaScript reduce function'),
  optional_prop('run_on_each_key', validator=bool, default=False,
                help='If specified and True, run a map reduce on each incoming' +
                     ' key. Otherwise run a single map reduce at the end.'),
  optional_prop('scope', validator=dict, default=None,
                help="Key/value pairs to be used as the 'scope' for the map" +
                     " and reduce functions (similar to SQL bind variables)."),
  optional_prop('query', validator=dict, default=None,
                help="If provided, this should be a json representation of " +
                      "a query to use on the initial map operation. Any string " +
                      "values in the query are checked to see if they have the " +
                      "form ${var}, where var is a key in the scope. If so, the " +
                      "string is replaced with the associated scope value. This is " +
                      "useful to filter the map by the key provided on the input " +
                      "port."),
  optional_prop('pre_delete_matching_records_in_output', validator=dict,
                default=None,
                help="If specified, delete the matching records in the output " +
                     "collection before running the map-reduce. The same scope " +
                     "substitutions are performed on the deletion query as on " +
                     "the query property. This property is " +
                     "useful when you are rerunning a map-reduce on updated data."),
  extra_debug_property
]

class mongo_map_reduce(Block):
  """This block runs map-reduce on a mongo db collection.

  Ports
  -----
   * input - expects 'key' messages and completion tokens
   * output - emits 'key' messages and completion tokens

  Operation
  ----------
  This block a single input port 'input' and a single output port 'output'.
  As portions of the input dataset are completed, a message should be sent to this
  port with a 'key' property specified giving a value identifying the completed
  part of the dataset. If run_on_each_key is True, then, when each key message
  is received on the input port, a map-reduce is run, adding the key and its
  value to the scope mapping. The same key is then emitted on the output port.
  If run_on_each_key is False, nothing is done until the token is received on the
  input port. When the input port receives the completion token, a single map-reduce
  is run. Either way, a completion token is sent on the output port after all
  map-reduce operations have been run.
  """

  def on_load(self, config):
    import pymongo
    import pymongo.database
    import pymongo.collection
    from pymongo import Connection
    from bson.code import Code
    self.Code = Code
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["key"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["key"])
    self.connection = Connection()
    # get the configuration
    self.config = config
    process_config(PROPERTIES, config, self)

    # now we get the actual collection on which we wil be performing the map reduce
    database = pymongo.database.Database(self.connection, self.database)
    self.input_collection_obj = pymongo.collection.Collection(database,
                                                              self.input_collection)
    if self.pre_delete_matching_records_in_output!=None:
      if isinstance(self.output_collection, dict):
        if not self.output_collection.has_key('reduce'):
          raise BlockPropertyError("%s: output_collection dict missing 'reduce' key" %
                                   self.id)
        self.oc_name = self.output_collection['reduce']
      else:
        self.oc_name = self.output_collection
      self.output_collection_obj = pymongo.collection.Collection(database,
                                                                 self.oc_name)
    self.log(INFO, "Mongo-Map-Reduce: block loaded")

  def _create_query(self, query, scope):
    """Given the specified query, return a version that
    substitutes the scope variables.
    """
    def subst(map):
      r = {}
      for (k, v) in map.items():
        if isinstance(v, str) or isinstance(v, unicode):
          mo = query_var_re.match(v)
          if mo:
            var = mo.group(1)
            if scope.has_key(var):
              r[k] = scope[var]
            else:
              self.log(WARN,
                       "Query key %s references variable %s, which was not found in scope" % (k, var))
              r[k] = v
          else: # value is not a variable
            r[k] = v
        elif isinstance(v, dict):
          r[k] = subst(v)
        else:
          r[k] = v
      return r
    if (not scope) or (not query):
      # if no substition or there isn't a query, no need for further processing
      return query
    else:
      q = subst(copy.deepcopy(query))
      self.log(DEBUG, "using query filter %s" % q.__repr__())
      return q
    
  def process_key(self, key):
    self.log(INFO, "Processing key %s" % key)
    if self.scope:
      scope = copy.deepcopy(self.scope)
    else:
      scope = {}
    scope["key"] = key
    mf = self.Code(self.map_function, scope=scope)
    rf = self.Code(self.reduce_function, scope=scope)
    if self.pre_delete_matching_records_in_output!=None:
      if extra_debug_enabled(self):
        self.log(DEBUG, "%d rows in output collection before running pre-delete step" %
                 self.output_collection_obj.count())
      remove_query = self._create_query(self.pre_delete_matching_records_in_output,
                                        scope)
      self.output_collection_obj.remove(remove_query)
      self.log(INFO,
               "Removed rows matching %s from %s before executing map-reduce" %
               (remove_query.__repr__(), self.oc_name))
      if extra_debug_enabled(self):
        self.log(DEBUG, "%d rows in output collection after running pre-delete step" %
                 self.output_collection_obj.count())
    oc = self.input_collection_obj.map_reduce(mf,
                                              rf,
                                              self.output_collection,
                                              query=self._create_query(self.query,
                                                                       scope))
    cnt = oc.count()
    self.log(INFO, "Successfully ran map reduce, output collection size was %d" % cnt)

  def process_all(self):
    self.log(INFO, "Processing map-reduce")
    if self.scope:
      scope = self.scope
    else:
      scope = None
    mf = self.Code(self.map_function, scope=scope)
    rf = self.Code(self.reduce_function, scope=scope)
    if self.pre_delete_matching_records_in_output!=None:
      if extra_debug_enabled(self):
        self.log(DEBUG, "%d rows in output collection %s before running pre-delete step" %
                 (self.output_collection_obj.count(), self.oc_name))
      remove_query = self._create_query(self.pre_delete_matching_records_in_output,
                                        scope)
      self.output_collection_obj.remove(remove_query)
      self.log(INFO,
               "Removed rows matching %s from %s before executing map-reduce" %
               (remove_query.__repr__(), self.oc_name))
      if extra_debug_enabled(self):
        self.log(DEBUG, "%d rows in output collection after running pre-delete step" %
                 self.output_collection_obj.count())
    oc = self.input_collection_obj.map_reduce(mf,
                                              rf,
                                              self.output_collection,
                                              query=self._create_query(self.query,
                                                                       scope))
    cnt = oc.count()
    self.log(INFO, "Successfully ran map reduce, output collection size was %d" % cnt)
    
    
  def send_finished_token(self):
    log = Log()
    log.set_log({"token":[self.block_name]})
    self.push("output", log)
    
  def recv_push(self, port, log):
    if log.log.has_key("token"):
      self.log(INFO, "Got completion token %s" % log.log["token"][0])
      if not self.run_on_each_key:
        self.process_all()
      self.send_finished_token()
    else:
      assert log.log.has_key("key")
      if self.run_on_each_key:
        self.process_key(log.log["key"])
        self.push("output", log) # forward the key to the output

  def on_shutdown(self):
    self.connection.disconnect()
      
        

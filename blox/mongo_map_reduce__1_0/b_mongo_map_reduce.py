#
# -*- py-indent-offset:2 -*- 
from logging import ERROR, WARN, INFO, DEBUG
import copy
import re


from block import *

query_var_re = re.compile('^\\$\\{(.+)\\}$')

class mongo_map_reduce(Block):
  """This block runs map-reduce on a mongo db collection.

  Configuration Parameters
  ------------------------
   * database - the name of the mongo db database
   * input_collection  - the name of the collection on which the map-reduce will be
                         performed
   * output_collection - the name of the collection to store the results of the
                         map-reduce
   * map_function      - a string containing the JavaScript map function
   * reduce_function   - a sring containing the JavaScript reduce function
   * run_on_each_key   - if specified and True, run a map reduce on each incoming
                         key. Otherwise run a single map reduce at the end.
   * scope             - Key/value pairs to be used as the 'scope' for the map
                         and reduce functions (similar to SQL bind variables).
   * query             - If provided, this should be a json representation of
                         a query to use on the initial map operation. Any string
                         values in the query are checked to see if they have the
                         form ${var}, where var is a key in the scope. If so, the
                         string is replaced with the associated scope value. This is
                         useful to filter the map by the key provided on the input
                         port.

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
  def _check_config(self, property, config):
    if not config.has_key(property):
      raise Exception("%s: configuration missing required property '%s'" %
                      (self.id, property))
    
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
    self._check_config("database", config)
    self.database_name = config["database"]
    self._check_config("input_collection", config)
    self.input_collection_name = config["input_collection"]
    self._check_config("output_collection", config)
    self.output_collection_name = config["output_collection"]
    if config.has_key("run_on_each_key") and config["run_on_each_key"]:
      self.run_on_each_key = True
    else:
      self.run_on_each_key = False
    self._check_config("map_function", config)
    self.map_function = config["map_function"]
    self._check_config("reduce_function", config)
    self.reduce_function = config["reduce_function"]
    if config.has_key("scope"):
      self.scope = config["scope"]
    else:
      self.scope = None
    if config.has_key("query"):
      self.query = config["query"]
      assert isinstance(self.query, dict)
    else:
      self.query = None
    # now we get the actual collection on which we wil be performing the map reduce
    database = pymongo.database.Database(self.connection, self.database_name)
    self.input_collection = pymongo.collection.Collection(database,
                                                          self.input_collection_name)
    self.log(INFO, "Mongo-Map-Reduce: block loaded")

  def _create_query(self, scope):
    """Given the query specified in the configuration, return a version that
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
    if (not scope) or (not self.query):
      # if no substition or there isn't a query, no need for further processing
      return self.query
    else:
      q = subst(self.query)
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
    oc = self.input_collection.map_reduce(mf,
                                          rf,
                                          self.output_collection_name,
                                          query=self._create_query(scope))
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
    oc = self.input_collection.map_reduce(mf,
                                          rf,
                                          self.output_collection_name,
                                          query=self._create_query(scope))
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
      
        

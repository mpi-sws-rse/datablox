#
# -*- py-indent-offset:2 -*- 

"""Install a block and its dependencies
"""
import os
import os.path
import json
import copy

import file_locator
import utils

def _add_resource_to_spec(block_id, key, installed):
  updated = copy.deepcopy(installed)
  # find the master host resource
  master_inst = None
  for inst in updated:
    if inst["id"]=="master-host":
      master_inst = inst
      break
  if not master_inst:
    raise Exception("Could not find master-host resource in installed resources file")
  updated.append({
    "id": block_id,
    "key": key,
    "inside": {
      "id": "master-host",
      "key": master_inst["key"],
      "port_mapping": {"host":"host"}
    }
  })
  return updated

def _res_key_to_spec_file(key):
    return key["name"].lower().replace(" ", "_") + "_spec.json"

def install_block(resource_key):
  fl = file_locator.FileLocator()
  # first we need to create a new install spec
  if not fl.is_installed_resources_file_present():
      raise Exception("Something seems to be wrong with you Engage setup - expecting installed resources file at '%s' but did not find it." % fl.get_installed_resources_file())
  with open(fl.get_installed_resources_file(), "rb") as f:
      installed = json.load(f)
  block_id = resource_key["name"].replace(" ", "_")
  already_installed = False
  for res in installed:
      key = res["key"]
      if key["name"]==resource_key["name"] and \
         key["version"]==resource_key["version"]:
        already_installed = True
        break
  if already_installed:
    print "Block %s already installed" % resource_key.__repr__()
    utils.run_svcctl(fl, ["start", block_id])
  else:
    print "Installing block %s" % resource_key.__repr__()
    fl.move_installed_resources_file()
    updated = _add_resource_to_spec(block_id, resource_key, installed)
    install_spec_file = os.path.join(fl.get_config_dir(),
                                     _res_key_to_spec_file(resource_key))
    with open(install_spec_file, "wb") as f:
      json.dump(updated, f, indent=2)
    utils.run_deployer(fl, install_spec_file)
        
    

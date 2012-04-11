"""This is a driver module for an arbitrary block. It should be copied
into the associated driver directory (datablox/drivers/<block_name>__1_0).
This is done automatically by the create_block.sh script.
"""
import engage.drivers.utils
import engage.drivers.datablox.generic_blox_driver

# this is used by the package manager to locate the packages.json
# file associated with the driver
def get_packages_filename():
    return engage.drivers.utils.get_packages_filename(__file__)

Manager = engage.drivers.datablox.generic_blox_driver.Manager


"""
Unit test script for mongodb 2.0 driver.
This script is designed to be run from engage.tests.test_drivers.
"""


# Id for the resource to be tested.
# An instance with this id must be present
# in the install script.
resource_id = "mongodb"

# The install script should be a json string
# containing a list which includes the
# resource instance for the driver being tested.
# It can use the following substitution variables:
#   deployment_home, hostname, username
_install_script = """
[
  { "id": "mongodb",
    "key": {"name": "mongodb", "version": "2.0"},
    "config_port": {
      "home": "${deployment_home}/mongodb-2.0",
      "log_file": "${deployment_home}/log/mongodb.log",
      "port": 27017
    },
    "input_ports": {
      "host": {
        "cpu_arch": "x86_64",
        "genforma_home": "${deployment_home}",
        "hostname": "${hostname}",
        "log_directory": "${deployment_home}/log",
        "os_type": "linux",
        "os_user_name": "${username}",
        "private_ip": null,
        "sudo_password": "GenForma/${username}/sudo_password"
      }
    },
    "output_ports": {
      "mongodb": {
        "home": "${deployment_home}/mongodb-2.0",
        "hostname": "localhost",
        "port": 27017
      }
    },
    "inside": {
      "id": "master-host",
      "key": {"name": "ubuntu-linux", "version": "10.04"},
      "port_mapping": {
        "host": "host"
      }
    }
  }
]
"""

def get_install_script():
    return _install_script

# If the driver needs access to the password database, either for the sudo
# password or for passwords it maintains in the database, define this function.
# It should return a dict containing an required password entries, except for the
# sudo password which is added by the test driver. If you don't need the password
# database just comment out this function or have it return None.
def get_password_data():
    return {}


"""
Unit test script for apache-solr-jetty-server 3.5 driver.
This script is designed to be run from engage.tests.test_drivers.
"""


# Id for the resource to be tested.
# An instance with this id must be present
# in the install script.
resource_id = "apache_solr_jetty_server"

# The install script should be a json string
# containing a list which includes the
# resource instance for the driver being tested.
# It can use the following substitution variables:
#   deployment_home, hostname, username
_install_script = """
[
  { "id": "__apache_solr_jetty_server__4_1__1",
    "key": {"name": "apache-solr-jetty-server", "version": "4.1"},
    "input_ports": {
      "host": {
        "cpu_arch": "x86_64",
        "genforma_home": "${deployment_home}",
        "hostname": "${hostname}",
        "log_directory": "${deployment_home}/log",
        "os_type": "mac-osx",
        "os_user_name": "${username}",
        "private_ip": null,
        "sudo_password": "GenForma/${username}/sudo_password"
      },
      "jvm": {
        "home": "/System/Library/Frameworks/JavaVM.framework/Versions/CurrentJDK/Home",
        "java_exe": "/usr/bin/java",
        "type": "jdk"
      },
      "schema_file": {
        "file_path": "${deployment_home}/solr_schema.xml"
      }
    },
    "output_ports": {
      "solr": {
        "home": "${deployment_home}/solr"
      }
    },
    "inside": {
      "id": "master-host",
      "key": {"name": "mac-osx", "version": "10.6"},
      "port_mapping": {
        "host": "host"
      }
    },
    "environment": [
      {
        "id": "__solr_block_schema_file__1_0__9",
        "key": {"name": "solr-block-schema-file", "version": "1.0"},
        "port_mapping": {
          "schema_file": "file_info"
        }
      },
      {
        "id": "__java_virtual_machine_abstract__1_6__8",
        "key": {"name": "java-virtual-machine-abstract", "version": "1.6"},
        "port_mapping": {
          "jvm": "jvm"
        }
      }
    ]
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

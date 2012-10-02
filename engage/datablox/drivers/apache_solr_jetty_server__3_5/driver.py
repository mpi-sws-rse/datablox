
"""Resource manager for apache-solr-jetty-server 3.5 
"""

# Common stdlib imports
import sys
import os
import os.path
import urllib
from contextlib import closing

# fix path if necessary (if running from source or running as test)
try:
    import engage.utils
except:
    sys.exc_clear()
    dir_to_add_to_python_path = os.path.abspath((os.path.join(os.path.dirname(__file__), "../../../..")))
    sys.path.append(dir_to_add_to_python_path)

import engage.drivers.service_manager as service_manager
import engage.drivers.utils

# Drivers compose *actions* to implement their methods.
from engage.drivers.action import *

# setup errors
from engage.utils.user_error import UserError, EngageErrInf
import gettext
_ = gettext.gettext

errors = { }

def define_error(error_code, msg):
    global errors
    error_info = EngageErrInf(__name__, error_code, msg)
    errors[error_info.error_code] = error_info

# error codes
# FILL IN
ERR_TBD = 0

define_error(ERR_TBD,
             _("Replace this with your error codes"))


# setup logging
from engage.utils.log_setup import setup_engage_logger
logger = setup_engage_logger(__name__)


# this is used by the package manager to locate the packages.json
# file associated with the driver
def get_packages_filename():
    return engage.drivers.utils.get_packages_filename(__file__)

def make_context(resource_json, sudo_password_fn, dry_run=False):
    """Create a Context object (defined in engage.utils.action). This contains
    the resource's metadata in ctx.props, references to the logger and sudo
    password function, and various helper functions. The context object is used
    by individual actions.

    If your resource does not need the sudo password, you can just pass in
    None for sudo_password_fn.
    """
    ctx = Context(resource_json, logger, __file__,
                  sudo_password_fn=sudo_password_fn,
                  dry_run=dry_run)
    ctx.check_port('input_ports.host',
                  genforma_home=unicode,
                  log_directory=unicode)
    ctx.check_port('input_ports.schema_file',
                  file_path=unicode)
    ctx.check_port('input_ports.jvm',
                  java_exe=unicode)
    ctx.check_port('output_ports.solr',
                  home=unicode)

    # add any extra computed properties here using the ctx.add() method.
    home = ctx.props.output_ports.solr.home
    example = os.path.join(home, "example")
    ctx.add('startup_jar_file',
            os.path.join(example, "start.jar"))
    ctx.add('schema_file_target',
            os.path.join(example, 'solr/conf/schema.xml'))
    ctx.add('pid_file',
            os.path.join(home, "apache_solr.pid"))
    ctx.add('log_file',
            os.path.join(ctx.props.input_ports.host.log_directory,
                         "apache_solr.log"))
    return ctx

@make_value_action
def is_solr_alive(self, solr_url='http://localhost:8983/solr/select'):
    try:
        with closing(urllib.urlopen(solr_url)) as aiu:
            aiu.read()
        return True
    except IOError:
        return False


class Manager(service_manager.Manager):
    REQUIRES_ROOT_ACCESS = False
    def __init__(self, metadata, dry_run=False):
        package_name = "%s %s" % (metadata.key["name"],
                                  metadata.key["version"])
        service_manager.Manager.__init__(self, metadata, package_name)
        self.ctx = make_context(metadata.to_json(),
                                None,
                                dry_run=dry_run)

    def validate_pre_install(self):
        p = self.ctx.props
        self.ctx.r(check_installable_to_dir,
                   p.output_ports.solr.home)

    def is_installed(self):
        return os.path.exists(self.ctx.props.output_ports.solr.home)

    def install(self, package):
        p = self.ctx.props
        self.ctx.r(extract_package_as_dir, package,
                   p.output_ports.solr.home)
        self.ctx.r(wrap_action(shutil.copy),
                   p.input_ports.schema_file.file_path,
                   p.schema_file_target)

    def validate_post_install(self):
        p = self.ctx.props
        self.ctx.r(check_dir_exists,  p.output_ports.solr.home)

    def start(self):
        p = self.ctx.props
        command_exe = p.input_ports.jvm.java_exe
        self.ctx.r(start_server,
                   [command_exe, "-jar", p.startup_jar_file],
                   p.log_file,
                   p.pid_file,
                   cwd=os.path.dirname(p.startup_jar_file))
        self.ctx.check_poll(10, 20, lambda r: r, is_solr_alive)

    def is_running(self):
        p = self.ctx.props
        return self.ctx.rv(get_server_status,
                           p.pid_file) != None

    def stop(self):
        p = self.ctx.props
        self.ctx.r(stop_server, p.pid_file)

    def get_pid_file_path(self):
        return self.ctx.props.pid_file

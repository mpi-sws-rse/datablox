
"""Resource manager for mongodb 2.0 
"""

# Common stdlib imports
import sys
import os
import os.path
## import commands

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
    ctx.check_port('config_port',
                  home=unicode,
                  log_file=unicode,
                  port=int)
    ctx.check_port('input_ports.host',
                  genforma_home=unicode,
                  os_user_name=unicode,
                  hostname=unicode,
                  log_directory=unicode)
    ctx.check_port('output_ports.mongodb',
                  home=unicode,
                  hostname=unicode,
                  port=int)

    # add any extra computed properties here using the ctx.add() method.
    ctx.add("server_exe", os.path.join(ctx.props.config_port.home, "bin/mongod"))
    ctx.add("dbpath", os.path.join(ctx.props.config_port.home, "db"))
    ctx.add("startup_log_file", os.path.join(os.path.dirname(ctx.props.config_port.log_file),
                                             "mongodb_startup.log"))
    ctx.add("pid_file", os.path.join(ctx.props.dbpath, "mongod.lock"))
    return ctx


# Now, define the main resource manager class for the driver. If this driver is
# a service, inherit from service_manager.Manager instead of
# resource_manager.Manager. If you need the sudo password, add
# PasswordRepoMixin to the inheritance list.
#
class Manager(service_manager.Manager):
    def __init__(self, metadata, dry_run=False):
        package_name = "%s %s" % (metadata.key["name"],
                                  metadata.key["version"])
        service_manager.Manager.__init__(self, metadata, package_name)
        self.ctx = make_context(metadata.to_json(),
                                None, # self._get_sudo_password,
                                dry_run=dry_run)

    def validate_pre_install(self):
        p = self.ctx.props
        self.ctx.r(check_installable_to_dir, p.config_port.home)


    def is_installed(self):
        return os.path.exists(self.ctx.props.config_port.home)

    def install(self, package):
        p = self.ctx.props
        self.ctx.r(extract_package_as_dir, package,
                   p.config_port.home)

    def validate_post_install(self):
        p = self.ctx.props
        self.ctx.r(check_dir_exists,  p.config_port.home)
        self.ctx.r(check_file_exists, p.server_exe)

    def start(self):
        p = self.ctx.props
        r = self.ctx.r
        r(ensure_dir_exists, p.dbpath)
        r(run_program,
          [p.server_exe, "--fork", "--dbpath", p.dbpath,
           "--logpath", p.config_port.log_file,
           "--port", str(p.config_port.port),
           "--logappend"],
          cwd=os.path.dirname(p.server_exe))
        self.ctx.check_poll(10, 1.0, lambda x: x!=None, get_server_status, p.pid_file)

    def stop(self):
        self.ctx.r(stop_server, self.ctx.props.pid_file)

    def is_running(self):
        return self.ctx.rv(get_server_status, self.ctx.props.pid_file)!=None

    def get_pid_file_path(self):
        return self.ctx.props.pid_file

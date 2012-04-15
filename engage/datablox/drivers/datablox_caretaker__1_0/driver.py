
"""Resource manager for datablox-caretaker 1.0 
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
ERR_BAD_PW_FILE = 1

define_error(ERR_BAD_PW_FILE,
             _("Master password file path seems to be changed from the default, which is not supported. Expecting %(exp)s, got %(act)s"))


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
                  pid_file=unicode,
                  config_dir=unicode,
                  log_file=unicode)
    ctx.check_port('input_ports.datablox_framework',
                  caretaker_exe=unicode,
                  BLOXPATH=unicode)
    ctx.check_port('input_ports.host',
                   genforma_home=unicode,
                   log_directory=unicode)

    # add any extra computed properties here using the ctx.add() method.
    return ctx


def np(path):
    return os.path.abspath(os.path.expanduser(path))
    
# Now, define the main resource manager class for the driver. If this driver is
# a service, inherit from service_manager.Manager instead of
# resource_manager.Manager. If you need the sudo password, add
# PasswordRepoMixin to the inheritance list.
#
class Manager(service_manager.Manager):
    # need to ensure that a password file is generated, as there might
    # be blocks that require a password.
    REQUIRES_PASSWORD_FILE = True
    def __init__(self, metadata, dry_run=False):
        package_name = "%s %s" % (metadata.key["name"],
                                  metadata.key["version"])
        service_manager.Manager.__init__(self, metadata, package_name)
        self.ctx = make_context(metadata.to_json(),
                                None, # self._get_sudo_password,
                                dry_run=dry_run)

    def validate_pre_install(self):
        pass
        ## """The password file resource lets one change the location of
        ## the master password file, but we don't currently support that.
        ## """
        ## p = self.ctx.props
        ## act_pw_file = np(p.input_ports.master_password_file.password_file)
        ## exp_pw_file = np(os.path.join(p.input_ports.host.genforma_home,
        ##                               "config/master.pw"))
        ## if act_pw_file != exp_pw_file:
        ##     raise UserError(errors[ERR_BAD_PW_FILE],
        ##                     msg_args={"act":act_pw_file,
        ##                               "exp":exp_pw_file})

    def is_installed(self):
        return os.path.exists(self.ctx.props.config_port.config_dir)

    def install(self, package):
        p = self.ctx.props
        self.ctx.r(ensure_dir_exists, p.config_port.config_dir)
        # BLOXPATH should be created by datablox framework?
        self.ctx.r(ensure_dir_exists,
                   p.input_ports.datablox_framework.BLOXPATH)

    def validate_post_install(self):
        pass

    def start(self):
        p = self.ctx.props
        self.ctx.r(start_server,
                   [p.input_ports.datablox_framework.caretaker_exe,
                    "--bloxpath=%s" % p.input_ports.datablox_framework.BLOXPATH,
                    "--config-dir=%s" % p.config_port.config_dir,
                    "--log-dir=%s" % p.input_ports.host.log_directory],
                   p.config_port.log_file,
                   p.config_port.pid_file)

    def stop(self):
        p = self.ctx.props
        self.ctx.r(stop_server,
                   p.config_port.pid_file)

    def is_running(self):
        p = self.ctx.props
        return self.ctx.rv(get_server_status,
                           p.config_port.pid_file) != None

    def get_pid_file_path(self):
        return self.ctx.props.config_port.pid_file

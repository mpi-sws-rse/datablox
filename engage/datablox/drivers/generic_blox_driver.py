
"""Generic Resource manager for databloxs.

This just extracts the archived Python code for the bock in the correct
place. To use it, you must defined two properties on the resource:
 * input_ports.datablock_framework.BLOXPATH - the blox directory
 * output_ports.block_info.home - the directory to be created for the block

The home directory should be a subdirectory of BLOXPATH.
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
    dir_to_add_to_python_path = os.path.abspath((os.path.join(os.path.dirname(__file__), "../../..")))
    sys.path.append(dir_to_add_to_python_path)

import engage.drivers.resource_manager as resource_manager
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
ERR_INVALID_HOME = 1

define_error(ERR_INVALID_HOME,
             _("Home path for resource %(id)s was %(home)s, which is not a subdirectory of BLOXPATH (%(blox)s)"))


# setup logging
from engage.utils.log_setup import setup_engage_logger
logger = setup_engage_logger(__name__)


# this is used by the package manager to locate the packages.json
# file associated with the driver
def get_packages_filename():
    return engage.drivers.utils.get_packages_filename(__file__)

def _normalize_path(path):
    return os.path.abspath(os.path.expanduser(path))

def make_context(resource_json, dry_run=False):
    """Create a Context object (defined in engage.utils.action). This contains
    the resource's metadata in ctx.props, references to the logger and sudo
    password function, and various helper functions. The context object is used
    by individual actions.

    """
    ctx = Context(resource_json, logger, __file__,
                  sudo_password_fn=None,
                  dry_run=dry_run)
    ctx.check_port('input_ports.datablox_framework',
                   BLOXPATH=unicode)
    ctx.check_port('output_ports.block_info',
                   home=unicode)
    p = ctx.props
    home_parent = _normalize_path(os.path.dirname(p.output_ports.block_info.home))
    bloxpath = _normalize_path(p.input_ports.datablox_framework.BLOXPATH)
    if home_parent != bloxpath:
        raise UserError(errors[ERR_INVALID_HOME],
                        msg_args={"id":p.id,
                                  "home":p.output_ports.block_info.home,
                                  "blox":bloxpath})
    return ctx


@make_action
def ensure_init_file_exists(self, parent_dir):
    """Create the __init__.py file in the directory if it is not present.
    """
    init_file = os.path.join(parent_dir, "__init__.py")
    if not os.path.exists(init_file):
        with open(init_file, "wb") as f:
            f.write("\n# Placeholder to ensure this directory is treated as a python module\n")


# Now, define the main resource manager class for the driver. If this driver is
# a service, inherit from service_manager.Manager instead of
# resource_manager.Manager. If you need the sudo password, add
# PasswordRepoMixin to the inheritance list.
#
class Manager(resource_manager.Manager):
    def __init__(self, metadata, dry_run=False):
        package_name = "%s %s" % (metadata.key["name"],
                                  metadata.key["version"])
        resource_manager.Manager.__init__(self, metadata, package_name)
        self.ctx = make_context(metadata.to_json(),
                                dry_run=dry_run)

    def validate_pre_install(self):
        p = self.ctx.props
        self.ctx.r(check_installable_to_dir,
                   p.output_ports.block_info.home)

    def is_installed(self):
        return os.path.exists(self.ctx.props.output_ports.block_info.home)

    def install(self, package):
        p = self.ctx.props
        self.ctx.r(ensure_dir_exists,
                   p.input_ports.datablox_framework.BLOXPATH)
        self.ctx.r(ensure_init_file_exists,
                   p.input_ports.datablox_framework.BLOXPATH)
        self.ctx.r(extract_package_as_dir, package,
                   p.output_ports.block_info.home)

    def validate_post_install(self):
        p = self.ctx.props
        self.ctx.r(check_dir_exists,  p.output_ports.block_info.home)


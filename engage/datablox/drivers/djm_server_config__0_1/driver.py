
"""Resource manager for djm-server-config 0.1 
"""

# Common stdlib imports
import sys
import os
import os.path

# fix path if necessary (if running from source or running as test)
try:
    import engage.utils
except:
    sys.exc_clear()
    dir_to_add_to_python_path = os.path.abspath((os.path.join(os.path.dirname(__file__), "../../../..")))
    sys.path.append(dir_to_add_to_python_path)

import engage.drivers.resource_manager as resource_manager
import engage.drivers.utils
# Drivers compose *actions* to implement their methods.
from engage.drivers.action import *
import engage.utils.system_info as system_info

MODEL_ADAPTER_CLASS="dist_job_mgr.mem_model.ModelAdapter"

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
                  genforma_home=unicode)
    ctx.check_port('input_ports.pkg_info',
                  version=unicode)
    ctx.check_port('output_ports.djm_server',
                  server_config_dir=unicode)

    # add any extra computed properties here using the ctx.add() method.
    ctx.add("djm_package_file",
            os.path.join(ctx.props.input_ports.host.genforma_home,
                         "engage/sw_packages/dist_job_mgr-%s.tar.gz" %
                         ctx.props.input_ports.pkg_info.version))
    ctx.add("djmctl",
            os.path.join(ctx.props.input_ports.host.genforma_home,
                         "python/bin/djmctl"))
    ctx.add("djm_config_file",
            os.path.join(ctx.props.output_ports.djm_server.server_config_dir,
                         "djmcfg.json"))
    return ctx

    
#
# Now, define the main resource manager class for the driver.
# If this driver is a service, inherit from service_manager.Manager.
# If the driver is just a resource, it should inherit from
# resource_manager.Manager. If you need the sudo password, add
# PasswordRepoMixin to the inheritance list.
#
class Manager(resource_manager.Manager):
    # Uncomment the line below if this driver needs root access
    ## REQUIRES_ROOT_ACCESS = True 
    def __init__(self, metadata, dry_run=False):
        package_name = "%s %s" % (metadata.key["name"],
                                  metadata.key["version"])
        resource_manager.Manager.__init__(self, metadata, package_name)
        self.ctx = make_context(metadata.to_json(),
                                None, # self._get_sudo_password,
                                dry_run=dry_run)

    def validate_pre_install(self):
        pass

    def is_installed(self):
        return os.path.exists(self.ctx.props.djm_config_file)

    def install(self, package):
        p = self.ctx.props
        r = self.ctx.r
        if os.path.exists(p.djm_package_file):
            djm_package_arg = ["--djm-package=%s" % p.djm_package_file,]
            logger.debug("djm_package is %s" % p.djm_package_file)
        else:
            djm_package_args = []
            logger.debug("djm_package is None, did not find %s" %
                         p.djm_package_file)
        r(check_file_exists, p.djmctl)
        r(ensure_dir_exists, p.output_ports.djm_server.server_config_dir)
        cmd = [p.djmctl, "setup-server",
               "--server-config-dir=%s" % p.output_ports.djm_server.server_config_dir]
        cmd.extend(djm_package_args)
        r(run_program, cmd, cwd=p.output_ports.djm_server.server_config_dir)
        cmd = [p.djmctl, "set-server-directory",
               p.output_ports.djm_server.server_config_dir]
        r(run_program, cmd, cwd=p.output_ports.djm_server.server_config_dir)
        # add the current node as "master"
        machine_info = system_info.get_machine_info()
        ## cmd = [p.djmctl, "add-node"]
        ## cmd.append("--hostname=%s" % machine_info["hostname"])
        ## cmd.append("--os-user=%s" % machine_info["username"])
        ## if machine_info["private_ip"]!=None:
        ##     cmd.append("--private-ip=%s" % machine_info["private_ip"])
        ## if machine_info["public_ip"]!=None:
        ##     cmd.append("--public-ip=%s" % machine_info["public_ip"])
        ## cmd.append("master")
        cmd = [p.djmctl, "add-master-node", "--bootstrap"]
        r(run_program, cmd, cwd=p.output_ports.djm_server.server_config_dir)
                   

    def validate_post_install(self):
        self.ctx.r(check_file_exists,  self.ctx.props.djm_config_file)



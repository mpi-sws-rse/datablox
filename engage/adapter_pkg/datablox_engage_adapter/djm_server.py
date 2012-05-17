"""Adapter layer to distributed job manager server api
"""
import os
import os.path
import logging
import datetime

from fabric.api import *
from fabric.operations import put
import fabric.network

from file_locator import FileLocator
import utils
from dist_job_mgr.client import get_local_connection
from dist_job_mgr.version import VERSION
import dist_job_mgr.common as common

class DjmAdapterError(Exception):
    pass

logger = logging.getLogger(__name__)


def get_djm_connection():
    fl = FileLocator()
    return get_local_connection(fl.get_djm_server_dir())

class DjmJob(object):
    def __init__(self, c, job_id, nodes):
        self.c = c
        self.job_id = job_id
        self.nodes = nodes
        self.nodes_by_name = {}
        for node in nodes:
            if node["private_ip"]!=None:
                ip_address = node["private_ip"]
            else:
                ip_address = node["public_ip"]
            if not ip_address:
                raise Exception("Neither public ip address nor private ip address specified for node %s, need to specify at least one" % node["name"])
            node["datablox_ip_address"] = ip_address
            self.nodes_by_name[node["name"]] = node
        self.nodes_except_master = filter(lambda name: name!="master",
                                          [node["name"] for node in self.nodes])
        # set up the fabric nodes
        env.hosts = [node["name"] for node in self.nodes]
        env.roledefs['workers'] = self.nodes_except_master
        for node in nodes:
            env.hostdefs[node["name"]] = "%s@%s" % (node["os_username"],
                                                    node["contact_address"])
            logger.debug("Node %s defined as %s" % (node["name"],
                                                    env.hostdefs[node["name"]]))
        if "master" in env.hosts:
            env.roledefs['master'] = ['master',]
        else:
            env.roledefs['master'] = []

    def has_node(self, node_name):
        return self.nodes_by_name.has_key(node_name)

    def get_node(self, node_name):
        return self.nodes_by_name[node_name]

    def stop_job(self, successful=True, msg=None):
        if successful:
            self.c.stop_job(self.job_id,
                            common.JobStatus.JOB_SUCCESSFUL,
                            comment=msg)
            logger.debug("Stopped job %s, status=JOB_SUCESSFUL" % self.job_id)
        else:
            self.c.stop_job(self.job_id,
                            common.JobStatus.JOB_FAILED,
                            comment=msg)
            logger.debug("Stopped job %s, status=FAILED" % self.job_id)
        fabric.network.disconnect_all()


@task
@parallel
@roles("workers")
def setup_worker_node(reuse_existing_installs):
    fl = FileLocator()
    dist_path = fl.get_engage_distribution_file()
    # todo: don't copy engage if existing install can be reused
    put(dist_path, "~/" + os.path.basename(dist_path))
    setup_script = os.path.join(fl.get_sw_packages_dir(), "setup_caretaker.sh")
    put(setup_script, "~/setup_caretaker.sh")
    run("chmod 755 ~/setup_caretaker.sh")
    run("~/setup_caretaker.sh")
    if reuse_existing_installs:
        run("~/setup_caretaker.sh --reuse-existing-install")
    else:
        run("~/setup_caretaker.sh")
    
def start_job_and_get_nodes(node_list, config_file_name, total_nodes=None,
                            reuse_existing_installs=True):
    """Given a node list and optional number of nodes, try to get the
    requested nodes and start a job.
    """
    if not total_nodes:
        total_nodes = len(node_list)
    if total_nodes<1:
        raise DjmAdapterError("Must have at least one node")
    c = get_djm_connection()
    # make sure there aren't any dead jobs laying around
    c.cleanup_dead_coordinators()
    pool = None
    for node_name in node_list:
        n = c.find_node_by_name(node_name)
        if not n:
            raise DjmAdapterError("Node '%s' not defined" % node_name)
        if n["pool"]:
            if pool and pool!=n["pool"]:
                raise DjmAdapterError("Cannot take nodes from both pool %s and pool %s"%
                                      (pool, n["pool"]))
            pool = n["pool"]
    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    j = c.start_job(config_file_name, common.JobType.ONE_TIME_JOB,
                    total_nodes, "Datablox job started %s" % start_time,
                    node_pool_name=pool, requested_nodes=node_list)
    logger.info("Started DJM job %s" % j)
    try:
        fl = FileLocator()
        allocated_nodes = c.query_nodes(job_id=j)
        djm_job = DjmJob(c, j, allocated_nodes)
        logger.info("Setting up nodes")
        # for all the non-master nodes, we setup the caretaker
        nodes_except_master = djm_job.nodes_except_master
        if len(nodes_except_master)>0:
            execute(setup_worker_node, reuse_existing_installs)
        # make sure the master node has the caretaker running
        if djm_job.has_node("master"):
            utils.run_svcctl(fl, ["start", "all"])
        return djm_job
    except KeyboardInterrupt:
        logger.exception("Got keyboard interrupt in node initialization")
        c.stop_job(j, common.JobStatus.JOB_FAILED,
                   comment="Got keyboard interrupt in node initialization")
        raise
    except Exception, e:
        logger.exception("DJM problem in node initialization: %s" % e)
        c.stop_job(j, common.JobStatus.JOB_FAILED,
                   comment="DJM problem in node initialization: %s" % e)
        raise


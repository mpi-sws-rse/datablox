"""Adapter layer to distributed job manager server api
"""
import os
import os.path
import logging
import datetime

from file_locator import FileLocator
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

    def has_node(self, node_name):
        return self.nodes_by_name.has_key(node_name)

    def get_node(self, node_name):
        return self.nodes_by_name[node_name]

    def stop_job(self, successful=True, msg=None):
        if successful:
            self.c.stop_job(self.job_id,
                            common.JobStatus.JOB_SUCCESSFUL,
                            comment=msg)
        else:
            self.c.stop_job(self.job_id,
                            common.JobStatus.JOB_FAILED,
                            comment=msg)

def _check_task_status(s, r, msg):
    if s!=common.TaskStatus.TASK_SUCCESSFUL:
        bad_nodes = filter(lambda res:
                           res.status!=common.TaskStatus.TASK_SUCCESSFUL,
                           r)
        raise Exception("%s for nodes: %s" %
                        (msg, [r.node_name for r in bad_nodes]))
        
def start_job_and_get_nodes(node_list, config_file_name, total_nodes=None):
    """Given a node list and optional number of nodes, try to get the
    requested nodes and start a job.
    """
    if not total_nodes:
        total_nodes = len(node_list)
    if total_nodes<1:
        raise DjmAdapterError("Must have at least one node")
    c = get_djm_connection()
    pool = None
    for node_name in node_list:
        n = c.find_node_by_name(node_name)
        if not n:
            raise DjmAdapterError("Node '%s' not defined" % n)
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
        nodes_except_master = filter(lambda name: name!="master",
                                     [node["name"] for node in allocated_nodes])
        if len(nodes_except_master)>0:
            (s, r) = c.run_task_on_node_list(j, "StartWorker", "start worker",
                                             nodes_except_master)
            _check_task_status(s, r, "DJM worker start failed")
            dist_path = fl.get_engage_distribution_file()
            logger.info("Copying engage distribution")
            (s, r) = c.run_task_on_node_list(j, "CopyFiles",
                                             "Copy engage distribution",
                                             nodes_except_master,
                                             dist_path,
                                             "~/" + os.path.basename(dist_path))
            _check_task_status(s, r, "Copying of engage distribution failed")
            (s, r) = c.run_task_on_node_list(j, "Command",
                                             "Setup remote caretaker",
                                             nodes_except_master,
                                             ["~/apps/engage/sw_packages/setup_caretaker.sh"])
            _check_task_status(s, r, "Caretaker setup script failed")
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


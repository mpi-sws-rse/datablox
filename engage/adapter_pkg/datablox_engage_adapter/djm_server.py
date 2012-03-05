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

    def stop_job(self, successful=True, msg=None):
        if successful:
            self.c.stop_job(self.job_id,
                            common.JobStatus.JOB_SUCCESSFUL,
                            comment=msg)
        else:
            self.c.stop_job(self.job_id,
                            common.JobStatus.JOB_FAILED,
                            comment=msg)

        
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
    try:
        allocated_nodes = c.query_nodes(job_id=j)
        return DjmJob(c, j, allocated_nodes)
    except Exception, e:
        logger.exception("DJM problem in node initialization: %s" % e)
        c.stop_job(j, common.JobStatus.JOB_FAILED,
                   comment="DJM problem in node initialization: %s" % e)
        raise


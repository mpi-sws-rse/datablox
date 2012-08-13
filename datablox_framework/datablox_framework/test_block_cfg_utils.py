
"""Unit tests for block configuration utilities"""

import unittest
import logging
import shutil
import tempfile
from os.path import join, abspath, expanduser

from block_cfg_utils import *


class DummyBlock(object):
    def __init__(self, id):
        self.id = id
        self.logger = logging.getLogger()

EXAMPLE_PROPERTIES = [
    required_prop('collection', validator=unicode,
                  help="Name of collection to dump"),
    required_prop('mongodb_home_path', validator=v_dir_exists,
                  transformer=t_fixpath,
                  help="Directory where mongodb is installed"),
    required_prop('crawl_id', validator=int,
                  help="Row id of crawl record in django"),
    required_prop('dump_directory', validator=v_dir_exists,
                  transformer=lambda name, base_dumpdir, block_inst:
                              join(abspath(expanduser(base_dumpdir)),
                                   "crawl_%d" % block_inst.crawl_id),
                  help="Directory under which crawl-specific dumps will be placed."),
    optional_prop('query', default=None,
                  validator=dict,
                  help="If specified, query parameter to filter rows to be dumped"),
    optional_prop('delete_data_after_dump', default=False, validator=bool,
                  help="If True, deletes the exported records from the collection")
]    

class TestBlockCfgUtils(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.cfg = {"collection":"all_file_data",
                    "collection2":"all_file_data",
                    "mongodb_home_path":self.tempdir,
                    "dump_directory":self.tempdir, "crawl_id":1,
                    "query":{}}
        self.block = DummyBlock("test_block")
        
    def tearDown(self):
        shutil.rmtree(self.tempdir)
        
    def test_valid_cfg(self):
        process_config(EXAMPLE_PROPERTIES, self.cfg, self.block)
        self.assertFalse(self.block.delete_data_after_dump)

    def test_invalid_type(self):
        try:
            self.cfg['crawl_id'] = 'bad value'
            process_config(EXAMPLE_PROPERTIES, self.cfg, self.block)
            self.fail("Did not get expected error")
        except BlockPropertyError, e:
            self.assertEqual(str(e),
                             "test_block: Property crawl_id has invalid value 'bad value'")

    def test_missing_required_prop(self):
        try:
            del self.cfg['crawl_id']
            process_config(EXAMPLE_PROPERTIES, self.cfg, self.block)
            self.fail("Did not get expected error")
        except BlockPropertyError, e:
            self.assertEqual(str(e),
                             "test_block: Required property crawl_id not specified")
            
    def test_opttype(self):
        del self.cfg['query']
        process_config(EXAMPLE_PROPERTIES, self.cfg, self.block)

    def test_opt_override(self):
        self.cfg['delete_data_after_dump'] = True
        process_config(EXAMPLE_PROPERTIES, self.cfg, self.block)
        self.assertTrue(self.block.delete_data_after_dump)
        

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()



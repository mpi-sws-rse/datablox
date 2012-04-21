#!/usr/bin/env python
# This is a sanity checker for the file crawler

import os
import os.path
import sys
import stat

try:
    import filetypes
except ImportError:
    raise Exception("You need to run with a python environment that has the filetypes package installed")

filetypes_utils_path = os.path.abspath(os.path.join(os.path.dirname(file), "../blox/filename_categorizer__1_0/"))
sys.path.append(filetypes_utils_path)

import filetypes_utils

def crawl_files(directory_to_crawl):
    path = os.path(os.path.expanduser(directory_to_crawl))
    assert os.path.isdir(path)
    for root, dirnames, filenames in os.walk(path):
        for filename in filenames:
            entry = []
            fpath = os.path.join(root, filename)
            entry.append(fpath)
            stat = os.stat(fpath)
            filesize = stat.st_size
            entry.append(filesize)
            (filetype, category) = filetype_utils.get_file_description_and_category(filepath)


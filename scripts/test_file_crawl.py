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

filetype_utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                    "../blox/filename_categorizer__1_0/"))
sys.path.append(filetype_utils_path)
import filetype_utils
    

def add_to_dict(d, key, value):
    if not d.has_key(key):
        d[key] = value
    else:
        d[key] = d[key] + value

def dict_to_csv(f, name, d):
    for key in d.keys():
        f.write("%s,%s,%s\n" % (name, key, d[key]))
                
def crawl_files(directory_to_crawl):
    path = os.path.abspath(os.path.expanduser(directory_to_crawl))
    assert os.path.isdir(path)
    print "Crawling directory %s" % path
    total_size = 0
    total_cnt = 0
    indexed_size = 0
    indexed_cnt = 0
    size_by_category = {}
    cnt_by_category = {}
    size_by_type = {}
    cnt_by_type = {}
    size_by_tag = {}
    cnt_by_tag = {}
    
    entries = [["Path", "Size", "Category", "Type", "Indexable?", "Tags"],]
    for root, dirnames, filenames in os.walk(path):
        for filename in filenames:
            entry = []
            fpath = os.path.join(root, filename)
            entry.append(fpath)
            stat = os.stat(fpath)
            filesize = stat.st_size
            total_size += filesize
            total_cnt += 1
            entry.append(str(filesize))
            (filetype, category) = filetype_utils.get_file_description_and_category(fpath)
            entry.append(category)
            add_to_dict(size_by_category, category, filesize)
            add_to_dict(cnt_by_category, category, 1)
            add_to_dict(size_by_type, filetype, filesize)
            add_to_dict(cnt_by_type, filetype, 1)
            entry.append(filetype)
            if filetype_utils.is_indexable_file(fpath):
                entry.append("Yes")
            else:
                entry.append("No")
            tags = filetype_utils.get_tags(fpath)
            if len(tags)>0:
                entry.append(tags[0])
                add_to_dict(size_by_tag, tags[0], filesize)
                add_to_dict(cnt_by_tag, tags[0], 1)
            else:
                entry.append("None")
                add_to_dict(size_by_tag, "untagged", filesize)
                add_to_dict(cnt_by_tag, "untagged", 1)
            entries.append(entry)
        print "Crawled %d files, for %3.2f MB total" % (total_cnt,
                                                        float(total_size)/
                                                        float(total_cnt)/1000000.0)
        datafile = os.path.abspath("./file_data.csv")
        with open(datafile, "w") as fd:
            for entry in entries:
                fd.write(", ".join(entry) + "\n")
        print "Wrote data to file %s" % datafile

        aggfile = os.path.abspath("./aggregate_data.csv")
        with open(aggfile, "w") as fa:
            fa.write("Group, Subgroup, Value\n")
            fa.write("Total, Count, %d\n" % total_cnt)
            fa.write("Total, Size, %d\n" % total_size)
            dict_to_csv(fa, "Cnt by Category", cnt_by_category)
            dict_to_csv(fa, "Size by Category", size_by_category)
            dict_to_csv(fa, "Cnt by Type", cnt_by_type)
            dict_to_csv(fa, "Size by Type", size_by_type)
            dict_to_csv(fa, "Cnt by Tag", cnt_by_tag)
            dict_to_csv(fa, "Size by Tag", size_by_tag)
        print "Wrote aggregates to file %s" % aggfile
            

def main():
    dirname = sys.argv[1]
    crawl_files(dirname)

main()

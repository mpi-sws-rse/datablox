"""Utility functions for determining information about files based on their
file extensions. We use the (unmaintained) filetypes module as a starting
point.

Copyright 2011 by genForma Corporation. Licenced under the Apache 2.0 license.

"""

import os.path
import re

try:
    import filetypes.filetypes
except ImportError:
    raise Exception("Unable to import the filetypes module. You can get it from PyPi at http://pypi.python.org/pypi/filetypes")

import mimetypes
if not mimetypes.inited:
  mimetypes.init()


# We have a map of extra files for certain well-known filenames. This is a
# map from filename to a (description, category, is_indexable) tuple.
extra_files = {
    "LICENSE": ("License file", "text", True)
}

# Map from extension to (description, category, is_indexable) tuple. This
# is used to override the defaults from the filetypes module
extra_extns = {
    "json": ("JSON file", "data", True), # picking data category to be consistent w/ XML
    "pdf" : ("Portable Document File", "text", False), # need to see if solr supports pdf,
    "qb2010" : ("QuickBooks 2010 data file", "data", False),
    "py" : ("Python Script", "development", True),
}

def _get_extn(filename):
    f = filename.lower()
    idx = f.rfind(".")
    if idx==(-1):
        return None # no file extension
    extn = f[idx+1:]
    # special case for .tar.gz file
    if extn=="gz" and f.endswith(".tar.gz"):
        return "tgz"
    else:
        return extn

def _get_filetypes_entry_by_ext(ext):
    """Probe the filetypes database for the given extension. We try to find
    a common file type and fallback to an uncommon one, if not present.
    Returns a (description, group) tuple.
    """
    common_files = []
    uncommon_files = []
    ft = filetypes.filetypes.filetypes
    for group in ft:
        if ext in ft[group]:
            if ft[group][ext][0] == 1:
                common_files.append((ft[group][ext][1], group),)
            else:
                uncommon_files.append((ft[group][ext][1], group),)
    if len(common_files)>0:
        return common_files[0]
    elif len(uncommon_files)>0:
        return uncommon_files[0]
    else:
        return ('unknown', 'unknown')


def _get_file_and_extension_overrides(filename):
    """Return a (description, category, is_indexable) tuple or
    (None, None, None) if entry not found. This function checks the
    extra_files and extra_extns maps.
    """
    if extra_files.has_key(filename):
        return extra_files[filename]
    else:
        extn = _get_extn(filename)
        if extn!=None and extra_extns.has_key(extn):
            return extra_extns[extn]
        else:
            return (None, None, None)

def get_file_description_and_category(filepath):
    """Given the path, return a (description, category) tuple or a
    ('unknown', 'unknown')
    tuple if the file type cannot be determined.
    """
    filename = os.path.basename(filepath)
    (d, c, i) = _get_file_and_extension_overrides(filename)
    if d!=None:
        return (d, c)
    else:
        extn = _get_extn(filename)
        if extn!=None:
            return _get_filetypes_entry_by_ext(extn)
        else:
            return ('unknown', 'unknown')


def is_indexable_file(filepath):
    """We can determine that a file is indexable by looking at the mimetype.
    Currently, we only support actual text files.
    """
    filename = os.path.basename(filepath)
    (d, c, i) = _get_file_and_extension_overrides(filename)
    if i!=None:
        return i
    else:
        (filetype, encoding) = mimetypes.guess_type(filename,
                                                    strict=False)
        if filetype and filetype.startswith("text"):
            return True
        else:
            return False

doc_pat = r"(\.xlsx$)|(\.xls$)|(\.doc$)|(\.docx$)|(\.pdf$)|(\.tax[0-9]*$)|(\.qbmb$)|(\.qb20[0-9][0-9]$)"
doc_re = re.compile(doc_pat)

tax_data = r"((^|\D)(1099|7004|w2|w4|940|941|1040|de9|de7|de6|1120)\D)|(\.qbmb$)|(\.qb20[0-9][0-9]$)|(\.tax[0-9]*$)"
tax_re = re.compile(tax_data)

fin_data = r"(receipt)|(payroll)|(accounts.receivable)|(accounts.payable)|(checking)|(savings)|(expense)"
fin_re = re.compile(fin_data)

legal_data = r"(patent)|(contract)|(agreement)|(nda)|(legal)|(loan)"
legal_re = re.compile(legal_data)

def get_tags(filepath):
    """Simulation of file content tagging. For now, just use a regexp on the
    filename"""
    filename = os.path.basename(filepath.lower())
    if not doc_re.search(filename):
        return []
    elif tax_re.search(filename):
        return ["tax"]
    elif fin_re.search(filename):
        return ["financial"]
    elif legal_re.search(filename):
        return ["legal"]
    else:
        return []


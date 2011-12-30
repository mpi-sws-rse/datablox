"""Utility functions for determining information about files based on their
file extensions. We use the (unmaintained) filetypes module as a starting
point.

Copyright 2011 by genForma Corporation. Licenced under the Apache 2.0 license.

"""

import os.path

try:
    import filetypes
except ImportError:
    raise Exception("Unable to import the filetypes module. You can get it from PyPi at http://pypi.python.org/pypi/filetypes")

import mimetypes
if not mimetypes.inited:
  mimetypes.init()


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
    
def get_type_description(filepath):
    extn = _get_extn(os.path.basename(filepath))
    if extn==None:
        return None
    else:
        types = filetypes.getByExtension(extn, only_common=False)
        if len(types)>0:
            return types[0]
        else:
            return None

def is_indexable_file(filepath):
    """We can determine that a file is indexable by looking at the mimetype.
    Currently, we only support actual text files.
    """
    (filetype, encoding) = mimetypes.guess_type(os.path.basename(filepath),
                                                strict=False)
    if filetype and filetype.startswith("text"):
        return True
    else:
        return False

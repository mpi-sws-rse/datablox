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

class FileMatchBase(object):
    """has some helper functions. Does not depend on any
    member fields.
    """
    def _get_filename(self, path):
        return os.path.basename(path)
    def _get_extn(self, path):
        return _get_extn(self._get_filename(path))
    
class FileMatchRule(FileMatchBase):
    """Base class for file matching rules.
    """
    def __init__(self, filetype, category, indexable):
        self.filetype = filetype
        self.category = category
        self.indexable = indexable
    def filetype_and_category(self, path):
        """Return the info corresponding to this file type. path is usually ignored,
        but is present for use by catchall rules (e.g. FiletypesLibRule). """
        return (self.filetype, self.category)

    def is_indexable(self, path):
        return self.indexable
    
    def has_match(self, path):
        """Override in subclass. Returns True if file matches this rule,
        False otherwise"""
        raise Exception("has_match not implemented for class %s" %
                        self.__class__.__name__)
        
class FileExtnRule(FileMatchRule):
    """Match specific file extensions"""
    def __init__(self, extn, filetype, category, indexable):
        FileMatchRule.__init__(self, filetype, category, indexable)
        self.extn = extn

    def has_match(self, path):
        return self.extn == self._get_extn(path)

class FileNameRule(FileMatchRule):
    """Match the full filename
    """
    def __init__(self, filename, filetype, category, indexable,
                 case_sensitive=True):
        FileMatchRule.__init__(self, filetype, category, indexable)
        self.filename = filename
        self.lc_filename = filename.lower()
        self.case_sensitive = case_sensitive

    def has_match(self, path):
        if self.case_sensitive:
            return self.filename == self._get_filename(path)
        else:
            return self.lc_filename == self._get_filename(path).lower()

class PathPatRule(FileMatchRule):
    """Use the regular expression pattern on the path to match.
    """
    def __init__(self, path_pattern, filetype, category, indexable):
        FileMatchRule.__init__(self, filetype, category, indexable)
        self.path_re = re.compile(path_pattern)

    def has_match(self, path):
        return self.path_re.search(path)!=None

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


class FiletypesLibRule(FileMatchBase):
    """Use the filetypes library to come up with a match.
    Note that this doesn't subclass from FileMatchRule, as the
    filetype info is not fixed.
    """
    def __init__(self):
        pass

    def has_match(self, path):
        extn = self._get_extn(path)
        if extn!=None:
            return True
        else:
            return False
    def filetype_and_category(self, path):
        extn = self._get_extn(path)
        return _get_filetypes_entry_by_ext(extn)

    def is_indexable(self, path):
        (filetype, encoding) = mimetypes.guess_type(self._get_filename(path),
                                                    strict=False)
        if filetype and filetype.startswith("text"):
            return True
        else:
            return False

class DefaultRule(FileMatchBase):
    def has_match(self, path):
        return True
    def filetype_and_category(self, path):
        return ("unknown", "unknown")
    def is_indexable(self, path):
        return False

# Rules for matching path to find file info. Should start with most precise and go to least
# precise. A given filetype should only correspond to a single category.
path_rules = [
    FileNameRule("LICENSE", "License file", "text", True),
    FileNameRule("config.sub", "Automake Configuration Script", "development", True),
    PathPatRule(r"Aquamacs\.app.*\.srt$", "Emacs SRecode file", "development", True),

    # structured text
    FileExtnRule("json", "JSON file", "data", True), # picking data category to be consistent w/ XML
    FileExtnRule("rst", "Restructured Text", "text", True),
    FileExtnRule("yaml", "Yet Another Markup Language (YAML) File", "text", True),

    # document files
    FileExtnRule("pdf", "Portable Document File", "text", False), # need to see if solr supports pdf
    FileExtnRule("ps", "PostScript", "text", False),
    FileExtnRule("eps", "Encapsulated PostScript", "text", False),
    FileExtnRule("manifest", "Manifest File", "text", False),

    # image files
    FileExtnRule("png", "Portable Network Graphic (PNG) Image", "Image", False),
    FileExtnRule("jpg", "JPEG Image", "Image", False),
    FileExtnRule("jpeg", "JPEG Image", "Image", False),
    FileExtnRule("tif", "Tagged Image File", "Image", False),
    FileExtnRule("tiff", "Tagged Image File", "Image", False),
    FileExtnRule("gif", "Graphical Interchange Format File", "Image", False),
    FileExtnRule("icns", "Mac OS X Icon Resource File", "Image", False),
    
    FileExtnRule("qb2010", "QuickBooks 2010 data file", "data", False),
    FileExtnRule("py", "Python Script", "development", True),
    FileExtnRule("pyc", "Compiled Python Script", "development", False),
    FileExtnRule("pyo", "Python Object File", "development", False),
    FileExtnRule("el", "Emacs Lisp File", "development", True),
    FileExtnRule("elc", "Compiled Emacs Lisp File", "development", False),
    FileExtnRule("tfm", "TeX Font Metric File", "font", False),
    FileExtnRule("mo", "GNU gettext Machine Object File", "data", False),
    FileExtnRule("po", "GNU gettext Portable Object File", "development", True),
    
    
    # For vmware filetypes see
    # http://www.vmware.com/support/ws55/doc/ws_learning_files_in_a_vm.html
    FileExtnRule("nvram", "VMware NVRAM File", "virtualization", False),
    FileExtnRule("vmdk", "VMware Disk File", "virtualization", False),
    FileExtnRule("vmem", "VMware Paging File", "virtualization", False),
    FileExtnRule("vmsd", "VMware Snapshot Metadata File", "virtualization", False),
    FileExtnRule("vmsn", "VMware Snapshot State File", "virtualization", False),
    FileExtnRule("vmss", "VMware Suspended State File", "virtualization", False),
    FileExtnRule("vmx", "VMware Configuration File", "virtualization", False),
    FileExtnRule("vmxf", "VMware Team Member File", "virtualization", True), # XML
    
    FileExtnRule("sub", "Subtitle File", "text", True), # force category text rather than video to be compatible with .srt
    FileExtnRule("srt", "Subtitle File", "text", True), # subtitle files are indexable
    FiletypesLibRule(),
    DefaultRule()
]


def match_path(path):
    for rule in path_rules:
        if rule.has_match(path):
            return rule
    assert 0, "Path %s bad: should alway match at least one rule" % path


def get_file_description_and_category(filepath):
    """Given the path, return a (description, category) tuple or a
    ('unknown', 'unknown')
    tuple if the file type cannot be determined.
    """
    return match_path(filepath).filetype_and_category(filepath)

def is_indexable_file(filepath):
    """We can determine that a file is indexable by looking at the mimetype.
    Currently, we only support actual text files.
    """
    return match_path(filepath).is_indexable(filepath)


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


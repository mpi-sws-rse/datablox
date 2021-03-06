#!/usr/bin/env python
"""
This utility generates random data for the file crawler. We use a two level
hierarchy to prevent having too many files in a given directory.
"""
import sys
import os
from os.path import abspath, join, dirname, exists, expanduser, isdir
from optparse import OptionParser
from math import ceil
import random
import string
import mimetypes
import shutil

MAX_FILES_PER_DIR = 1000
SMALL_FILE_SIZE = 1000
LARGE_FILE_SIZE = 1000000
DEFAULT_PCT_SMALL_FILES = 90
DEFAULT_PCT_TEXT_FILES = 50

known_extns = [".py", ".rst", ".html", ".docx"]

quiet = False

def msg(message):
    if not quiet:
        print message

def _get_extn(filename):
    f = filename.lower()
    idx = f.rfind(".")
    if idx==(-1):
        return None # no file extension
    extn = f[idx+1:]
    return extn
    
def is_indexable_file(filename):
  """We can determine that a file is indexable by looking at the mimetype.
  Currently, we only support actual text files.
  """
  if _get_extn(filename) in known_extns:
      return True
  (filetype, encoding) = mimetypes.guess_type(filename,
                                              strict=False)
  if filetype and filetype.startswith("text"):
      return True
  else:
      return False

def generate_words_from_file(filename):
    """Read a file and generate a set of words in the file"""
    words = set()
    with open(filename, "r") as f:
        for line in f:
            l = line.split()
            for word in l:
                if word.endswith(".") or word.endswith(","):
                    word = word[0:len(word)-1]
                use_word = True
                for c in word:
                    if c not in string.letters:
                        use_word = False
                        break
                if use_word:
                    words.add(word)
    return words

def generate_words(base_dir):
    base_dir = abspath(expanduser(base_dir))
    assert exists(base_dir)
    msg("Scanning for words under %s" % base_dir)
    words = set()
    for (dirpath, dirnames, filenames) in os.walk(base_dir):
        for filename in filenames:
            if is_indexable_file(filename):
                filepath = join(dirpath, filename)
                words_in_file = generate_words_from_file(filepath)
                words = words.union(words_in_file)
    return words

def get_num_dirs(num_files):
    return int(ceil(float(num_files)/float(MAX_FILES_PER_DIR)))

def get_dname(num_files, dir_idx):
    num_dirs = get_num_dirs(num_files)
    dir_digits = len(str(num_dirs))
    dir_num = str(dir_idx+1)
    padding = dir_digits - len(dir_num)
    return "dir_%s%s" % ("0"*padding, dir_num)
    
def make_dirs(root_directory, num_files, dry_run=False):
    if not exists(root_directory):
        msg("Creating %s" % root_directory)
        if not dry_run:
            os.makedirs(root_directory)
    num_dirs = get_num_dirs(num_files)
    msg("Making %d directories" % num_dirs)
    for i in range(num_dirs):
        dirname = get_dname(num_files, i)
        msg("Creating %s" % dirname)
        path = join(root_directory, dirname)
        if not dry_run:
            os.mkdir(path)


def make_random_file(filepath, size, words, magic_text=None):
    curr_size = 0
    num_words = 0
    word_list = [w for w in words]
    with open(filepath, "w") as f:
        if magic_text:
            f.write(magic_text)
            num_words += 1
            curr_size += len(magic_text)
        while curr_size < size:
            word = random.choice(word_list)
            num_words += 1
            d = num_words % 8
            if d!=1:
                f.write(" ")
                curr_size += 1
            f.write(word)
            curr_size += len(word)
            if d==0:
                f.write("\n")
                curr_size += len("\n")
            
        
def make_files(root_directory, num_files, words, pct_small, pct_txt,
               dry_run=False, dont_use_links=False, magic_text=None):
    msg("Creating files...")
    num_dirs = get_num_dirs(num_files)
    file_digits = len(str(num_files))
    file_paths = {}
    file_counts = {SMALL_FILE_SIZE:0, LARGE_FILE_SIZE:0}
    num_text_files = 0
    unique_files = 0
    for i in range(num_files):
        r1 = random.randint(1, 100)
        r2 = random.randint(1, 100)
        if r1<=pct_txt:
            ext = ".txt"
            num_text_files += 1
        else:
            ext = ".bin"
        if r2>pct_small:
            size = LARGE_FILE_SIZE
        else:
            size = SMALL_FILE_SIZE
        file_counts[size] += 1
        fnum = str(i+1)
        fname = "file_%s%s%s" % (0*(file_digits-len(fnum)), fnum, ext)
        d_idx = (i+1)%num_dirs
        dname = get_dname(num_files, d_idx)
        fpath = join(join(root_directory, dname), fname)
        if not dry_run:
            assert isdir(dirname(fpath)), "%s is a bad path" % fpath
        if dont_use_links:
            msg("Creating file %s, size %d" % (fpath, size))
            if not dry_run:
                make_random_file(fpath, size, words, magic_text)
                unique_files += 1
        elif file_paths.has_key(size):
            p = file_paths[size]
            msg("linking file %s to %s" % (fpath, p))
            if not dry_run:
                try:
                    os.link(p, fpath)
                except OSError, e:
                    msg("  Unable to create link, creating a new file")
                    make_random_file(fpath, size, words, magic_text)
                    file_paths[size] = fpath
                    unique_files += 1
        else:
            file_paths[size] = fpath
            msg("Creating file %s, size %d" % (fpath, size))
            if not dry_run:
                make_random_file(fpath, size, words, magic_text)
                unique_files += 1
    if not dry_run:
        unique = "%d" % unique_files
    else:
        unique = "N/A"
    print "status: small: %d  large: %d  text: %d  binary: %d  unique: %s" % \
          (file_counts[SMALL_FILE_SIZE], file_counts[LARGE_FILE_SIZE],
           num_text_files, num_files - num_text_files, unique)
        
        

def main(argv):
    global quiet
    usage = "%prog [options] root_directory num_files"
    parser = OptionParser(usage=usage)
    parser.add_option("-d", "--dry-run", dest="dry_run", default=False,
                      action="store_true",
                      help="If specified, just print what would be done without actually doing it")
    parser.add_option("-q", "--quiet", dest="quiet", default=False,
                      action="store_true",
                      help="Don't print anything other than file counts")
    parser.add_option("-u", "--unattended", dest="unattended", default=False,
                      action="store_true",
                      help="Bail out if user input is needed")
    parser.add_option("--dont-use-links", dest="dont_use_links", default=False,
                      action="store_true",
                      help="If specified, don't use links - create a new file for each entry")
    parser.add_option("-r", "--random", dest="random", default=False,
                      action="store_true",
                      help="If specified, let python initialize the random seed rather than using the same seed every time")
    parser.add_option("--pct-small-files", dest="pct_small_files",
                      default=None,
                      help="Percentage of small files (defaults to %d)" %
                      DEFAULT_PCT_SMALL_FILES)
    parser.add_option("--pct-text-files", dest="pct_text_files",
                      default=None,
                      help="Percentage of text files (defaults to %d)" %
                      DEFAULT_PCT_TEXT_FILES)
    parser.add_option("-m", "--magic-text", dest="magic_text", default=None,
                      help="If specified, include string in each file that is generated")
    parser.add_option("-w", "--words", dest="words", default=False, action="store_true",
                      help="If specified try to find the system words file (e.g. /usr/share/dict/words) and use that as a source of words")
    
    (options,args) = parser.parse_args(argv)
    if len(args) != 2:
        parser.print_help()
        parser.error("Expecting root_directory and num_files")
    root_directory = abspath(expanduser(args[0]))
    try:
        num_files = int(args[1])
    except ValueError:
        parser.error("num_files should be an integer")
    if not options.random:
        random.seed("This is a test")
    if options.quiet:
        quiet = True
    if options.pct_small_files:
        try:
            pct_small = int(options.pct_small_files)
            if pct_small<0 or pct_small>100: raise ValueError()
        except ValueError:
            parser.error("--pct-small-files should be an integer between 0 and 100")
    else:
        pct_small = DEFAULT_PCT_SMALL_FILES
    if options.pct_text_files:
        try:
            pct_text = int(options.pct_text_files)
            if pct_text<0 or pct_text>100: raise ValueError()
        except ValueError:
            parser.error("--pct-text-files should be an integer between 0 and 100")
    else:
        pct_text = DEFAULT_PCT_TEXT_FILES
    if options.words:
        words_file = None
        candidate_word_files = ['/usr/share/dict/words', '/usr/dict/words']
        for f in candidate_word_files:
            if os.path.exists(f):
                words_file = f
                break
        if not words_file:
            parser.error("Could not find words file in the following places: %s. If on ubuntu, make sure you have the wordlist package installed" %
                         ', '.join(candidate_word_files))
    else:
        words_file = None
    if os.path.exists(root_directory):
        if options.unattended:
            print "%s already exists, cannot continue due to --unattended option" % \
                  root_directory
            return 1
        print "%s already exists, delete it? [y/N]" % root_directory,
        l = sys.stdin.readline()
        if l.rstrip().lower()=="y":
            print "deleting %s" % root_directory
            if not options.dry_run:
                shutil.rmtree(root_directory)
        else:
            return 1
    msg("Will create %d pct small files and %d pct text files" % (pct_small, pct_text))
                

    if options.dry_run:
        msg("Executing a dry run...")
    make_dirs(root_directory, num_files, options.dry_run)
    if words_file:
        words = generate_words_from_file(words_file)
    else:
        words = generate_words(join(dirname(__file__), ".."))
    if options.magic_text and options.magic_text in words:
        words.remove(options.magic_text)
    msg("%d words found" % len(words))
    make_files(root_directory, num_files, words, pct_small, pct_text,
               dry_run=options.dry_run, dont_use_links=options.dont_use_links,
               magic_text=options.magic_text)
    
    return 0


if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))


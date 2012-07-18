"""Python script to test the filetypes_utils
"""
import sys
import os.path
import unittest
import logging
import filetype_utils

logger = logging.getLogger(__name__)

filelist_file = None
ft_to_category_history = {}

class TestFileTypes(unittest.TestCase):
    def _check_category(self, path, filetype, category):
        if ft_to_category_history.has_key(filetype):
            # check that filetype always maps to same category
            (prev_cat, prev_path) = ft_to_category_history[filetype]
            self.assertEqual(prev_cat, category,
                             "Filetype %s has inconsistent category mappings: %s maps to category %s, %s maps to category %s" %
                             (filetype, prev_cat, prev_path, category, path))
        else:
            ft_to_category_history[filetype] = (category, path)
        
    def _tc(self, path, filetype, category, indexable):
        """The work for running a single testcase"""
        (t, c) = filetype_utils.get_file_description_and_category(path)
        self.assertEqual(t, filetype,
                         "Expecting filetype %s for %s, got %s" %
                         (filetype, path, t))
        self.assertEqual(c, category,
                         "Expecting category %s for %s, got %s" %
                         (category, path, c))
        self._check_category(path, t, c)
        i = filetype_utils.is_indexable_file(path)
        self.assertEqual(i, indexable,
                         "Path %s is %sindexable, expecting %sindexable" %
                         (path, "" if i else "not ", "" if indexable else "not "))
               
    def testNameRule(self):
        self._tc("foo/bar/LICENSE", "License file", "text", True)

    def testExtnRule(self):
        self._tc("foo/bar/baz/foo.pdf", "Portable Document File", "text", False)

    def testFiletypesLibRule(self):
        self._tc("foo/bar/testing.mp3", 'MP3 Audio File', 'audio', False)

    def testDefaultRule(self):
        self._tc("foo/bar/alkjedlk33jhdl3jlc", "unknown", "unknown", False)

    def testSubtitleRules(self):
        self._tc("/Applications/Aquamacs.app/Contents/Resources/etc/srecode/wisent.srt",
                 "Emacs SRecode file", "development", True)
        self._tc("/Developer/usr/share/automake-1.10/config.sub",
                 "Automake Configuration Script", "development", True)
        self._tc("foo/bar/subtitles.srt",
                 "Subtitle File", "text", True)
        self._tc("foo/bar/subtitles.sub",
                 "Subtitle File", "text", True)

    def testFileList(self):
        """Optionally check all the filetype => category mappings for a list of
        paths provided in a file.
        """
        if filelist_file:
            logging.info("Running filelist test using file %s" % filelist_file)
            with open(filelist_file, "rb") as f:
                for line in f:
                    path = line.decode("utf-8").rstrip()
                    (t, c) = filetype_utils.get_file_description_and_category(path)
                    self._check_category(path, t, c)
        else:
            logging.info("Skipping filelist test. To run, specify --filelist=path, where path is a path to a list of files")
        
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    args = sys.argv
    for i in range(1,len(sys.argv)):
        if sys.argv[i].startswith("--filelist="):
            filelist_file = os.path.abspath(os.path.expanduser(sys.argv[i][len("--filelist="):]))
            if not os.path.exists(filelist_file):
                sys.stderr.write("Filelist file %s does not exist\n" % filelist_file)
                sys.exit(1)
            args = sys.argv[0:i] + sys.argv[i+1:]
            break
            
    unittest.main(argv=args)

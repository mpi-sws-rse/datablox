# -*- coding: utf-8 -*

"""Unit tests for url encryption"""
import unittest
import logging
import os
import os.path
from random import choice, randint
import string
import urlparse, urllib

from block import encrypt_path, decrypt_path, BlockUtils

#BASE_PATH = os.path.abspath(os.path.expanduser("/"))
BASE_PATH = os.path.abspath(os.path.expanduser("/etc/ssl/certs"))

PROBLEMATIC_PATHS = [
    unicode("/etc/ssl/certs/TÜBİTAK_UEKAE_Kök_Sertifika_Hizmet_Sağlayıcısı_-_Sürüm_3.pem",
            "utf-8"),
]

def gen_random(length, chars=string.letters+string.digits):
    return ''.join([ choice(chars) for i in range(length) ])

class TestURLEncryption(unittest.TestCase):
    def setUp(self):
        self.key = gen_random(8)
    def test_walk(self):
        bp = unicode(BASE_PATH)
        for root, dirnames, filenames in os.walk(bp):
            for filename in filenames:
                fpath = os.path.join(unicode(root), unicode(filename))
                #print "Path = %s" % fpath.encode("utf-8")
                encrypted_path = encrypt_path(fpath, self.key)
                decrypted_path = decrypt_path(encrypted_path, self.key)
                self.assertEqual(fpath, decrypted_path,
                                 "Decrypted path %s not equal to original path %s, encrypted version =%s, key=%s" %
                                 (decrypted_path, fpath, encrypted_path, self.key))
                
    def test_problematic_paths(self):
        for path in PROBLEMATIC_PATHS:
            encrypted_path = encrypt_path(path, self.key)
            decrypted_path = decrypt_path(encrypted_path, self.key)
            self.assertEqual(path, decrypted_path,
                             "Decrypted path %s not equal to original path %s, encrypted version =%s, key=%s" %
                             (decrypted_path, path, encrypted_path, self.key))

    def test_url_handling(self):
        bp = unicode(BASE_PATH)
        for root, dirnames, filenames in os.walk(bp):
            for filename in filenames:
                fpath = os.path.join(unicode(root), unicode(filename))
                url = BlockUtils.generate_url_for_path(fpath, key_for_testing=self.key)
                p = urlparse.urlparse(url)
                query_dict = urlparse.parse_qs(p.query)
                expected_len = long(query_dict["len"][0])
                encrypted_path = query_dict["key"][0]
                decrypted_path = decrypt_path(encrypted_path, self.key)
                self.assertEqual(fpath, decrypted_path,
                                 "Decrypted path %s not equal to original path %s, url='%s'" %
                                 (decrypted_path, encrypted_path, url))

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

"""Unit tests for url encryption"""

import unittest
import logging
import os
import os.path
from random import choice, randint
import string

from block import encrypt_path, decrypt_path

BASE_PATH = os.path.abspath(os.path.expanduser("/"))

def gen_random(length, chars=string.letters+string.digits):
    return ''.join([ choice(chars) for i in range(length) ])

class TestURLEncryption(unittest.TestCase):
    def setUp(self):
        self.key = gen_random(8)
    def test_walk(self):
        for root, dirnames, filenames in os.walk(BASE_PATH):
            for filename in filenames:
                fpath = os.path.join(root, filename)
                encrypted_path = encrypt_path(fpath, self.key)
                decrypted_path = decrypt_path(encrypted_path, self.key)
                self.assertEqual(fpath, decrypted_path,
                                 "Decrypted path %s not equal to original path %s, encrypted version =%s, key=%s" %
                                 (decrypted_path, fpath, encrypted_path, self.key))
                
    

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

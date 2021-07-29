from .utils import StorageTest
import unittest
from ..adls_gen1 import ADLSStorage

stg = ADLSStorage()


class TestADLGen1Storage(StorageTest, unittest.TestCase):

    def get_base_test_path(self):
        return 'test'

    def get_storage(self):
        return stg
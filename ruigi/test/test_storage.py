import unittest
from ruigi import Task
from ruigi.storage import adls_gen1

class TestAzureGen1Storage(unittest.TestCase):
    class Task1(Task):
        _storage = adls_gen1.AzureStorage
        def easy_run(self, inputs):
            return 'done'

    def test_target_is_saved(self):

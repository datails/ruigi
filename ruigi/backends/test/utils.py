import unittest
import os
from .. import adls_gen1
from pandas.util import testing as pd_test


class StorageTest():
    """ General Storage testing (Create, Read, Update, and Delete operations)
    """
    def get_base_test_path(self):
        return '.'

    def test_upload_and_download_file(self):
        local_file_path = 'TEST_FILE.txt'
        remote_file_path = self.get_base_test_path() + '/TEST_REMOTE_FILE.txt'

        try:
            with open(local_file_path, 'w') as fp:
                fp.write('TEST - OK')
            stg = self.get_storage()
            try:
                stg.save(remote_file_path, local_file_path, format='file')
                self.assertTrue(stg.exists(remote_file_path))
            finally:
                stg.delete(remote_file_path)
        finally:
            os.remove(local_file_path)

    def test_upload_and_download_pickle(self):
        remote_file_path = self.get_base_test_path() + '/TEST_REMOTE_FILE'

        df = pd_test.makeDataFrame()
        stg = self.get_storage()
        try:
            stg.save(remote_file_path, df, format='pickle')
            self.assertTrue(stg.exists(remote_file_path))
            df_remote = stg.load(remote_file_path, format='pickle')
            pd_test.assert_frame_equal(df, df_remote)
        finally:
            stg.delete(remote_file_path)

    def test_upload_and_download_parquet(self):
        remote_file_path = self.get_base_test_path() + '/TEST_REMOTE_FILE'

        df = pd_test.makeDataFrame()
        stg = self.get_storage()
        try:
            stg.save(remote_file_path, df, format='parquet')
            self.assertTrue(stg.exists(remote_file_path))
            df_remote = stg.load(remote_file_path, format='parquet')
            pd_test.assert_frame_equal(df, df_remote)

        finally:
            stg.delete(remote_file_path)


if __name__ == '__main__':
    unittest.main()

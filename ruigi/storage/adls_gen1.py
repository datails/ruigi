""" Azure Data Lake Storage Gen1
"""

import os
import pickle
import gzip
import pandas as pd
import joblib
from io import BytesIO
import tempfile

from azure.datalake.store import core, lib, multithread

__TEMP_STORAGE__ = os.path.join(tempfile.gettempdir(), 'ruigi')


class AzureStorage:
    def __init__(self, bucket_name, service_account_path='https://datalake.azure.net/', parent_folder='.'):
        os.makedirs(__TEMP_STORAGE__, exist_ok=True)
        self.parent_folder = parent_folder
        self.service_account_path = service_account_path
        self.base_path = '.'
        self.client = None
        self._init_client(bucket_name)

    def _init_client(self, bucket_name):
        """ Initialize Azure back-end
        """
        creds = lib.auth(url_suffix=bucket_name, resource=self.service_account_path)
        self.base_path = f"adl://{bucket_name}.azuredatalakestore.net/"
        self.client = core.AzureDLFileSystem(creds, store_name=bucket_name)

    def save(self, name, obj, format='pickle', chunk_size=None):
        """
        Args:
            name: `str`.
                Filename to be used when saving the `obj`
            obj: `obj`
                It depends on the `format` parameter.
            format: `str`
                Possible values:
                    1. `pickle`: It uses `pickle.dump` to save a gzip binary file.
                    2. `joblib`: It uses `joblib.dump` to save a BytesIO binary file.
                    3. `file`: It saves a local file sending it directly to GCS.
                    4. `parquet`: It saves a parquet file using pandas.to_parquet.
            chunk_size: `int` default `None`
                The size of a chunk of data whenever iterating (in bytes).
                This must be a multiple of 256 KB per the API specification.
        """
        remote_file_name = '/'.join([self.base_path, self.parent_folder, name])
        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))

        try:
            if format == 'parquet':
                if not isinstance(obj, pd.DataFrame):
                    raise ValueError(f"Object to be saved as parquet must be a "
                                     f"DataFrame. Received a {type(obj)}")
                obj.to_parquet(local_file_name)

            elif format == 'joblib':
                # TODO
                raise NotImplementedError('Azure Gen 1 save for joblib format not implemented')

            elif format == 'pickle':
                with gzip.open(local_file_name, 'wb') as f:
                    pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

            elif format != 'file':
                raise ValueError("Supported formats are pickle, joblib, file or parquet")

            multithread.ADLUploader(self.client,
                                    lpath=local_file_name,
                                    rpath=remote_file_name,
                                    nthreads=64,
                                    overwrite=True,
                                    buffersize=4194304,
                                    blocksize=4194304)
        finally:
            if os.path.exists(local_file_name):
                os.remove(local_file_name)

    def load(self, name, format='pickle', columns=None, chunk_size=None):
        """
        Args:
            name: `str`.
                Filename to be load
            format: `str`
                Possible values:
                    1. `pickle`: It uses `pickle.dump` to load a gzip binary file.
                    2. `joblib`: It uses `joblib.dump` to load a BytesIO binary file.
                    3. `file`: It saves a local file sending it directly to GCS.
                    4. `parquet`: It saves a parquet file using pandas.read_parquet.
            columns: `list` default `None`
                Columns to fetch when using `parquet=True`
            chunk_size: `int` default `None`
                The size of a chunk of data whenever iterating (in bytes).
                This must be a multiple of 256 KB per the API specification.
        """
        remote_file_name = '/'.join([self.base_path, self.parent_folder, name])
        blob = self.bucket.blob(remote_file_name, chunk_size=chunk_size)
        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))

        if not blob.exists():
            raise FileNotFoundError(f'Remote file {remote_file_name} not found')

        blob.reload()
        remote_ts = blob.updated.timestamp()

        if format == 'file':
            blob.download_to_filename(local_file_name)
            return local_file_name

        elif format == 'pickle':
            blob.download_to_filename(local_file_name)
            with gzip.open(local_file_name, 'rb') as f:
                return pickle.load(f)

        else:
            buffer = BytesIO()
            blob.download_to_file(buffer)

        if format == 'joblib':
            return joblib.load(buffer)

        elif format == 'parquet':
            return pd.read_parquet(buffer, columns=columns)

        else:
            raise ValueError("Supported formats are pickle, joblib, file or parquet")

    def exists(self, name):
        remote_file_name = os.path.join(self.base_path, self.parent_folder, name)
        return self.client.exists(remote_file_name)

    def delete(self, name):
        remote_file_name = os.path.join(self.base_path, self.parent_folder, name)
        self.client.rm(remote_file_name, recursive=True)

        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))
        if os.path.exists(local_file_name):
            os.remove(local_file_name)

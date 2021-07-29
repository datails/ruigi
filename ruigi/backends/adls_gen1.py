import os
import pickle
import gzip
import pandas as pd
import joblib
from io import BytesIO
import tempfile

from azure.datalake.store import core, lib

__TEMP_STORAGE__ = os.path.join(tempfile.gettempdir(), 'ruigi')


class ADLSStorage:
    def __init__(self, parent_folder=None, **kwargs):
        os.makedirs(__TEMP_STORAGE__, exist_ok=True)
        self.parent_folder = parent_folder
        self._init(**kwargs)

    def _init(self, token=None, store_name=None, resource=None, creds=None):
        """
        Initialize Azure back-end
        """
        self.token = token or os.environ.get('ADL_TOKEN', None)
        self.store_name = store_name or os.environ.get('ADL_STORE', None)
        resource = resource or os.environ.get('ADL_RESOURCE', 'https://datalake.azure.net/')
        self.base_url = f"adl://{self.store_name}.azuredatalakestore.net"

        if self.token is None:
            if creds is None:
                creds = lib.auth(url_suffix=self.store_name, resource=resource)
            self.client = core.AzureDLFileSystem(creds, store_name=self.store_name)
        else:
            self.client = core.AzureDLFileSystem(token=token, store_name=self.store_name)

    def _upload_local(self, local_file_name, remote_file_name):
        self.client.put(local_file_name, remote_file_name)

    def _upload_buffer(self, buffer, remote_file_name):
        with self.client.open(remote_file_name, 'wb') as f:
            f.write(buffer.getvalue())

    def save(self, name, obj, format='pickle', chunk_size=None):
        """ Save file to cloud

        Args:
            name: `str`.
                Filename to be used when saving the `obj`
            obj: `obj`
                It depends on the `format` parameter.
            format: `str`
                Possible values:
                    1. `pickle`: It uses `pickle.dump` to save a gzip binary file.
                    2. `joblib`: It uses `joblib.dump` to save a BytesIO binary file.
                    3. `file`: It saves a local file sending it directly to ADLS.
                    4. `parquet`: It saves a parquet file using pandas.to_parquet.
            chunk_size: `int` default `None`
                The size of a chunk of data whenever iterating (in bytes).
                This must be a multiple of 256 KB per the API specification.
        """
        remote_file_name = '/'.join([self.parent_folder, name]) if self.parent_folder else name
        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))

        if format == 'parquet':
            if not isinstance(obj, pd.DataFrame):
                # In case it is a Spark DF
                obj = obj.toPandas()
            try:
                obj.to_parquet(local_file_name)
                self._upload_local(local_file_name, remote_file_name)
            finally:
                os.remove(local_file_name)

        elif format == 'joblib':
            with BytesIO() as buffer:
                joblib.dump(obj, buffer)
                buffer.seek(0)
                self._upload_buffer(buffer, remote_file_name)
            return

        elif format == 'pickle':
            try:
                with gzip.open(local_file_name, 'wb') as f:
                    pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
                self._upload_local(local_file_name, remote_file_name)
            finally:
                os.remove(local_file_name)

        elif format == 'file':
            self._upload_local(obj, remote_file_name)

        else:
            raise ValueError(
                "Supported formats are pickle, joblib, file or parquet")

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
        remote_file_name = '/'.join([self.parent_folder, name]) if self.parent_folder else name
        local_file_name = os.path.join(
            __TEMP_STORAGE__, remote_file_name.replace("/", "-"))

        if format == 'file':
            with self.client.open(remote_file_name, 'rb') as fr:
                with open(local_file_name, 'wb') as fw:
                    fw.write(fr)
                return local_file_name

        elif format == 'joblib':
            with self.client.open(remote_file_name, 'rb') as fr:
                return joblib.load(fr)

        if format == 'parquet':
            with self.client.open(remote_file_name, 'rb') as fr:
                return pd.read_parquet(fr, columns=columns)

        elif format == 'pickle':
            with self.client.open(remote_file_name, 'rb') as fr:
                try:
                    # Load ZIP
                    with open(local_file_name, 'wb') as fw:
                        fw.write(fr.read())
                    # Unzip
                    with gzip.open(local_file_name, 'rb') as gr:
                        obj = pickle.load(gr)
                        return obj
                finally:
                    os.remove(local_file_name)
        else:
            raise ValueError("Supported formats are pickle, joblib, file or parquet")

    def exists(self, path):
        path = '/'.join([self.parent_folder, path]) if self.parent_folder else path
        return self.client.exists(path)

    def list_dir(self, path):
        path = '/'.join([self.parent_folder, path]) if self.parent_folder else path
        return self.client.ls(path)

    def delete(self, name):
        remote_file_name = '/'.join([self.parent_folder, name]) if self.parent_folder else name
        self.client.remove(remote_file_name)

        local_file_name = os.path.join(
            __TEMP_STORAGE__, remote_file_name.replace("/", "-"))

        if os.path.isfile(local_file_name):
            os.remove(local_file_name)

import os
import pickle
import gzip
import pandas as pd
import joblib
from io import BytesIO
import tempfile
from retry import retry
from google.resumable_media import DataCorruption
from google.api_core.exceptions import GatewayTimeout, ServiceUnavailable
from google.oauth2 import service_account
from google.cloud import storage

_RETRY_LIST = (GatewayTimeout, DataCorruption, ServiceUnavailable)
__TEMP_STORAGE__ = os.path.join(tempfile.gettempdir(), 'ruigi')

class GoogleStorage:
    def __init__(self, service_account_path, project, bucket_name, parent_folder='', ):
        os.makedirs(__TEMP_STORAGE__, exist_ok=True)

        self.project = project
        self.bucket_name = bucket_name
        self.parent_folder = parent_folder
        self._init_gcp( project, service_account_path)

    def _init_gcp(self,  project, service_account_path):
        """
        Initialize GCP back-end

        """

        #TODO: USe envs variables.
        gcp_credentials = service_account.Credentials.from_service_account_file(service_account_path)
        self.client = storage.Client(credentials=gcp_credentials, project=project)
        self.bucket = self.client.bucket(self.bucket_name)


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

        remote_file_name = os.path.join(self.parent_folder, name)
        blob = self.bucket.blob(remote_file_name, chunk_size=chunk_size)
        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))

        if format=='parquet':
            if not isinstance(obj, pd.DataFrame):
                raise ValueError(f"Object to be saved as parquet must be a "
                                 f"DataFrame. Received a {type(obj)}")
            obj.to_parquet(local_file_name)
        elif format == 'joblib':
            with BytesIO() as buffer:
                joblib.dump(obj, buffer)
                buffer.seek(0)
                blob.upload_from_file(buffer)
            return
        elif format == 'pickle':
            with gzip.open(local_file_name, 'wb') as f:
                pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
        elif format == 'file':
            local_file_name = obj
        else:
            raise ValueError("Supported formats are pickle, joblib, file or parquet")

        blob.upload_from_filename(filename=local_file_name)

    @retry(_RETRY_LIST, tries=5)
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

        remote_file_name = os.path.join(self.parent_folder, name)
        blob = self.bucket.blob(remote_file_name, chunk_size=chunk_size)
        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))

        if not blob.exists():
            raise FileNotFoundError(f'Remote file {remote_file_name} not found')

        blob.reload()
        remote_ts = blob.updated.timestamp()

        if format == 'file':
            blob.download_to_filename(local_file_name)
            return local_file_name

        else:
            buffer = BytesIO()
            blob.download_to_file(buffer)
        
        if format == 'joblib':
            return joblib.load(buffer)
        elif format == 'parquet':
            return pd.read_parquet(buffer, columns=columns)
        elif format == 'pickle':
            blob.download_to_filename(local_file_name)
            with gzip.open(local_file_name, 'rb') as f:
                return pickle.load(f)
        else:
            raise ValueError("Supported formats are pickle, joblib, file or parquet")


    def exists(self, name):

        remote_file_name = os.path.join(self.parent_folder, name)
        blob = self.bucket.blob(remote_file_name)
        return blob.exists()

    def delete(self, name):
        remote_file_name = os.path.join(self.parent_folder, name)
        blob = self.bucket.blob(remote_file_name)
        if blob.exists():
            blob.delete()

        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))
        if os.path.isfile(local_file_name):
            os.remove(local_file_name)
import os
import pickle
import gzip
import pandas as pd
import joblib
from io import BytesIO
import tempfile
from retry import retry
import boto3
import botocore.exceptions

_RETRY_LIST = ()
__TEMP_STORAGE__ = os.path.join(tempfile.gettempdir(), 'ruigi')


class S3Storage:
    def __init__(self, bucket_name, aws_access_key_id=None,
                 aws_secret_access_key=None, aws_session_token=None, parent_folder='', ):
        os.makedirs(__TEMP_STORAGE__, exist_ok=True)

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.bucket_name = bucket_name
        self.parent_folder = parent_folder
        self._init()

    def _init(self,):
        """
        Initialize S3 back-end

        """

        # TODO: USe envs variables.
        self.client = boto3.resource(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token
        )
        self.bucket = self.client.Bucket(self.bucket_name)

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
        local_file_name = os.path.join(
            __TEMP_STORAGE__, remote_file_name.replace("/", "-"))

        if format == 'parquet':
            if not isinstance(obj, pd.DataFrame):
                raise ValueError(f"Object to be saved as parquet must be a "
                                 f"DataFrame. Received a {type(obj)}")
            obj.to_parquet(local_file_name)
        elif format == 'joblib':
            with BytesIO() as buffer:
                joblib.dump(obj, buffer)
                buffer.seek(0)
                self.bucket.upload_fileobj(buffer, remote_file_name)
            return
        elif format == 'pickle':
            with gzip.open(local_file_name, 'wb') as f:
                pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
        elif format == 'file':
            local_file_name = obj
        else:
            raise ValueError(
                "Supported formats are pickle, joblib, file or parquet")

        self.bucket.upload_file(local_file_name, remote_file_name)

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
        obj = self.bucket.Object(remote_file_name, )
        local_file_name = os.path.join(
            __TEMP_STORAGE__, remote_file_name.replace("/", "-"))

        if obj is None:
            raise FileNotFoundError(
                f'Remote file {remote_file_name} not found')

        if format == 'file':
            self.bucket.download_file(remote_file_name, local_file_name)
            return local_file_name

        else:
            buffer = BytesIO()
            self.bucket.download_fileobj(remote_file_name, buffer)

        if format == 'joblib':
            return joblib.load(buffer)
        elif format == 'parquet':
            return pd.read_parquet(buffer, columns=columns)
        elif format == 'pickle':
            self.bucket.download_file(remote_file_name, local_file_name)
            with gzip.open(local_file_name, 'rb') as f:
                return pickle.load(f)
        else:
            raise ValueError(
                "Supported formats are pickle, joblib, file or parquet")

    def exists(self, name):

        remote_file_name = os.path.join(self.parent_folder, name)
        obj = self.bucket.Object(remote_file_name)
        try:
            obj.load()
        except botocore.exceptions.ClientError:
            return False
        else:
            return True

    def delete(self, name):
        remote_file_name = os.path.join(self.parent_folder, name)
        obj = self.bucket.Object(remote_file_name)
        if obj is not None:
            obj.delete()

        local_file_name = os.path.join(
            __TEMP_STORAGE__, remote_file_name.replace("/", "-"))
        if os.path.isfile(local_file_name):
            os.remove(local_file_name)

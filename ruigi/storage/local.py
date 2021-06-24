import os
import pandas as pd
import joblib


class LocalStorage:

    def save(self, name, obj, format='pickle', chunk_size=None, **kwargs):
        """
        Args:
            name: `str`.
                Filename to be used when saving the `obj`
            obj: `obj`
                It depends on the `format` parameter.
            format: `str`
                Possible values:
                    1. `pickle`: It uses `pickle.dump` to save a gzip binary file.
                    2. `file`: It saves a local file.
                    3. `parquet`: It saves a parquet file using pandas.to_parquet.
        """
        os.makedirs(os.path.dirname(name), exist_ok=True)

        if format == 'parquet':
            if not isinstance(obj, pd.DataFrame):
                raise ValueError(f"Object to be saved as parquet must be a "
                                 f"DataFrame. Received a {type(obj)}")
            obj.to_parquet(name)

        elif format == 'pickle':
            joblib.dump(obj, name)

        elif format == 'file':
            with open(name, 'w') as fp:
                fp.write(obj)

        elif format == 'json':
            obj.to_json(name)

        else:
            raise ValueError("Supported formats are file, pickle or parquet")

    def load(self, name, format='pickle', columns=None, **kwargs):
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
        if format == 'file':
            with open(name, 'r') as fp:
                return fp.read()

        if format == 'joblib':
            return joblib.load(name)

        elif format == 'parquet':
            return pd.read_parquet(name, columns=columns, **kwargs)

        elif format == 'json':
            return pd.read_json(name, **kwargs)

        elif format == 'pickle':
            return joblib.load(name)

        else:
            raise ValueError("Supported formats are pickle, joblib, file or parquet")
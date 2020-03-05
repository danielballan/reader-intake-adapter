import dask.dataframe


class FWFReader:
    """
    Accepts file, filepath, or filepath glob.
    """
    container = 'dask.dataframe.core.DataFrame'

    def __init__(self, file):
        # read_fwf requires a filepath and will not accept a file buffer.
        if isinstance(file, str):
            self._filepath = file
        else:
            self._filepath = file.name
        self._closed = False

    def __repr__(self):
        return f"{self.__class__.__name__}({self._filepath!r})"

    def read(self):
        if self._closed:
            raise Closed(f"{self} is closed and can no longer be read.")
        return dask.dataframe.read_fwf(self._filepath)

    def close(self):
        self._closed = True
        # Nothing to clean up.

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self.close()


class Closed(Exception):
    ...


# intake compatibility
from reader_adapter import adapt  # noqa
FWFDataSource = adapt(FWFReader, 'FWFDataSource')

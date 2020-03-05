"""
Adapters for common 'container' types.
"""
from ._base import ReaderAdapter
from ._vendored import Schema


class DaskArrayAdapter(ReaderAdapter):
    """
    Wrap a Reader that returns a dask.array.Array in the DataSource API.

    We expect a subclass to define a _reader_class attribute.
    """
    container = 'ndarray'
    partition_access = True

    def __init__(self, *args, metadata=None, storage_options=None, **kwargs):
        # Instantiate and stash our Reader (subclass) instance.
        self.__reader = self._reader_class(*args, **kwargs)
        # Verify that its 'container' matches the type that this adapater
        # expects. If not, the adapter has been misapplied, and we should fail
        # early.
        EXPECTED_CONTAINER = 'dask.array.Array'
        if self.__reader.container != EXPECTED_CONTAINER:
            raise TypeError(
                "Expected Reader with container {EXPECTED_CONTAINER!r}")
        self.__reading = None  # will cache value of self.__reader.read()

        super().__init__(metadata=metadata, storage_options=storage_options)

    def __read(self):
        """
        Call read on our Reader and cache the result.
        """
        if self.__reading is None:
            self.__reading = self.__reader.read()
        return self.__reading

    def _close(self):
        self.__reader.close()

    # Everything from here implements the DataSource API in terms of __read().

    def _get_schema(self):
        reading = self.__read()
        return Schema(
            datashape=None,
            dtype=str(reading.dtype),  # str so it is serializable
            shape=reading.shape,
            npartitions=reading.npartitions,
            chunks=reading.chunks,
            extra_metadata={})

    def _get_partition(self, i):
        if isinstance(i, list):
            i = tuple(i)
        return self.__read().blocks[i].compute()

    def read_partition(self, i):
        self._get_schema()
        return self._get_partition(i)

    def to_dask(self):
        return self.__read()

    def read(self):
        return self.__read().compute()

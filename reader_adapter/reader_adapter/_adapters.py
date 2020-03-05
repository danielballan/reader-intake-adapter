"""
Adapters for common 'container' types.
"""
from ._base import Adapter
from ._vendored import Schema


class DaskArrayAdapter(Adapter):
    """
    Wrap a Reader that returns a dask.array.core.Array in the DataSource API.

    We expect a subclass to define a _reader_class attribute.
    """
    container = 'ndarray'  # name matching intake's container registry
    _EXPECTED_CONTAINER = 'dask.array.core.Array'  # fully-qualified class name

    # Implement the DataSource API in terms of Adapter._adpater_read().

    def _get_schema(self):
        reading = self._adapter_read()
        return Schema(
            datashape=None,
            dtype=str(reading.dtype),  # str so it is serializable
            shape=reading.shape,
            npartitions=reading.npartitions,
            chunks=reading.chunks,
            extra_metadata={})

    def _get_partition(self, i):
        # This handles that fact that tuples serialize/deserialize as lists in
        # msgpack.
        if isinstance(i, list):
            i = tuple(i)
        return self._adapter_read().blocks[i].compute()

    def read_partition(self, i):
        # Base class assumes the i is an integer, but i is a tuple in our case,
        # so we have to override the base class here.
        self._get_schema()
        return self._get_partition(i)

    def to_dask(self):
        return self._adapter_read()

    def read(self):
        return self._adapter_read().compute()

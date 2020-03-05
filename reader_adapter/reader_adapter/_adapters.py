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


class DaskDataFrameAdapter(Adapter):
    """
    Wrap a Reader that returns a dask.dataframe.core.DataFrame in the DataSource API.

    We expect a subclass to define a _reader_class attribute.
    """
    container = 'dataframe'  # name matching intake's container registry
    _EXPECTED_CONTAINER = 'dask.dataframe.core.DataFrame'  # fully-qualified class name

    # Implement the DataSource API in terms of Adapter._adpater_read().

    def _get_schema(self):
        reading = self._adapter_read()
        # HACK: Is there a public accessor that can get this or do we have to
        # rely on _meta?
        dtypes = {name: str(dtype) for name, dtype in
                  reading._meta.dtypes.to_dict().items()}
        return Schema(
            datashape=None,
            dtype=dtypes,
            shape=reading.shape,
            npartitions=reading.npartitions,
            extra_metadata={})

    def _get_partition(self, i):
        return self._adapter_read().get_partition(i).compute()

    def read_partition(self, i):
        # The base class implementation works fine in this case.
        return super().read_partition(i)

    def to_dask(self):
        return self._adapter_read()

    def read(self):
        return self._adapter_read().compute()

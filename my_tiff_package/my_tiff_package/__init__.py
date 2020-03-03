import dask.array
import tifffile


class TIFFReader:
    """
    Accepts file, filepath, or filepath glob.
    """
    name = 'sigh'
    container = 'array'
    has_been_persisted = False

    def __init__(self, file, metadata):
        if isinstance(file, str):
            # file is a filepath or filepath glob
            import os
            if os.path.isfile(file):
                self._tiff_files = [tifffile.TiffFile(file)]
            else:
                import glob
                self._tiff_files = [tifffile.TiffFile(file_)
                                    for file_ in glob.glob(file)]
            self.metadata = metadata
            # TODO Pick off metadata of interest from self._tiff_files and
            # merge it with this.
        else:
            # file is a file buffer
            self._tiff_files = [tifffile.TiffFile(file)]
        self._file = file  # used in __repr__
        self._closed = False
        self._dask_array = None  # cached here first time read() is called

    def __repr__(self):
        return f"TiffReader({self._file!r})"

    def read(self, delayed=False):
        if self._closed:
            raise Closed(f"{self} is closed and can no longer be read.")
        if self._dask_array is None:
            stack = []
            for tf in self._tiff_files:
                assert len(tf.series) == 1  # should be True by construction
                series = tf.series[0]
                dtype = series.dtype
                for page in series.pages:
                    stack.append(dask.array.from_delayed(
                        dask.delayed(page.asarray)(),
                        shape=page.shape, dtype=dtype))
            dask_array = dask.array.stack(stack)
            # Stash result for next time...
            self._dask_array = dask_array
        if delayed:
            return self._dask_array
        else:
            return self._dask_array.compute()

    def close(self):
        self._closed = True
        self._dask_array = None
        for tf in self._tiff_files:
            tf.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self.close()


class Closed(Exception):
    ...

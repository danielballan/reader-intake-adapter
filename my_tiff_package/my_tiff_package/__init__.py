import dask.array
import tifffile


class TIFFReader:
    """
    Accepts file, filepath, or filepath glob.
    """
    container = 'dask.array.Array'

    def __init__(self, file):
        if isinstance(file, str):
            # file is a filepath or filepath glob
            import os
            if os.path.isfile(file):
                self._tiff_files = [tifffile.TiffFile(file)]
            else:
                import glob
                self._tiff_files = [tifffile.TiffFile(file_)
                                    for file_ in glob.glob(file)]
        else:
            # file is a file buffer
            self._tiff_files = [tifffile.TiffFile(file)]
        self._file = file  # only used in __repr__
        self._closed = False

    def __repr__(self):
        return f"{self.__class__.__name__}({self._file!r})"

    def read(self):
        if self._closed:
            raise Closed(f"{self} is closed and can no longer be read.")
        stack = []
        for tf in self._tiff_files:
            assert len(tf.series) == 1  # should be True by construction
            series = tf.series[0]
            dtype = series.dtype
            for page in series.pages:
                stack.append(dask.array.from_delayed(
                    dask.delayed(page.asarray)(),
                    shape=page.shape, dtype=dtype))
        return dask.array.stack(stack)

    def close(self):
        self._closed = True
        for tf in self._tiff_files:
            tf.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self.close()


class Closed(Exception):
    ...


# intake compatibility
from reader_adapter import adapt  # noqa
TIFFDataSource = adapt(TIFFReader, 'TIFFDataSource')

This proof of concept aims to demonstrate that if a library implements this API:

```py
class MyReader:
    container = '...'  # fully-qualified class name of type(self.read())

    def read(self):
        """
        Return any type that reader_adapter knows about.

        This currently includes dask.array.core.Array and
        dask.dataframe.core.DataFrame, but could grow to include numpy.ndarray,
        pandas.DataFrame, and xarray types.
        """
        ...

    def close(self):
        ...
```

we can automatically build a fully-compliant intake `DataSource` around it. (The
cache and persist functionality are not filled in to this proof of concept yet,
but I believe there is a path to add that as well, and we should do so.)

Currently, libraries that want to implement a `DataSource` must do one of the
following:

* Accept an intake dependency, subclass `DataSource`, and define `_get_schema`,
  `_get_partition`, `read`, `to_dask`, and (sometimes) `read_partition`.
* Avoid the intake dependency and implement the `DataSource` API from scratch.
  (I am not aware of any packages that do this, but it's conceivable.)

This proposal has two aims:

* Make it easier to write and maintain a plugin for a given format by moving
  logic that is generic for a given container type out of the format-specific
  plugin.
* Provide a path for existing libraries like PIMS, imageio, tifffile,
  area-detector-handlers, and others to potentially add intake-compatible
  plugins and entrypoints without taking on an intake dependency.

As is, this shows that *without any changes to intake* any package could define
a `Reader` as above, import `reader_adapter` (a zero-dependency project that
could theorhetically go up on PyPI), and use

```py
MyDataSource = reader_adapter.adapt(MyReader, 'MyDataSource')
```

to auto-build a complete `DataSource` out of their Reader and declare that as an
`'intake.drivers'` entrypoints. This is what `my_tiff_package` and
`my_fwf_package` do. The example scripts `tiff_example.py` and `fwf_example.py`
show that these work.

We could make this even more convenient for package authors by making one change
to intake itself.  If intake were to incorporate this `reader_adapter` code and
define a third entrypoint `intake.readers` (alongside the more generic
`intake.drivers` and the recently-added `intake.catalogs`) then intake could do
the reader-to-DataSource conversion itself transparently. The package author
would only need to define the simple `Reader` API and declare it as an
`'intake.readers'` entrypoint.

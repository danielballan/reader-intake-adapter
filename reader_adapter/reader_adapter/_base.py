from ._vendored import DictSerialiseMixin


class ReaderAdapter(DictSerialiseMixin):
    """
    This builds the DataSource API around a Reader class.
    """
    name = None
    version = None
    container = None
    partition_access = False
    datashape = None
    description = None

    # @property
    # def cache_dirs(self):
    #     return [c._cache_dir for c in self.cache]

    # def set_cache_dir(self, cache_dir):
    #     for c in self.cache:
    #         c._cache_dir = make_path_posix(cache_dir)

    def __init__(self, storage_options=None, metadata=None):
        self.metadata = {}
        # if isinstance(self.metadata, dict):
        #     storage_options = self._captured_init_kwargs.get('storage_options',
        #                                                      {})
        #     self.cache = make_caches(self.name, self.metadata.get('cache'),
        #                              catdir=self.metadata.get('catalog_dir',
        #                                                       None),
        #                              storage_options=storage_options)
        self.datashape = None
        self.dtype = None
        self.shape = None
        self.npartitions = 0
        self._schema = None
        self.catalog_object = None
        self.on_server = False
        self.cat = None  # the cat from which this source was made

    # def _get_cache(self, urlpath):
    #     if len(self.cache) == 0:
    #         return [urlpath]
    #     return [c.load(urlpath) for c in self.cache]

    def _get_schema(self):
        """Subclasses should return an instance of base.Schema"""
        raise Exception('Subclass should implement _get_schema()')

    def _get_partition(self, i):
        """Subclasses should return a container object for this partition

        This function will never be called with an out-of-range value for i.
        """
        raise Exception('Subclass should implement _get_partition()')

    def _close(self):
        """Subclasses should close all open resources"""
        raise Exception('Subclass should implement _close()')

    # These methods are implemented from the above two methods and do not need
    # to be overridden unless custom behavior is required

    def _load_metadata(self):
        """load metadata only if needed"""
        if self._schema is None:
            self._schema = self._get_schema()
            self.datashape = self._schema.datashape
            self.dtype = self._schema.dtype
            self.shape = self._schema.shape
            self.npartitions = self._schema.npartitions
            self.metadata.update(self._schema.extra_metadata)

    def _yaml(self, with_plugin=False):
        import inspect
        kwargs = self._captured_init_kwargs.copy()
        meta = kwargs.pop('metadata', self.metadata) or {}
        kwargs.update(dict(zip(inspect.signature(self.__init__).parameters,
                               self._captured_init_args)))
        data = {'sources': {self.name: {
            'driver': self.classname,
            'description': self.description or "",
            'metadata': meta,
            'args': kwargs
        }}}
        if with_plugin:
            data['plugins'] = {
                'source': [{'module': self.__module__}]}
        return data

    def yaml(self, with_plugin=False):
        """Return YAML representation of this data-source

        The output may be roughly appropriate for inclusion in a YAML
        catalog. This is a best-effort implementation

        Parameters
        ----------
        with_plugin: bool
            If True, create a "plugins" section, for cases where this source
            is created with a plugin not expected to be in the global Intake
            registry.
        """
        from yaml import dump
        data = self._yaml(with_plugin=with_plugin)
        return dump(data, default_flow_style=False)

    @property
    def plots(self):
        """List custom associated quick-plots """
        return list(self.metadata.get('plots', {}))

    def discover(self):
        """Open resource and populate the source attributes."""
        self._load_metadata()

        return dict(datashape=self.datashape,
                    dtype=self.dtype,
                    shape=self.shape,
                    npartitions=self.npartitions,
                    metadata=self.metadata)

    def read(self):
        """Load entire dataset into a container and return it"""
        if not self.partition_access or self.npartitions == 1:
            return self._get_partition(0)
        else:
            raise NotImplementedError

    def read_chunked(self):
        """Return iterator over container fragments of data source"""
        self._load_metadata()
        for i in range(self.npartitions):
            yield self._get_partition(i)

    def read_partition(self, i):
        """Return a part of the data corresponding to i-th partition.

        By default, assumes i should be an integer between zero and npartitions;
        override for more complex indexing schemes.
        """
        self._load_metadata()
        if i < 0 or i >= self.npartitions:
            raise IndexError('%d is out of range' % i)

        return self._get_partition(i)

    def to_dask(self):
        """Return a dask container for this data source"""
        raise NotImplementedError

    def to_spark(self):
        """Provide an equivalent data object in Apache Spark

        The mapping of python-oriented data containers to Spark ones will be
        imperfect, and only a small number of drivers are expected to be able
        to produce Spark objects. The standard arguments may b translated,
        unsupported or ignored, depending on the specific driver.

        This method requires the package intake-spark
        """
        raise NotImplementedError

    def close(self):
        """Close open resources corresponding to this data source."""
        self._close()

    # Boilerplate to make this object also act like a context manager
    def __enter__(self):
        self._load_metadata()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def plot(self):
        """
        Returns a hvPlot object to provide a high-level plotting API.

        To display in a notebook, be sure to run ``intake.output_notebook()``
        first.
        """
        try:
            from hvplot import hvPlot
        except ImportError:
            raise ImportError("The intake plotting API requires hvplot."
                              "hvplot may be installed with:\n\n"
                              "`conda install -c pyviz hvplot` or "
                              "`pip install hvplot`.")
        metadata = self.metadata.get('plot', {})
        fields = self.metadata.get('fields', {})
        for attrs in fields.values():
            if 'range' in attrs:
                attrs['range'] = tuple(attrs['range'])
        metadata['fields'] = fields
        plots = self.metadata.get('plots', {})
        return hvPlot(self, custom_plots=plots, **metadata)

    @property
    def hvplot(self):
        """
        Returns a hvPlot object to provide a high-level plotting API.
        """
        return self.plot

    # def persist(self, ttl=None, **kwargs):
    #     """Save data from this source to local persistent storage

    #     Parameters
    #     ----------
    #     ttl: numeric, optional
    #         Time to live in seconds. If provided, the original source will
    #         be accessed and a new persisted version written transparently
    #         when more than ``ttl`` seconds have passed since the old persisted
    #         version was written.
    #     kargs: passed to the _persist method on the base container.
    #     """
    #     from ..container import container_map
    #     from ..container.persist import PersistStore
    #     import time
    #     if 'original_tok' in self.metadata:
    #         raise ValueError('Cannot persist a source taken from the persist '
    #                          'store')
    #     if ttl is not None and not isinstance(ttl, (int, float)):
    #         raise ValueError('Cannot persist using a time to live that is '
    #                          f'non-numeric. User-provided ttl was {ttl}')
    #     store = PersistStore()
    #     out = self._export(store.getdir(self), **kwargs)
    #     out.metadata.update({
    #         'ttl': ttl,
    #         'cat': {} if self.cat is None else self.cat.__getstate__()
    #     })
    #     out.name = self.name
    #     store.add(self._tok, out)
    #     return out

    def export(self, path, **kwargs):
        """Save this data for sharing with other people

        Creates a copy of the data in a format appropriate for its container,
        in the location specified (which can be remote, e.g., s3).

        Returns the resultant source object, so that you can, for instance,
        add it to a catalog (``catalog.add(source)``) or get its YAML
        representation (``.yaml()``).
        """
        return self._export(path, **kwargs)

    def _export(self, path, **kwargs):
        from ..container import container_map
        import time
        method = container_map[self.container]._persist
        # may need to create path - access file-system method
        out = method(self, path=path, **kwargs)
        out.description = self.description
        metadata = {'timestamp': time.time(),
                    'original_metadata': self.metadata,
                    'original_source': self.__getstate__(),
                    'original_name': self.name,
                    'original_tok': self._tok,
                    'persist_kwargs': kwargs}
        out.metadata = metadata
        out.name = self.name
        return out

    def get_persisted(self):
        from ..container.persist import store
        return store[self._tok]()

    @staticmethod
    def _persist(source, path, **kwargs):
        """To be implemented by 'container' sources for locally persisting"""
        raise NotImplementedError

    @property
    def has_been_persisted(self):
        return False

    # @property
    # def has_been_persisted(self):
    #     from ..container.persist import store
    #     return self._tok in store

    @property
    def is_persisted(self):
        from ..container.persist import store
        return self.metadata.get('original_tok', None) in store

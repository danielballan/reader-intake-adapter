"""
These objects are vendored from intake.
"""

import collections


class Schema(dict):
    """
    Vendored from intake.source. Original docstring:

    Holds details of data description for any type of data-source

    This should always be pickleable, so that it can be sent from a server
    to a client, and contain all information needed to recreate a RemoteSource
    on the client.
    """

    def __init__(self, **kwargs):
        """
        Parameters
        ----------
        kwargs: typically include datashape, dtype, shape
        """
        super(Schema, self).__init__(**kwargs)
        for field in ['datashape', 'dtype', 'extra_metadata', 'shape']:
            # maybe a default-dict
            if field not in self:
                self[field] = None

    def __repr__(self):
        return ("<Schema instance>\n"
                "dtype: {}\n"
                "shape: {}\n"
                "metadata: {}"
                "".format(self.dtype, self.shape, self.extra_metadata))

    def __getattr__(self, item):
        return self[item]


class DictSerialiseMixin:
    "Vendored from intake.utils. (Original has no class docstring.)"
    def __new__(cls, *args, **kwargs):
        """Capture creation args when instantiating"""
        from dask.base import tokenize
        o = object.__new__(cls)
        o._captured_init_args = args
        o._captured_init_kwargs = kwargs
        o.__dict__['_tok'] = tokenize(o.__getstate__())
        return o

    @property
    def classname(self):
        return classname(self)

    def __dask_tokenize__(self):
        return hash(self)

    def __getstate__(self):
        args = [arg.__getstate__() if isinstance(arg, DictSerialiseMixin)
                else arg
                for arg in self._captured_init_args]
        # We employ OrderedDict in several places. The motivation
        # is to speed up dask tokenization. When dask tokenizes a plain dict,
        # it sorts the keys, and it turns out that this sort operation
        # dominates the call time, even for very small dicts. Using an
        # OrderedDict steers dask toward a different and faster tokenization.
        kwargs = collections.OrderedDict(
            {k: arg.__getstate__()
             if isinstance(arg, DictSerialiseMixin) else arg
             for k, arg in self._captured_init_kwargs.items()})
        return collections.OrderedDict(cls=self.classname,
                                       args=args,
                                       kwargs=kwargs)

    def __setstate__(self, state):
        # reconstitute instances here
        self._captured_init_kwargs = state['kwargs']
        self._captured_init_args = state['args']
        state.pop('cls', None)
        self.__init__(*state['args'], **state['kwargs'])

    def __hash__(self):
        return int(self._tok, 16)

    def __eq__(self, other):
        return hash(self) == hash(other)


def classname(ob):
    """Get the object's class's name as package.module.Class"""
    import inspect
    if inspect.isclass(ob):
        return '.'.join([ob.__module__, ob.__name__])
    else:
        return '.'.join([ob.__class__.__module__, ob.__class__.__name__])

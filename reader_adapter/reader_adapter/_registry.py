from ._adapters import DaskArrayAdapter


def register(container, adapter):
    """
    Register an adapter for a new container.

    Parameters
    ----------
    container: string
        Fully-qualified class name, such as 'dark.array.Array'
    adapter: callabel
        Should accept a Reader class return a class that is API compatible with
        intake DataSource
    """
    register._registry[container] = adapter


def adapt(reader_class, class_name):
    # First check that reader_class implements all the expected API.
    try:
        reader_class.read
        reader_class.close
        container = reader_class.container
    except AttributeError as err:
        raise ValueError(
            "reader_class must implement read, close, and container") from err
    try:
        base = register._registry[reader_class.container]
    except KeyError as err:
        raise KeyError(
            f"No adapter is registered for the container type {container}") from err
    return type(class_name, (base,), {'_reader_class': reader_class})


register._registry = {}  # maps container (string) to adapter_class


# Register built-in adapters.
register('dask.array.Array', DaskArrayAdapter)

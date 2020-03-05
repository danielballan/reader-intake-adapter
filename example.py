import my_tiff_package


# The Reader is usable on its own.
reader = my_tiff_package.TIFFReader('example_data/coffee.tif')
reader.read()
reader.close()

# The wrapped-in-DataSource version implements most of the API (except cache
# and persist).
data_source = my_tiff_package.TIFFDataSource('example_data/coffee.tif')
data_source.discover()
data_source.read()
list(data_source.read_chunked())
data_source.read_partition(0)
data_source.to_dask()
data_source.close()

# It works in a catalog.
import intake
catalog = intake.open_catalog('catalog.yml')
catalog.coffee.read()

# And in a remote catalog:
# Run intake-server catalog.yml
# and then:
# catalog = intake.open_catalog('intake://localhost:5000')
# catalog.coffee.read()

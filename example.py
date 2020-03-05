import my_tiff_package


reader = my_tiff_package.TIFFReader('example_data/coffee.tif')
reader.read()
reader.close()

data_source = my_tiff_package.TIFFDataSource('example_data/coffee.tif')
data_source.read()
data_source.to_dask()
data_source.discover()
data_source.close()

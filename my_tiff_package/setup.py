from setuptools import setup


setup(
    name='my_tiff_package',
    packages=['my_tiff_package'],
    entry_points={
        'intake.drivers':
            ['tiff = my_tiff_package:TIFFReader']},
    install_requires=['dask[array]', 'tifffile'],
)

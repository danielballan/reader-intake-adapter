from setuptools import setup


setup(
    name='my_fwf_package',
    packages=['my_fwf_package'],
    entry_points={
        'intake.drivers':
            ['fwf = my_fwf_package:FWFDataSource']},
    install_requires=['dask[dataframe]'],
)

from setuptools import setup, find_packages

setup(
    name='datablox_framework',
    version='1.0',
    author='MPI',
    author_email='',
    url='',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points = {
        'console_scripts': [
          'datablox-care-taker = datablox_framework.care_taker:call_from_console_script',
          'datablox-loader = datablox_framework.loader:call_from_console_script'
            ]},
    install_requires=[],
    license='Apache V2.0',
    description='A dataflow language and runtime',
    long_description="description"
    )

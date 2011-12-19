from setuptools import setup, find_packages

setup(
    name='datablox_engage_adapter',
    version='1.0',
    author='genForma Corporation',
    author_email='code@genforma.com',
    url='',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points = {
        'console_scripts': [
            ]},
    install_requires=[],
    license='Apache V2.0',
    description='Adapter connecting datablox to the Engage environment that it is installed into',
    long_description="description"
    )

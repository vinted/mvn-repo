
from setuptools import setup, find_packages

setup(
    name='vintools',
    version='0.1',
    packages=find_packages(),
    license='MIT',
    description='Vinted utilities for notebooks',
    long_description=open('README.txt').read(),
    install_requires=['findspark', 'pyspark', 'pivottablejs'],
    url='https://github.com/vinted/chef',
    author='David Kazlauskas',
    author_email='deividas.kazlauskas@vinted.com'
)

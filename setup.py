#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: setup.py
.. moduleauthor:: Pat Daburu <pat@daburu.net>

This file is used to create the package we'll publish to PyPI.
"""

import os
import luijo
from setuptools import setup, find_packages, Command  # Always prefer setuptools over distutils
from codecs import open  # To use a consistent encoding
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README'), encoding='utf-8') as f:
    long_description = f.read()

# Get the base version from the library.
version = luijo.__version__

# If the environment has a build number set...
if os.getenv('buildnum') is not None:
    # ...append it to the version.
    version = "{version}.{buildnum}".format(version=version, buildnum=os.getenv('buildnum'))

setup(
  name='luijo',
  description="Extensions for Spotify Luigi",
  long_description=long_description,
  packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
  version=version,
  include_package_data=True,
  install_requires=[
    'luigi', 'psycopg2', 'SQLAlchemy'
  ],
  python_requires=">=3.6.1",
  license='MIT',
  author='Pat Daburu',
  author_email='patb@daburu.net',
  url='https://github.com/patdaburu/luijo.git',  # Use the URL to the github repo.
  download_url='https://github.com/patdaburu/luiji/archive/{version}.tar.gz'.format(version=version),
  keywords=[

  ],  # arbitrary keywords
  # See https://PyPI.python.org/PyPI?%3Aaction=list_classifiers
  classifiers=[
    # How mature is this project? Common values are
    #   3 - Alpha
    #   4 - Beta
    #   5 - Production/Stable
    'Development Status :: 3 - Alpha',

    # Indicate who your project is intended for
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Libraries',

    # Pick your license as you wish (should match "license" above)
    'License :: OSI Approved :: MIT License',

    # Specify the Python versions you support here. In particular, ensure
    # that you indicate whether you support Python 2, Python 3 or both.
    'Programming Language :: Python :: 3.6',
  ],
)

#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created by pat on 4/21/18
"""
.. currentmodule:: luijo.config
.. moduleauthor:: Pat Daburu <pat@daburu.net>

Say something descriptive about the 'config' module.
"""
import luigi
import logging
import os
from typing import cast


class FileSystem(luigi.Config):
    """
    These are configuration settings to which we can refer when working with
    the local file system.

    :cvar target_home_dir: the default home directory for local targets
    """
    target_home_dir: luigi.Parameter = luigi.Parameter(default=os.getcwd())


# Attempt to create the local home directory for targets.
try:
    os.makedirs(cast(str, FileSystem().target_home_dir), exist_ok=True)
except OSError:
    logging.exception(f'Target home directory creation failed '
                      f'({FileSystem().target_home_dir})')

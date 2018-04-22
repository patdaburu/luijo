#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created by pat on 4/21/18
"""
.. currentmodule:: luijo.config
.. moduleauthor:: Pat Daburu <pat@daburu.net>

Say something descriptive about the 'config' module.
"""
import logging
import os
from typing import cast
from typing import Iterable
import luigi


def find_configs() -> Iterable[str]:
    """
    This function attempts to find the current Luigi
    :return:
    """
    # Create a list to hold the config paths as we find them.
    configs = []
    if 'LUIGI_CONFIG_PATH' in os.environ:
        configs.append(os.environ['LUIGI_CONFIG_PATH'])
    else:
        for candidate in [
                os.path.join(os.getcwd(), 'luiji.cfg'),
                os.path.join(os.getcwd(), 'client.cfg'),
                '/etc/luigi/client.cfg'
        ]:
            if os.path.exists(candidate) and os.path.isfile(candidate):
                configs.append(candidate)
    return configs


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

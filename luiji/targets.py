#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: luiji.tasks
.. moduleauthor:: Pat Daburu <pat@daburu.net>

Luigi targets, plus just a little more.
"""

from abc import ABCMeta, abstractmethod
import luigi.target
import logging


class Target(luigi.Target):
    """
    This is a common parent class that provides some additional conveniences for Luigi targets.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def exists(self):
        """
        Does the target exist?
        :return:
        """
        raise NotImplementedError('The subclass must implement this method.')

    @classmethod
    def get_logger(cls) -> logging.Logger:
        """
        Get this target's logger.
        :return: the target's logger
        """
        return logging.getLogger('{module}.{cls}'.format(module=cls.__module__, cls=cls.__name__))
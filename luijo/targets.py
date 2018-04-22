#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: luijo.tasks
.. moduleauthor:: Pat Daburu <pat@daburu.net>

Luigi targets, plus just a little more.
"""

from abc import ABCMeta
from typing import Any
import jsonpickle
import luigi.target


def touch(local_target: luigi.LocalTarget):
    """
    You can use this function to create a local target file in cases where you
    just need it to signify that the task completed.

    :param local_target: the local target
    """
    with local_target.open('w') as target:
        target.write('')


class LocalObjectTarget(luigi.LocalTarget):
    """
    This is a local target you can use to serialize a Python object to a file.
    """
    __metaclass__ = ABCMeta

    # TODO: Add support for binary serialization.  # pylint: disable=fixme

    def deserialize(self) -> Any:
        """
        Retrieve the target object.
        :return: the target object
        """
        with self.open('r') as fin:
            frozen = fin.read()
            thawed = jsonpickle.decode(frozen)
            return thawed

    def serialize(self, obj: Any):
        """
        Serialize an object to the local target.

        :param obj: the object you want to serialize
        """
        # Encode the object.
        frozen = jsonpickle.encode(obj)
        # Open the output file for writing.
        with self.open('w') as fout:
            # Write the encoded object.
            fout.write(frozen)

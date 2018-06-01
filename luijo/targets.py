#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: luijo.tasks
.. moduleauthor:: Pat Daburu <pat@daburu.net>

Luigi targets, plus just a little more.
"""

from abc import ABCMeta
from enum import Enum
import pickle
import os
import jsonpickle
import luigi.target


class Serialization(Enum):
    """
    These are the supported serialization methods.
    """
    JSON = 'json'
    BINARY = 'binary'


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

    def deserialize(self):
        """
        Retrieve the target object.

        :return: the target object
        """
        # We'll try to use jsonpickle first...
        try:
            with self.open('r') as fin:
                frozen = fin.read()
                thawed = jsonpickle.decode(frozen)
                return thawed
        except UnicodeDecodeError:
            # If the attempt to unpickle using JSON fails, we assume we have a
            # binary file.
            with open(self.path, 'rb') as fin:
                unpickled = pickle.load(fin)
                return unpickled

    def serialize(self,
                  obj,
                  format_: Serialization = Serialization.BINARY):
        """
        Serialize an object to the local target.

        :param obj: the object you want to serialize
        :param format_: the serialization format
        """
        if format_ == Serialization.JSON:
            # Encode the object.
            frozen = jsonpickle.encode(obj)
            # Open the output file for writing.
            with self.open('w') as fout:
                # Write the encoded object.
                fout.write(frozen)
        elif format_ == Serialization.BINARY:
            # Pickle the object to the file.
            os.makedirs(
                os.path.dirname(
                    os.path.abspath(
                        os.path.expanduser(
                            self.path
                        )
                    )
                ), exist_ok=True)
            with open(self.path, 'wb') as fout:
                pickle.dump(obj, fout, pickle.HIGHEST_PROTOCOL)
        else:  # pragma: no cover
            raise NotImplementedError(
                f'Unsupported serialization: { format_.name }')

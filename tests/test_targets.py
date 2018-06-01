#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import unittest
import random
import string
from luijo.targets import LocalObjectTarget, Serialization


class TestObject1(object):
    __test__ = False

    def __init__(self, name):
        self.name = name


def get_temp_filename(length: int):
    """
    Generate a random temp file path for testing.

    :param length: the length of the file name
    :return: the file name
    """
    rnd = ''.join(
        random.SystemRandom().choice(string.ascii_uppercase + string.digits)
        for _ in range(length)
    )
    return os.path.join('/tmp', __name__, rnd)


class TestLocalObjectTargetSuite(unittest.TestCase):

    def test_init_serializeDeserializeJson_objectsEqual(self):
        """
        Arrange: Create a :py:class:`LocalObjectTarget`.
        Act: Serialize (JSON) and deserialize an object.
        Assert: Deserialized object matches serialized object.
        """
        # Arrange.
        path = get_temp_filename(9)
        target = LocalObjectTarget(path)
        obj = TestObject1(name='testing')
        try:
            # Act.
            target.serialize(obj, format_=Serialization.JSON)
            thawed = target.deserialize()
            # Assert.
            self.assertIsInstance(thawed, TestObject1)
            self.assertEqual('testing', obj.name)
        finally:
            if os.path.exists(path):
                os.remove(path)

    def test_init_serializeDeserializeBinary_objectsEqual(self):
        """
        Arrange: Create a :py:class:`LocalObjectTarget`.
        Act: Serialize (JSON) and deserialize an object.
        Assert: Deserialized object matches serialized object.
        """
        # Arrange.
        path = get_temp_filename(9)
        target = LocalObjectTarget(path)
        obj = TestObject1(name='testing')
        try:
            # Act.
            target.serialize(obj, format_=Serialization.BINARY)
            thawed = target.deserialize()
            # Assert.
            self.assertIsInstance(thawed, TestObject1)
            self.assertEqual('testing', obj.name)
        finally:
            if os.path.exists(path):
                os.remove(path)

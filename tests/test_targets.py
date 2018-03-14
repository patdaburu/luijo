#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest
import unittest
import random
import string
from luijo.parameters import is_empty_parameter, ClassParameter
from luijo.targets import LocalObjectTarget #Target


# class TestTarget(Target):
#     def exists(self):
#         return True
#
#
# class TestTargetNoOverride(Target):
#     def exists(self):
#         return super().exists()
#
#
# class TestTargetSuite(unittest.TestCase):
#
#     def test_getLogger_returnsLogger(self):
#         target = TestTarget()
#         self.assertIsNotNone(target.get_logger())
#         self.assertTrue(target.exists())
#
#     def test_noOverride_existsRaisesNotImplementedError(self):
#         target = TestTargetNoOverride()
#         with pytest.raises(NotImplementedError):
#             target.exists()

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
    rnd = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(length))
    return os.path.join('/tmp', __name__, rnd)



class TestLocalObjectTargetSuite(unittest.TestCase):

    def test_serialize(self):
        path = get_temp_filename(9)
        target = LocalObjectTarget(path)
        obj = TestObject1(name='testing')
        try:
            target.serialize(obj)
            thawed = target.deserialize()
            self.assertIsInstance(obj, TestObject1)
            self.assertEqual('testing', obj.name)
        finally:
            if os.path.exists(path):
                os.remove(path)



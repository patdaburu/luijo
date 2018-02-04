#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import unittest
from luijo.parameters import is_empty_parameter, ClassParameter
from luijo.targets import Target


class TestTarget(Target):
    def exists(self):
        return True


class TestTargetNoOverride(Target):
    def exists(self):
        return super().exists()


class TestTargetSuite(unittest.TestCase):

    def test_getLogger_returnsLogger(self):
        target = TestTarget()
        self.assertIsNotNone(target.get_logger())
        self.assertTrue(target.exists())

    def test_noOverride_existsRaisesNotImplementedError(self):
        target = TestTargetNoOverride()
        with pytest.raises(NotImplementedError):
            target.exists()


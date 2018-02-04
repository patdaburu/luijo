#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import unittest
from luijo.parameters import is_empty_parameter, ClassParameter
from luijo.tasks import Task


class TestParametersSuite(unittest.TestCase):

    def test_noneIsEmptyParameter_true(self):
        self.assertTrue(is_empty_parameter(None))

    def test_emptyStringIsEmptyParameter_true(self):
        self.assertTrue(is_empty_parameter(''))

    def test_nonEmptyStringIsEmptyParameter_false(self):
        self.assertFalse(is_empty_parameter('alpha'))


class TestClassParameterSuite(unittest.TestCase):

    def test_parse_success(self):
        self.assertIs(Task, ClassParameter.parse('luijo.tasks.Task'))
        self.assertIs(Task, ClassParameter.parse(Task))

    def test_serialize_success(self):
        self.assertEqual('luijo.tasks.Task', ClassParameter.serialize('luijo.tasks.Task'))
        self.assertEqual('luijo.tasks.Task', ClassParameter.serialize(Task))

    def test_serialize_raisesValueError(self):
        with pytest.raises(ValueError):
            s = ClassParameter.serialize(1)  # The argument needs to be a type.

    def test_parse_raisesValueError(self):
        with pytest.raises(ValueError):
            c = ClassParameter.parse(1)  # We can't parse a number
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import unittest
from luijo.parameters import is_empty_parameter, ClassParameter


class TestParametersSuite(unittest.TestCase):

    def test_noneIsEmptyParameter_true(self):
        self.assertTrue(is_empty_parameter(None))

    def test_emptyStringIsEmptyParameter_true(self):
        self.assertTrue(is_empty_parameter(''))

    def test_nonEmptyStringIsEmptyParameter_false(self):
        self.assertFalse(is_empty_parameter('alpha'))


class TestClassParameterSuite(unittest.TestCase):

    def test_parse_success(self):
        self.assertIs(
            ClassParameter,
            ClassParameter().parse('luijo.parameters.ClassParameter'))
        self.assertIs(ClassParameter, ClassParameter().parse(ClassParameter))

    def test_serialize_success(self):
        self.assertEqual('luijo.parameters.ClassParameter',
                         ClassParameter().serialize(
                             'luijo.parameters.ClassParameter'))
        self.assertEqual('luijo.parameters.ClassParameter',
                         ClassParameter().serialize(ClassParameter))

    def test_serialize_raisesValueError(self):
        with pytest.raises(ValueError):
            # The argument needs to be a type.
            _ = ClassParameter().serialize(1)

    def test_parse_raisesValueError(self):
        with pytest.raises(ValueError):
            _ = ClassParameter().parse(1)  # We can't parse a number

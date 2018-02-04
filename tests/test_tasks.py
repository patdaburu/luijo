#!/usr/bin/env python
# -*- coding: utf-8 -*-

import luigi
import pytest
import unittest
from luijo.tasks import Task, TaskContact, TaskDescriptor, taskinfo


@taskinfo(
    friendly_name='Test Dependency Task',
    synopsis='Test this task.',
    description="""Test the task description""",
    contact=TaskContact(name='Pat',
                        email='pat@daburu.net',
                        phone='180-555-1212'))
class TestDependencyTask(Task):
    def run(self):
        pass


@taskinfo(
    friendly_name='Test Task',
    synopsis='Test this task.',
    description="""Test the task description""",
    contact=TaskContact(name='Pat',
                        email='pat@daburu.net',
                        phone='180-555-1212'))
class TestTask(Task):

    def requires(self):
        return [TaskDescriptor(TestDependencyTask), TestDependencyTask]

    def run(self):
        pass


@taskinfo(
    friendly_name='No Requirements Task',
    synopsis='Test this task.',
    description="""Test the task description""",
    contact=TaskContact(name='Pat',
                        email='pat@daburu.net',
                        phone='180-555-1212'))
class NoRequirementsTestTask(Task):
    def run(self):
        pass


@taskinfo(
    friendly_name='No Requirements Task',
    synopsis='Test this task.',
    description="""Test the task description""",
    contact=TaskContact(name='Pat',
                        email='pat@daburu.net',
                        phone='180-555-1212'))
class CallParentRunTestTask(Task):
    def run(self):
        super().run()


class TestTaskContactSuite(unittest.TestCase):

    def test_taskContact_init(self):
        task_contact = TaskContact(name='test name', email='test email', phone='test phone')
        self.assertEqual(task_contact.name, 'test name')
        self.assertEqual(task_contact.email, 'test email')
        self.assertEqual(task_contact.phone, 'test phone')


class TestTaskDescriptorSuite(unittest.TestCase):

    def test_taskDescriptor_instanceReturnsInitClass(self):
        descriptor = TaskDescriptor(TestTask, runid='abc123')
        instance = descriptor.instance()
        self.assertIsInstance(instance, TestTask)
        instance.run()  # for coverage

    def test_taskDescriptor_defines(self):
        descriptor = TaskDescriptor(TestTask, runid='abc123')
        self.assertTrue(descriptor.defines('runid'))
        self.assertFalse(descriptor.defines('not_supplied'))

    def test_taskDescriptor_params(self):
        descriptor = TaskDescriptor(TestTask, runid='abc123')
        self.assertTrue('runid' in descriptor.params)
        self.assertFalse('not_supplied' in descriptor.params)

    def test_taskDescriptor_get(self):
        descriptor = TaskDescriptor(TestTask, runid='abc123')
        self.assertEqual('abc123', descriptor.get('runid'))

    def test_taskDescriptor_set(self):
        descriptor = TaskDescriptor(TestTask, runid='abc123')
        self.assertEqual('abc123', descriptor.get('runid'))
        descriptor.set('runid', 'def456')
        self.assertEqual('def456', descriptor.get('runid'))

    def test_taskDescriptor_add(self):
        descriptor = TaskDescriptor(TestTask, runid='abc123')
        self.assertFalse(descriptor.defines('testparam'))
        descriptor.set('testparam', 'xyz999')
        self.assertTrue(descriptor.defines('testparam'))
        self.assertEqual('xyz999', descriptor.get('testparam'))

    def test_taskDescriptor_copy(self):
        descriptor = TaskDescriptor(TestTask, runid='abc123')
        copy = descriptor.copy()
        self.assertIsInstance(copy.instance(), TestTask)
        self.assertTrue('runid' in descriptor.params)
        self.assertFalse('not_supplied' in descriptor.params)
        self.assertEqual('abc123', descriptor.get('runid'))


class TestTasksSuite(unittest.TestCase):

    def test_task_runSucceeds(self):
        task = TestTask(runid='abc123')
        task.run()

    def test_task_output(self):
        task = TestTask(runid='def456')
        target = task.output()
        self.assertIsInstance(target, luigi.LocalTarget)

    def test_requiresOverridden_returnOverride(self):
        task = TestTask(runid='def456')
        requirements = task.requires()
        self.assertEqual(2, len(list(requirements)))

    def test_requiresNotOverridden_returnParent(self):
        task = NoRequirementsTestTask(runid='def456')
        requirements = task.requires()
        self.assertEqual(0, len(list(requirements)))

    def test_task_taskInfoProperties(self):
        task = TestTask(runid='def456')
        self.assertEqual('Test Task', task.friendly_name)
        self.assertEqual('Test this task.', task.synopsis)
        self.assertEqual('Test the task description', task.description)
        self.assertEqual(task.friendly_name, TestTask.get_task_info()['friendly_name'])
        self.assertIsNone(task._get_task_info_attr('for_coverage_only'))

    def test_task_taskContactProperties(self):
        task = TestTask(runid='def456')
        contact = task.contact
        self.assertEqual('Pat', contact.name)
        self.assertEqual('pat@daburu.net', contact.email)
        self.assertEqual('180-555-1212', contact.phone)

    def test_initWithoutRunID_raisesValueError(self):
        with pytest.raises(ValueError):
            task = TestTask()  # No runid!
            task.run()

    def test_callBaseRun_raisesNotImplementedError(self):
        with pytest.raises(NotImplementedError):
            task = CallParentRunTestTask(runid='abc123')
            task.run()

    def test_augmentDescriptor_noneIsNone(self):
        task = TestTask(runid='def456')
        empty_desc: TaskDescriptor = None
        with pytest.raises(ValueError):
            task._augment_descriptor(empty_desc)



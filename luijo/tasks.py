#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: luiji.tasks
.. moduleauthor:: Pat Daburu <pat@daburu.net>

Herein you'll find kindly assistance for working with Luigi tasks.
"""

from abc import ABCMeta, abstractmethod
from collections import namedtuple
import datetime
from functools import partial
import luigi
import logging
from typing import Any, Dict, Iterable, Type


TaskContact = namedtuple('TaskContact', ['name', 'email', 'phone'])  #: Contact information for tasks.


class TaskDescriptor(object):

    """
    A task descriptor contains the information necessary to create a new task instance.
    """
    def __init__(self, cls: Type[luigi.Task], **kwargs):
        """
        :param cls: the Task class
        :param kwargs: the constructor arguments
        """
        self._cls: Type[luigi.Task] = cls  #: the Task class
        self._kwargs: Dict[str, any] = {k: v for (k, v) in kwargs.items()}  #: the constructor arguments

    def defines(self, param) -> bool:
        """
        Does this descriptor define a given parameter?
        :param param: the parameter name
        :return: `True` if the parameter is defined by the descriptor, otherwise `False`
        """
        return param in self._kwargs

    @property
    def params(self) -> Iterable[str]:
        """
        Get the names of all the parameters defined in this descriptor.
        :return: an iteration of the names
        """
        return [param for param in self._kwargs]

    def get(self, param) -> Any or None:
        """
        Get a defined parameter value
        :param param: the parameter name
        :return: the parameter value
        :raises KeyError: if the parameter isn't defined
        """
        return self._kwargs[param]

    def set(self, param, value):
        """
        Set a parameter value.
        :param param: the parameter name
        :param value: the value
        """
        self._kwargs[param] = value

    def copy(self) -> 'TaskDescriptor':
        """
        Make a copy of the descriptor.
        :return: a copy of this descriptor
        """
        return TaskDescriptor(cls=self._cls, **self._kwargs)

    def instance(self):
        """
        Create an instance of the task.
        :return: an instance of the task
        """
        return self._cls(**self._kwargs)


class Task(luigi.Task):
    """
    Extend this class to create your own types of tasks.
    """
    __metaclass__ = ABCMeta

    runid: luigi.Parameter = luigi.Parameter(default='')  #: the ID of the run in which this task is executing

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Replace the defined 'run' method with a partial function calling the protected wrapper method which will
        # in turn call the original method.
        self.run = partial(self._mario_run, self.run)
        # Replace the defined 'requires' method with a partial function calling the protected wrapper method which
        # will in turn call the original method.
        self.requires = partial(self._mario_requires, self.requires)

    def _augment_descriptor(self,
                            descriptor: TaskDescriptor) -> TaskDescriptor:
        """
        Augment a task descriptor to include desirable parameters.
        :param descriptor:
        :return: the augmented descriptor
        """
        # Sanity check...
        if descriptor is None:
            raise ValueError('descriptor cannot be None.')
        # Copy the original descriptor.
        copy = descriptor.copy()
        # Modify it.
        if not copy.defines('runid'):
            copy.set('runid', self.runid)
        # Return it.
        return copy

    def _mario_requires(self, requires):
        """
        This is a wrapper for the task's :py:func:`requires` method.
        :param requires: the original :py:func:`requires` method
        :return: an iteration of Luigi targets
        """
        # Let's look at all the tasks in the original requirements.
        for task in requires():
            # If what we actually have is a type descriptor...
            if isinstance(task, TaskDescriptor):
                # Get the augmented version of the descriptor.
                aug: TaskDescriptor = self._augment_descriptor(task)
                # Create the Task instance and return it.
                yield aug.instance()
            else:
                yield task

    def requires(self) -> Iterable[luigi.Task]:
        """
        Override this method to indicate the tasks that are prerequisite for the successful completion of this task.
        :return: an iteration of :py:class:`luigi.Task` objects that must run before this begins
        """
        return []

    def output(self) -> luigi.LocalTarget:
        """
        Get the output target for the task.
        :return: the output target
        """
        return luigi.LocalTarget("{basename}.{runid}".format(basename=self.get_logger().name,
                                                             runid=self.runid))

    def _mario_run(self, run):
        """
        This is the method that will be called when the task is run.  It will perform additional logic before and
        after executing the original :py:func:`luigi.Task.run` function.
        :param run: the original :py:func:`luigi.Task.run` method
        :return: the result of the original run method
        """
        # If this task doesn't have a proper run ID, we have a problem.
        if self.runid is None or len(str(self.runid).strip()) == 0:
            raise ValueError('The task does not have a run ID.')
        # Make note of the start time (i.e. right now).
        start = datetime.datetime.now()
        # Submit this information to the log.
        self.get_logger().info('The task started at {time} on {date}.'.format(time=start.strftime('%H:%M:%S'),
                                                                              date=start.strftime('%d/%m/%Y')))
        # Run the original logic and get the result.  (We don't actually expect a result, but just in case.)
        result = run()
        # Make note of when the task completed (again... that's right now).
        end = datetime.datetime.now()
        # How long did it take?
        runtime = end - start
        # Log how long the task took.
        self.get_logger().debug(
            'The task completed successfully in {seconds} seconds.'.format(seconds=runtime.total_seconds()))
        # TODO: Consider additional reporting for analysis.
        return result

    @abstractmethod
    def run(self):
        """
        Override this method to define what the task does when it runs.
        """
        raise NotImplementedError('The run method is not implemented. Please define what the task does.')

    @property
    def friendly_name(self) -> str:
        """
        This is a human-friendly name for the task.
        :return: the task's friendly name
        """
        return self._get_task_info_attr('friendly_name')

    @property
    def synopsis(self) -> str:
        """
        This is a brief synopsis of what the task does.
        :return: the task's synopsis
        """
        return self._get_task_info_attr('synopsis')

    @property
    def description(self) -> str:
        """
        This is a nice, long, thorough description of what the task does.
        :return: the task's description
        """
        return self._get_task_info_attr('description')

    @property
    def contact(self) -> TaskContact:
        """
        This is contact information for the task.
        :return: the task's contact information
        """
        return self._get_task_info_attr('contact')

    def _get_task_info_attr(self, attr: str) -> Any:
        """
        Get a task information item.
        :param attr: the information item's key
        :return: the value
        """
        try:
            return self.get_task_info()[attr]
        except KeyError:
            return None

    @classmethod
    def get_task_info(cls) -> Dict[str, Any]:
        """
        Retrieve the task's metadata dictionary.  (You probably don't actually need this.)
        :return: the metadata dictionary
        """
        if not hasattr(cls, '_task_info'):
            cls._task_info = {}
        return cls._task_info

    @classmethod
    def get_logger(cls) -> logging.Logger:
        """
        Get this task's logger.
        :return: the task's logger
        """
        return logging.getLogger('{module}.{cls}'.format(module=cls.__module__, cls=cls.__name__))


def taskinfo(friendly_name: str,
             synopsis: str,
             description: str,
             contact: TaskContact):
    """
    Use this decorator to provide helpful information about your task.
    :param friendly_name: a brief, human-friendly name for the task
    :param synopsis: a brief description of what the task does
    :param description: a nice, long, thorough description of what this task is expected to do
    :param contact: contact information for this task
    :return: The decorator returns the original class, after it's been modified.
    """
    def set_task_info(cls: Task):
        """
        Update the task information dictionary in the Task class.
        :param cls: the :py:class:`Task` class
        :return:
        """
        task_info = cls.get_task_info()
        task_info['friendly_name'] = (
            friendly_name if friendly_name is not None else
            '{module}.{cls}'.format(module=cls.__module__, cls=cls.__name__)
        )
        task_info['synopsis'] = synopsis
        task_info['description'] = description  # TODO: Replace tabs, newlines, and multiple spaces with single spaces.
        task_info['contact'] = contact
        # Return the original class.
        return cls
    # Return the inner function.
    return set_task_info

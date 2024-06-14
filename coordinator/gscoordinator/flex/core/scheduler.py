#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2023 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import datetime
import random
import time
from abc import ABCMeta
from abc import abstractmethod
from string import ascii_uppercase

import schedule
from schedule import CancelJob

from gscoordinator.flex.core.stoppable_thread import StoppableThread
from gscoordinator.flex.core.utils import decode_datetimestr


class Schedule(object):
    """Schedule class that wrapper dbader schedule

    Repo: https://github.com/dbader/schedule.
    """

    def __init__(self):
        self._schedule = schedule.Scheduler()
        # thread
        self._run_pending_trd = StoppableThread(target=self.run_pending, args=())
        self._run_pending_trd.daemon = True
        self._run_pending_trd.start()

    @property
    def schedule(self):
        return self._schedule

    def run_pending(self):
        """Run all jobs that are scheduled to run."""
        while True:
            try:
                self._schedule.run_pending()
                time.sleep(1)
            except:  # noqa: E722, B110
                pass


schedule = Schedule().schedule  # noqa: F811


class Scheduler(metaclass=ABCMeta):
    """
    Objects instantiated by the :class:`Scheduler <Scheduler>` are
    factories to create jobs, keep record of scheduled jobs and
    handle their execution in the :method:`run` method.
    """

    def __init__(self, at_time, repeat):
        # scheduler id
        self._scheduler_id = "SCHEDULER-{0}".format(
            "".join(random.choices(ascii_uppercase, k=16))
        )
        # periodic job as used
        self._job = None
        # true will be run immediately
        self._run_now = False
        # time at which this job to schedule
        self._at_time = self._decode_datetimestr(at_time)
        # repeat every day or week, or run job once(no repeat)
        # optional value "day", "week", "once"
        self._repeat = repeat
        # job running thread, note that the last job should be
        # end of execution at the beginning of the next job
        self._running_thread = None
        # tags
        self._tags = []

        # the following variables will be generated and overridden
        # when the job actually scheduled
        self._jobid = None
        self._last_run = None

    def _decode_datetimestr(self, datetime_str):
        if datetime_str == "now":
            self._run_now = True
            return datetime.datetime.now()
        return decode_datetimestr(datetime_str)

    def __str__(self):
        return "Scheduler(at_time={}, repeat={})".format(self._at_time, self._repeat)

    @property
    def monday(self):
        return self._at_time.weekday() == 0

    @property
    def tuesday(self):
        return self._at_time.weekday() == 1

    @property
    def wednesday(self):
        return self._at_time.weekday() == 2

    @property
    def thursday(self):
        return self._at_time.weekday() == 3

    @property
    def friday(self):
        return self._at_time.weekday() == 4

    @property
    def saturday(self):
        return self._at_time.weekday() == 5

    @property
    def sunday(self):
        return self._at_time.weekday() == 6

    @property
    def timestr(self):
        """return str of the time object."""
        return str(self._at_time.time())

    @property
    def job(self):
        """A periodic job managed by the dbader scheduler.
        https://github.com/dbader/schedule.
        """
        return self._job

    @property
    def jobid(self):
        """id for the last scheduled job"""
        return self._jobid

    @property
    def schedulerid(self):
        """id for the scheduler"""
        return self._scheduler_id

    @property
    def last_run(self):
        """datetime of the last run"""
        return self._last_run

    @property
    def tags(self):
        return self._tags

    @property
    def running_thread(self):
        return self._running_thread

    def run_once(self):
        """Run the job immediately."""
        self.do_run()
        return CancelJob

    def waiting_until_to_run(self):
        """Run the job once at a specific time."""
        if datetime.datetime.now() >= self._at_time:
            return self.run_once()

    def do_run(self):
        """Start a thread for the job."""
        # overwrite for each scheduled job
        self._jobid = "JOB-{0}".format("".join(random.choices(ascii_uppercase, k=16)))
        self._last_run = datetime.datetime.now()
        # schedule in a thread
        self._running_thread = StoppableThread(target=self.run, args=())
        self._running_thread.daemon = True
        self._running_thread.start()

    def submit(self):
        if not self._run_now and self._repeat not in [
            "week",
            "day",
            "once",
            None,
        ]:
            raise RuntimeError(
                "Submit schedule job failed: at_time: '{0}', repeat: '{1}'".format(
                    self._at_time, self._repeat
                )
            )

        if self._run_now:
            self._job = schedule.every().seconds.do(self.run_once)

        if not self._run_now and self._repeat == "week":
            if self.monday:
                self._job = schedule.every().monday.at(self.timestr).do(self.do_run)
            elif self.tuesday:
                self._job = schedule.every().tuesday.at(self.timestr).do(self.do_run)
            elif self.wednesday:
                self._job = schedule.every().wednesday.at(self.timestr).do(self.do_run)
            elif self.thursday:
                self._job = schedule.every().thursday.at(self.timestr).do(self.do_run)
            elif self.friday:
                self._job = schedule.every().friday.at(self.timestr).do(self.do_run)
            elif self.saturday:
                self._job = schedule.every().saturday.at(self.timestr).do(self.do_run)
            elif self.sunday:
                self._job = schedule.every().sunday.at(self.timestr).do(self.do_run)

        if not self._run_now and self._repeat == "day":
            self._job = schedule.every().day.at(self.timestr).do(self.do_run)

        if not self._run_now and self._repeat in ["once", None]:
            self._job = (
                schedule.every().day.at(self.timestr).do(self.waiting_until_to_run)
            )

        # tag
        self._job.tag(self._scheduler_id, *self._tags)

    def start(self):
        """Submit and schedule the job."""
        self.submit()

    def cancel(self, wait=False):
        """
        Set the running job thread stoppable and wait for the
        thread to exit properly by using join() method.

        Args:
            wait: Whether to wait for the wait to exit properly.
        """
        if self._running_thread is not None and self._running_thread.is_alive():
            self._running_thread.stop()
            if wait:
                self._running_thread.join()

    def stopped(self):
        """
        Check the stoppable flag of the current thread.
        """
        if self._running_thread is None:
            return True
        return self._running_thread.stopped()

    @abstractmethod
    def run(self):
        """
        Methods that all subclasses need to implement, note that
        subclass needs to handle exception by itself.
        """
        raise NotImplementedError


def cancel_job(job, delete_scheduler=True):
    """
    Cancel the job which going to scheduled or cancel the whole scheduler.

    Args:
        job: Periodic job as used by :class:`Scheduler`.
        delete_scheduler: True will can the whole scheduler, otherwise,
            delay the next-run time by on period.
    """
    if delete_scheduler:
        schedule.cancel_job(job)
    else:
        job.next_run += job.period

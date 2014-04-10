#
# Run tasks in a background process, passing the data they require via fork().
#
# (c) 2014 Russell Stuart
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU AFFERO General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# As a special exception to the AGPLv3+, the author grants you permission
# to redistribute this program without the accompanying ""Installation
# Information" described in clause 6.
#
import errno
import fcntl
import cPickle
import os
import select
import sys
import thread


class RaisedException(object):
    """
      This class is returned by TaskResult if the func passed to submit_job
      raises and exception instead of returning normally.  It's one member
      exc_info is the sys.exc_info() of the exception.
    """
    exc_info = None

    def __init__(self, exc_info):
        self.exc_info = exc_info


class TaskResult(object):
    """
       This is the return value of BackgroundTasks.submit_job().  It is
       a function like object that when called returns TaskResult.RUNNING
       until the task finishes, then TaskResult.task_completed() is called and
       this returns the return value of the function passed to
       BackgroundTasks.submit_job().  TaskResult.task_completed() may be called
       from another thread if background_thread is set to True in
       BackgroundTasks.  The default TaskResult.task_completed() does nothing.
    """
    RUNNING = object()
    __background_task = None
    __result = RUNNING
    task_completed = None

    def __init__(self, background_task):
        self.__background_task = background_task
        self.task_completed = lambda self: None

    def __call__(self):
        self.__background_task._dispatch()
        self.__background_task._lock.acquire()
        result = self.__result
        self.__background_task._lock.release()
        return result

    def set_result(self, result):
        self.__result = result
        self.task_completed(self)


class BackgroundTasks(object):
    __LEN = 8
    max_processes = None
    _lock = None
    __queue = None
    __results = None
    __processes = None
    __thread_pipe = None

    def __init__(self, max_processes=1, background_thread=True):
        """
          Create a new background tasks object.  map_processes is the
          maximum number of processes that can be running at once.
          If background_thread is True new tasks will be fired off by
          a background thread, otherwise we depend on clients polling
          us.
        """
        self.max_processes = max_processes
        self.__queue = []
        self.__results = {}
        self.__processes = {}
        self._lock = thread.allocate_lock()
        if background_thread:
            self.__thread_pipe = self._pipe()
            thread.start_new_thread(self._thread, ())

    def submit_task(self, func, *args, **kwds):
        """
          Submit a new task to be run in the background.  The task is:
            result = func(*args, **kwds)
          The return value is a function that will return TaskResult.RUNNING
          until the result is known, thereafter the return value of func()
          unless it raised an exception, and a RaisedException object if it
          raise an exception.
        """
        taskid = object()
        self._lock.acquire()
        try:
            result = TaskResult(self)
            self.__queue.append((id(result), func, args, kwds))
            self.__results[id(result)] = result
        finally:
            self._lock.release()
        self._dispatch()
        return result

    def _dispatch(self):
        """See if any background tasks can be started."""
        self._poll()
        self._lock.acquire()
        if not self.__queue or len(self.__processes) == self.max_processes:
            self._lock.release()
            return
        if self.max_processes == 1:
            task_count = len(self.__queue)
        else:
            task_count = len(self.__queue) // self.max_processes + 1
        tasks = self.__queue[-task_count:]
        del self.__queue[-task_count:]
        self._lock.release()
        pipe = self._pipe()
        child_pid = os.fork()
        if child_pid == 0:
            # Releases resources that aren't relevant to us.
            self.__queue = None
            for r in self.__processes:
                os.close(r)
            self.__processes = None
            if self.__thread_pipe:
                for h in self.__thread_pipe:
                    os.close(h)
                self.__thread_pipe = None
            os.close(pipe[0])
            self.__results = None
            self._process_tasks(pipe[1], tasks)
        os.close(pipe[1])
        self._lock.acquire()
        self.__processes[pipe[0]] = (child_pid, [""])
        self._lock.release()
        if self.__thread_pipe is not None:
            os.write(self.__thread_pipe[1], "x")

    def _process_tasks(self, pipe, tasks):
        """
            Process the tasks passed, and report the results to the parent
            process via out pipe.
        """
        while tasks:
            taskid, func, args, kwds = tasks.pop(0)
            try:
                result = func(*args, **kwds)
            except Exception, e:
                result = self.RaisedException(sys.exc_info())
            data = cPickle.dumps((taskid, result))
            length = "%0*x" % (self.__LEN, len(data))
            os.write(pipe, length + data)
        os.close(pipe)
        sys.exit(0)

    def _poll(self):
        """Process any reports from the background tasks."""
        try:
            self._lock.acquire()
            while True:
                if not self.__processes:
                    return
                # Does anyone have anything to report?
                ready, _, _ = select.select(list(self.__processes), [], [], 0)
                if not ready:
                    return
                for r in ready:
                    # Find the reporting process
                    child_pid, data_list = self.__processes[r]
                    data_list_len = sum(len(d) for d in data_list)
                    if data_list_len < self.__LEN:
                        # He is tell us how big the pickled results are
                        d = os.read(r, self.__LEN - data_list_len)
                        data_list[0] += d
                    else:
                        # Read the pickled results.
                        data_len = int(data_list[0], 16)
                        d = os.read(r, data_len + self.__LEN - data_list_len)
                        data_list.append(d)
                    # An empty read means the process has exited.
                    if not d:
                        os.close(r)
                        os.waitpid(child_pid, 0)
                        del self.__processes[r]
                        if self.__thread_pipe is not None:
                            os.write(self.__thread_pipe[1], "e")
                    data_list_len += len(d)
                    # If we have read all of the picked data, process it.
                    if data_list_len >= self.__LEN:
                        data_len = int(data_list[0], 16)
                        if data_list_len == data_len + self.__LEN:
                            tid, result = cPickle.loads(''.join(data_list[1:]))
                            self.__results[tid].set_result(result)
                            del self.__results[tid]
                            data_list[:] = [""]
        finally:
            self._lock.release()

    def _thread(self):
        """Fire off processes in the background."""
        while True:
            self._lock.acquire()
            rlist = list(self.__processes) + [self.__thread_pipe[0]]
            self._lock.release()
            # Wait until some child is ready, or the self._process changes.
            ready, _, _ = select.select(rlist, [], [])
            if self.__thread_pipe[0] in rlist:
                rlist.remove(self.__thread_pipe[0])
                try:
                    d = os.read(self.__thread_pipe[0], 1)
                    if not d:
                        return
                except OSError, e:
                    if e.errno != errno.EAGAIN:
                        raise
            if rlist:
                self._dispatch()

    def _pipe(cls):
        result = os.pipe()
        for fd in result:
            orig = fcntl.fcntl(fd, fcntl.F_GETFL)
            fcntl.fcntl(fd, fcntl.F_SETFL, orig | os.O_NONBLOCK)
        return result
    _pipe = classmethod(_pipe)

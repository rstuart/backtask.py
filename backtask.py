# Run tasks in a background process, passing the data they require via fork().
#
#
# Quick start
# -----------
#
#   import backtask
#
#   def a_task(arg, kwd=true):
#     :
#     :
#
#   def on_complete(result):
#     a_task_return_value = result()
#     if isinstance(a_task_return_value, backtask):
#       # a_task raised the exception a_task_return_value.exc_info[]
#     else:
#       # a_task_return_value is the return value of a_task()
#
#   bt = backtask.BackgroundTasks()
#   result = bt.submit_job(a_task, 1, kwd=false)
#   result.on_complete(on_complete)
#
#
# Unit Test
# ---------
#
#   python backtask.py
#
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
       BackgroundTasks.submit_task() returns an instance of this class.  This
       is a function like object that when called returns TaskResult.RUNNING
       until the task completes, then it returns whatever the func given to
       submit_task() returned.  If the func threw an exception instead of
       returning normally this will be an instance of RaisedException.  The
       function passed to set_on_complete() will be called when the task
       completes.  It's one argument is this instance.  Beware that if
       BackgroundTasks uses a background thread, this function may be called
       from a thread.
    """
    RUNNING = object()
    __background_task = None
    __notified = False
    __on_complete = None
    __result = RUNNING

    def __init__(self, background_task):
        self.__background_task = background_task

    def __call__(self):
        self.__background_task._dispatch()
        self.__background_task._lock.acquire()
        result = self.__result
        self.__background_task._lock.release()
        return result

    def _set_result(self, result):
        self.__background_task._lock.acquire()
        self.__result = result
        on_complete = self.__on_complete
        if on_complete is not None:
            self.__notified = True
        self.__background_task._lock.release()
        if on_complete is not None:
            on_complete(self)

    def set_on_complete(self, on_complete):
        """
            Arrange for on_complete to be called when the task completes.
            It's one argument is this instance.
        """
        self.__background_task._lock.acquire()
        result = self.__result
        self.__on_complete = on_complete
        if result is self.RUNNING or self.__notified:
            on_complete = None
        elif on_complete is not None:
            self.__notified = True
        self.__background_task._lock.release()
        if on_complete is not None:
            on_complete(self)


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
            self.__thread_pipe = [self._nonblock(fd) for fd in os.pipe()]
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
        tasks, self.__queue[:task_count] = self.__queue[:task_count], []
        self._lock.release()
        pipe = os.pipe()
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
        self._nonblock(pipe[0])
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
            except BaseException, e:
                result = self.RaisedException(sys.exc_info())
            data = cPickle.dumps((taskid, result))
            length = "%0*x" % (self.__LEN, len(data))
            os.write(pipe, length + data)
        os.close(pipe)
        sys.exit(0)

    def _poll(self):
        """Process any reports from the background tasks."""
        set_results = []
        try:
            self._lock.acquire()
            while True:
                if not self.__processes:
                    break
                # Does anyone have anything to report?
                ready, _, _ = select.select(list(self.__processes), [], [], 0)
                if not ready:
                    break
                for r in ready:
                    # Find the reporting process
                    child_pid, data_list = self.__processes[r]
                    data_list_len = sum(len(d) for d in data_list)
                    if data_list_len < self.__LEN:
                        # He is telling us how big the pickled results are
                        read_amount = self.__LEN - data_list_len
                    else:
                        # Read the pickled results.
                        data_len = int(data_list[0], 16)
                        read_amount = data_len + self.__LEN - data_list_len
                    try:
                        d = os.read(r, read_amount)
                    except OSError, e:
                        if e.errno == errno.EAGAIN:
                            continue
                        raise
                    # An empty read means the process has exited.
                    if not d:
                        os.close(r)
                        os.waitpid(child_pid, 0)
                        del self.__processes[r]
                        if self.__thread_pipe is not None:
                            os.write(self.__thread_pipe[1], "e")
                    else:
                        if data_list_len < self.__LEN:
                            data_list[0] += d
                        else:
                            data_list.append(d)
                        data_list_len += len(d)
                    # If we have read all of the picked data, process it.
                    if data_list_len >= self.__LEN:
                        data_len = int(data_list[0], 16)
                        if data_list_len == data_len + self.__LEN:
                            tid, result = cPickle.loads(''.join(data_list[1:]))
                            task_result = self.__results.pop(tid)
                            set_results.append((task_result, result))
                            data_list[:] = [""]
        finally:
            self._lock.release()
        for task_result, result in set_results:
            task_result._set_result(result)

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

    def _nonblock(cls, fd):
        """Make the passed file descriptor non blocking"""
        orig = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, orig | os.O_NONBLOCK)
        return fd
    _nonblock = classmethod(_nonblock)


def unit_test():
    """
        Should print:

        =====
        Hi 0!
        *****
        Hi 1!
        [0, 1, 2, 3, 4]
    """
    import time
    b = BackgroundTasks()
    r = [b.submit_task(lambda i: i, i) for i in range(5)]
    r[0].set_on_complete(lambda _: sys.stdout.write('Hi 0!\n'))
    print '====='
    time.sleep(1)
    print '*****'
    r[1].set_on_complete(lambda _: sys.stdout.write('Hi 1!\n'))
    z = [rr() for rr in r]
    print z

if __name__ == "__main__":
    unit_test()

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
import cPickle
import cStringIO
import errno
import fcntl
import os
import select
import sys
import thread
import traceback


class RaisedException(object):
    """
      This class is returned by TaskResult if the func passed to submit_job
      raises and exception instead of returning normally.  It's members are
      exception, the BaseException raised, and traceback, the traceback (a
      string).
    """
    exception = None
    traceback = None

    def __init__(self, exc_info):
        self.exception = exc_info[1]
        backtrace = []
        e = exc_info
        while True:
            string_file = cStringIO.StringIO()
            traceback.print_exception(e[0], e[1], e[2], None, string_file)
            lines = string_file.getvalue().splitlines()
            if backtrace:
                lines[:1] = ["", lines[0].replace("Traceback", "Caused by")]
            backtrace.extend([l.rstrip() for l in lines])
            cause = getattr(e[1], "cause", None)
            if not cause or len(cause) != 3 or type(cause[2]) != type(e[2]):
                break
            e = cause
        self.traceback = '\n'.join(backtrace) + "\n"

    def __repr__(self):
        return 'RaisedException(%r)' % (self.traceback,)


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
        try:
            result = self.__result
        finally:
            self.__background_task._lock.release()
        return result

    def _set_result(self, result):
        self.__background_task._lock.acquire()
        try:
            self.__result = result
            on_complete = self.__on_complete
            if on_complete is not None:
                self.__notified = True
        finally:
            self.__background_task._lock.release()
        if on_complete is not None:
            on_complete(self)

    def set_on_complete(self, on_complete):
        """
            Arrange for on_complete to be called when the task completes.
            It's one argument is this instance.
        """
        self.__background_task._lock.acquire()
        try:
            result = self.__result
            self.__on_complete = on_complete
            if result is self.RUNNING or self.__notified:
                on_complete = None
            elif on_complete is not None:
                self.__notified = True
        finally:
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
    __thread_lock = None
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
            self.__thread_lock = thread.allocate_lock()
            self.__thread_lock.acquire()
            try:
                self.__thread_pipe = os.pipe()
                thread.start_new_thread(self._thread, ())
                # Wait the thread to start
                os.read(self.__thread_pipe[0], 1)
            finally:
                self.__thread_lock.release()
            self._nonblock(self.__thread_pipe[1])

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
        try:
            if not self.__queue or len(self.__processes) == self.max_processes:
                return
            if self.max_processes == 1:
                task_count = len(self.__queue)
            else:
                task_count = len(self.__queue) // self.max_processes + 1
            tasks, self.__queue[:task_count] = self.__queue[:task_count], []
        finally:
            self._lock.release()
        pipe = os.pipe()
        child_pid = os.fork()
        if child_pid == 0:
            # Releases resources that aren't relevant to us.
            self.__queue = None
            for r in self.__processes:
                os.close(r)
            self.__processes = None
            if self.__thread_pipe is not None:
                for h in self.__thread_pipe:
                    os.close(h)
                self.__thread_pipe = None
            os.close(pipe[0])
            self.__results = None
            self._process_tasks(pipe[1], tasks)
        os.close(pipe[1])
        self._lock.acquire()
        try:
            self._nonblock(pipe[0])
            self.__processes[pipe[0]] = (child_pid, [""])
        finally:
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
                result = RaisedException(sys.exc_info())
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
        # Tell main thread we have starteed.
        os.write(self.__thread_pipe[1], "s")
        self.__thread_lock.acquire()
        self._nonblock(self.__thread_pipe[0])
        while True:
            self._lock.acquire()
            try:
                rlist = list(self.__processes) + [self.__thread_pipe[0]]
            finally:
                self._lock.release()
            # Wait until some child is ready, or the self._process changes.
            ready, _, _ = select.select(rlist, [], [])
            if self.__thread_pipe[0] in rlist:
                rlist.remove(self.__thread_pipe[0])
                # Main thread closes the pipe to tell us to exit.
                try:
                    d = os.read(self.__thread_pipe[0], 1)
                    if not d:
                        break
                except OSError, e:
                    if e.errno != errno.EAGAIN:
                        raise
            if rlist:
                self._dispatch()
        os.close(self.__thread_pipe[0])
        # Tell main thread we have exited.
        self.__thread_lock.release()

    def _nonblock(cls, fd):
        """Make the passed file descriptor non blocking"""
        orig = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, orig | os.O_NONBLOCK)
    _nonblock = classmethod(_nonblock)

    def task_count(self):
        """Return the number of pending tasks."""
        self._lock.acquire()
        try:
            return len(self.__results) + len(self.__queue)
        finally:
            self._lock.release()

    def close(self):
        """Shutdown nicely.  If you don't call this you may get
        "sys.excepthook is missing" messages.   If you call this before
        all tasks have completed the background tasks will fail with 
        broken pipes."""
        if self.__thread_pipe is not None:
            os.close(self.__thread_pipe[1])
            self.__thread_lock.acquire()
            self.__thread_lock.release()
        for r in self.__processes:
            os.close(r)


def unit_test():
    """
        Should print:

        =====
        Hi 0!
        *****
        Hi 1!
        [0, 1, 2, 3, 4, RaisedException('...')]
        Traceback (most recent call last):
          :
          :
        OSError: [Errno 32] Broken pipe
    """
    import time
    b = BackgroundTasks()
    r = [b.submit_task(lambda i: i, i) for i in range(5)]
    r.append(b.submit_task(lambda: 1 // 0))
    r[0].set_on_complete(lambda _: sys.stdout.write('Hi 0!\n'))
    sys.stdout.write('=====\n')
    time.sleep(1)
    sys.stdout.write('*****\n')
    r[1].set_on_complete(lambda _: sys.stdout.write('Hi 1!\n'))
    sys.stdout.write("%r\n" % ([rr() for rr in r],))
    r.append(b.submit_task(lambda: 1 // 0))
    b.close()

if __name__ == "__main__":
    unit_test()

import threading
import time


class TimeoutError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class RecursionError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class StealError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class SharedLock(object):
    """Defines a shared lock which allows multiple threads to acquire the lock
       in shared mode, but only one thread to acquire the lock in exlcusive
       mode.
    """

    def __init__(self):
        """Constructor for a shared lock.
        """
        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)
        self.exclusiveLock = None
        self.lockersList = set()

    def __call__(self, shared):
        """Make the objects callable to acquire a lock.
        """
        self.acquire(shared=shared)
        return self

    def __enter__(self):
        """Alows the shared lock to be used with the "with" construct.
        """
        return self

    def __exit__(self, type, value, traceback):
        """Alows the shared lock to be used with the "with" construct.
        """
        self.release()

    def acquire(self, timeout=None, shared=True):
        """Acquire a lock.
        """
        if timeout is not None:
            waitStart = time.time()
            waitTime = 0

        with self.lock:
            # Wait until there are no exclusive locks
            while self.exclusiveLock is not None:
                if timeout is None:
                    # No timeouts
                    self.condition.wait()
                else:
                    self.condition.wait(timeout - waitTime)
                    waitTime = time.time() - waitStart
                    if self.exclusiveLock is not None and waitTime > timeout:
                        raise TimeoutError("Timeout waiting for a shared lock")

            # exclusive lock is now None, we can acquire either a shared or
            # exclusive lock
            if shared:
                if threading.current_thread() in self.lockersList:
                    raise RecursionError("SharedLock is not recursive")
                self.lockersList.add(threading.current_thread())
            else:
                while len(self.lockersList) > 0:
                    if timeout is None:
                        # No timeouts
                        self.condition.wait()
                    else:
                        self.condition.wait(timeout - waitTime)
                        waitTime = time.time() - waitStart
                        if len(self.lockersList) > 0 and waitTime > timeout:
                            raise TimeoutError(
                                    "Timeout waiting for a shared lock")
                self.exclusiveLock = threading.current_thread()

    def steal(self):
        """Assign ownership of an exclusive lock to the current thread
        """
        if self.exclusiveLock is not None:
            self.exclusiveLock = threading.current_thread()
        else:
            raise StealError("It is only possible to steal an exclusive lock")


    def release(self):
        """Release a previously acquired lock.
        """

        with self.lock:
            if self.exclusiveLock == threading.current_thread():
                self.exclusiveLock = None
                assert len(self.lockersList) == 0
            else:
                self.lockersList.remove(threading.current_thread())
            self.condition.notify_all()

from __future__ import with_statement
from collections import deque
import functools
import random
import socket
import abc

# Use timer that's not susceptable to time of day adjustments.
try:
    # perf_counter is only present on Py3.3+
    from time import perf_counter as time_now
except ImportError:
    # fall back to using time
    from time import time as time_now


__all__ = ['StatsClient']


def safe_wraps(wrapper, *args, **kwargs):
    """Safely wraps partial functions."""
    while isinstance(wrapper, functools.partial):
        wrapper = wrapper.func
    return functools.wraps(wrapper, *args, **kwargs)


class Timer(object):
    """A context manager/decorator for statsd.timing()."""

    def __init__(self, client, stat, rate=1):
        self.client = client
        self.stat = stat
        self.rate = rate
        self.ms = None
        self._sent = False
        self._start_time = None

    def __call__(self, f):
        """Thread-safe timing function decorator."""
        @safe_wraps(f)
        def _wrapped(*args, **kwargs):
            start_time = time_now()
            try:
                return_value = f(*args, **kwargs)
            finally:
                elapsed_time_ms = 1000.0 * (time_now() - start_time)
                self.client.timing(self.stat, elapsed_time_ms, self.rate)
            return return_value
        return _wrapped

    def __enter__(self):
        return self.start()

    def __exit__(self, typ, value, tb):
        self.stop()

    def start(self):
        self.ms = None
        self._sent = False
        self._start_time = time_now()
        return self

    def stop(self, send=True):
        if self._start_time is None:
            raise RuntimeError('Timer has not started.')
        dt = time_now() - self._start_time
        self.ms = 1000.0 * dt  # Convert to milliseconds.
        if send:
            self.send()
        return self

    def send(self):
        if self.ms is None:
            raise RuntimeError('No data recorded.')
        if self._sent:
            raise RuntimeError('Already sent data.')
        self._sent = True
        self.client.timing(self.stat, self.ms, self.rate)


class StatsClientBase(object):
    """A Base class for various statsd clients."""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def _send(self):
        pass

    @abc.abstractmethod
    def pipeline(self):
        pass

    def timer(self, stat, rate=1):
        return Timer(self, stat, rate)

    def timing(self, stat, delta, rate=1):
        """Send new timing information. `delta` is in milliseconds."""
        self._send_stat(stat, '%0.6f|ms' % delta, rate)

    def incr(self, stat, count=1, rate=1):
        """Increment a stat by `count`."""
        self._send_stat(stat, '%s|c' % count, rate)

    def decr(self, stat, count=1, rate=1):
        """Decrement a stat by `count`."""
        self.incr(stat, -count, rate)

    def set(self, stat, value, rate=1):
        """Set a set value."""
        self._send_stat(stat, '%s|s' % value, rate)

    def _send_stat(self, stat, value, rate):
        self._after(self._prepare(stat, value, rate))

    def _prepare(self, stat, value, rate):
        if rate < 1:
            if random.random() > rate:
                return
            value = '%s|@%s' % (value, rate)

        if self._prefix:
            stat = '%s.%s' % (self._prefix, stat)

        return '%s:%s' % (stat, value)

    def _after(self, data):
        if data:
            self._send(data)


class StatsClient(StatsClientBase):
    """A client for statsd."""

    def __init__(self, host='localhost', port=8125, prefix=None, maxudpsize=512):
        """Create a new client."""
        fam = socket.AF_INET
        family, _, _, _, addr = socket.getaddrinfo(
            host, port, fam, socket.SOCK_DGRAM)[0]
        self._addr = addr
        self._sock = socket.socket(family, socket.SOCK_DGRAM)
        self._prefix = prefix
        self._maxudpsize = maxudpsize

    def _send(self, data):
        """Send data to statsd."""
        try:
            self._sock.sendto(data.encode('ascii'), self._addr)
        except (socket.error, RuntimeError):
            # No time for love, Dr. Jones!
            pass

    def pipeline(self):
        raise NotImplementedError

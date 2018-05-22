from __future__ import with_statement
import functools
import random
import re
import socket

import mock
from nose.tools import eq_

from statsd import StatsClient


ADDR = (socket.gethostbyname('localhost'), 8125)


# proto specific methods to get the socket method to send data
send_method = {
    'udp': lambda x: x.sendto,
}


# proto specific methods to create the expected value
make_val = {
    'udp': lambda x, addr: mock.call(str.encode(x), addr),
}


def _udp_client(prefix=None, addr=None, port=None, ipv6=False):
    if not addr:
        addr = ADDR[0]
    if not port:
        port = ADDR[1]
    sc = StatsClient(host=addr, port=port, prefix=prefix, ipv6=ipv6)
    sc._sock = mock.Mock()
    return sc


def _timer_check(sock, count, proto, start, end):
    send = send_method[proto](sock)
    eq_(send.call_count, count)
    value = send.call_args[0][0].decode('ascii')
    exp = re.compile('^%s:\d+|%s$' % (start, end))
    assert exp.match(value)


def _sock_check(sock, count, proto, val=None, addr=None):
    send = send_method[proto](sock)
    eq_(send.call_count, count)
    if not addr:
        addr = ADDR
    if val is not None:
        eq_(
            send.call_args,
            make_val[proto](val, addr),
        )


class assert_raises(object):
    """A context manager that asserts a given exception was raised.

    >>> with assert_raises(TypeError):
    ...     raise TypeError

    >>> with assert_raises(TypeError):
    ...     raise ValueError
    AssertionError: ValueError not in ['TypeError']

    >>> with assert_raises(TypeError):
    ...     pass
    AssertionError: No exception raised.

    Or you can specify any of a number of exceptions:

    >>> with assert_raises(TypeError, ValueError):
    ...     raise ValueError

    >>> with assert_raises(TypeError, ValueError):
    ...     raise KeyError
    AssertionError: KeyError not in ['TypeError', 'ValueError']

    You can also get the exception back later:

    >>> with assert_raises(TypeError) as cm:
    ...     raise TypeError('bad type!')
    >>> cm.exception
    TypeError('bad type!')
    >>> cm.exc_type
    TypeError
    >>> cm.traceback
    <traceback @ 0x3323ef0>

    Lowercase name because that it's a class is an implementation detail.

    """

    def __init__(self, *exc_cls):
        self.exc_cls = exc_cls

    def __enter__(self):
        # For access to the exception later.
        return self

    def __exit__(self, typ, value, tb):
        assert typ, 'No exception raised.'
        assert typ in self.exc_cls, '%s not in %s' % (
            typ.__name__, [e.__name__ for e in self.exc_cls])
        self.exc_type = typ
        self.exception = value
        self.traceback = tb

        # Swallow expected exceptions.
        return True


def _test_incr(cl, proto):
    cl.incr('foo')
    _sock_check(cl._sock, 1, proto, val='foo:1|c')

    cl.incr('foo', 10)
    _sock_check(cl._sock, 2, proto, val='foo:10|c')

    cl.incr('foo', 1.2)
    _sock_check(cl._sock, 3, proto, val='foo:1.2|c')

    cl.incr('foo', 10, rate=0.5)
    _sock_check(cl._sock, 4, proto, val='foo:10|c|@0.5')


@mock.patch.object(random, 'random', lambda: -1)
def test_incr_udp():
    """StatsClient.incr works."""
    cl = _udp_client()
    _test_incr(cl, 'udp')


def _test_decr(cl, proto):
    cl.decr('foo')
    _sock_check(cl._sock, 1, proto, 'foo:-1|c')

    cl.decr('foo', 10)
    _sock_check(cl._sock, 2, proto, 'foo:-10|c')

    cl.decr('foo', 1.2)
    _sock_check(cl._sock, 3, proto, 'foo:-1.2|c')

    cl.decr('foo', 1, rate=0.5)
    _sock_check(cl._sock, 4, proto, 'foo:-1|c|@0.5')


@mock.patch.object(random, 'random', lambda: -1)
def test_decr_udp():
    """StatsClient.decr works."""
    cl = _udp_client()
    _test_decr(cl, 'udp')


def test_ipv6_udp():
    """StatsClient can use to IPv6 address."""
    addr = ('::1', 8125, 0, 0)
    cl = _udp_client(addr=addr[0], ipv6=True)
    _test_ipv6(cl, 'udp', addr)


def _test_resolution(cl, proto, addr):
    cl.incr('foo')
    _sock_check(cl._sock, 1, proto, 'foo:1|c', addr=addr)


def test_ipv6_resolution_udp():
    cl = _udp_client(addr='localhost', ipv6=True)
    _test_resolution(cl, 'udp', ('::1', 8125, 0, 0))


def test_ipv4_resolution_udp():
    cl = _udp_client(addr='localhost')
    _test_resolution(cl, 'udp', ('127.0.0.1', 8125))


def _test_set(cl, proto):
    cl.set('foo', 10)
    _sock_check(cl._sock, 1, proto, 'foo:10|s')

    cl.set('foo', 2.3)
    _sock_check(cl._sock, 2, proto, 'foo:2.3|s')

    cl.set('foo', 'bar')
    _sock_check(cl._sock, 3, proto, 'foo:bar|s')

    cl.set('foo', 2.3, 0.5)
    _sock_check(cl._sock, 4, proto, 'foo:2.3|s|@0.5')


@mock.patch.object(random, 'random', lambda: -1)
def test_set_udp():
    """StatsClient.set works."""
    cl = _udp_client()
    _test_set(cl, 'udp')


def _test_timing(cl, proto):
    cl.timing('foo', 100)
    _sock_check(cl._sock, 1, proto, 'foo:100.000000|ms')

    cl.timing('foo', 350)
    _sock_check(cl._sock, 2, proto, 'foo:350.000000|ms')

    cl.timing('foo', 100, rate=0.5)
    _sock_check(cl._sock, 3, proto, 'foo:100.000000|ms|@0.5')


@mock.patch.object(random, 'random', lambda: -1)
def test_timing_udp():
    """StatsClient.timing works."""
    cl = _udp_client()
    _test_timing(cl, 'udp')


def _test_prepare(cl, proto):
    tests = (
        ('foo:1|c', ('foo', '1|c', 1)),
        ('bar:50|ms|@0.5', ('bar', '50|ms', 0.5)),
        ('baz:23|g', ('baz', '23|g', 1)),
    )

    def _check(o, s, v, r):
        with mock.patch.object(random, 'random', lambda: -1):
            eq_(o, cl._prepare(s, v, r))

    for o, (s, v, r) in tests:
        _check(o, s, v, r)


@mock.patch.object(random, 'random', lambda: -1)
def test_prepare_udp():
    """Test StatsClient._prepare method."""
    cl = _udp_client()
    _test_prepare(cl, 'udp')


def _test_prefix(cl, proto):
    cl.incr('bar')
    _sock_check(cl._sock, 1, proto, 'foo.bar:1|c')


@mock.patch.object(random, 'random', lambda: -1)
def test_prefix_udp():
    """StatsClient.incr works."""
    cl = _udp_client(prefix='foo')
    _test_prefix(cl, 'udp')


def _test_timer_manager(cl, proto):
    with cl.timer('foo'):
        pass

    _timer_check(cl._sock, 1, proto, 'foo', 'ms')


def test_timer_manager_udp():
    """StatsClient.timer can be used as manager."""
    cl = _udp_client()
    _test_timer_manager(cl, 'udp')


def _test_timer_decorator(cl, proto):
    @cl.timer('foo')
    def foo(a, b):
        return [a, b]

    @cl.timer('bar')
    def bar(a, b):
        return [b, a]

    # make sure it works with more than one decorator, called multiple
    # times, and that parameters are handled correctly
    eq_([4, 2], foo(4, 2))
    _timer_check(cl._sock, 1, proto, 'foo', 'ms')

    eq_([2, 4], bar(4, 2))
    _timer_check(cl._sock, 2, proto, 'bar', 'ms')

    eq_([6, 5], bar(5, 6))
    _timer_check(cl._sock, 3, proto, 'bar', 'ms')


def test_timer_decorator_udp():
    """StatsClient.timer is a thread-safe decorator (UDP)."""
    cl = _udp_client()
    _test_timer_decorator(cl, 'udp')


def _test_timer_capture(cl, proto):
    with cl.timer('woo') as result:
        eq_(result.ms, None)
    assert isinstance(result.ms, float)


def test_timer_capture_udp():
    """You can capture the output of StatsClient.timer (UDP)."""
    cl = _udp_client()
    _test_timer_capture(cl, 'udp')


def _test_timer_context_rate(cl, proto):
    with cl.timer('foo', rate=0.5):
        pass

    _timer_check(cl._sock, 1, proto, 'foo', 'ms|@0.5')


@mock.patch.object(random, 'random', lambda: -1)
def test_timer_context_rate_udp():
    """StatsClient.timer can be used as manager with rate."""
    cl = _udp_client()
    _test_timer_context_rate(cl, 'udp')


def _test_timer_decorator_rate(cl, proto):
    @cl.timer('foo', rate=0.1)
    def foo(a, b):
        return [b, a]

    @cl.timer('bar', rate=0.2)
    def bar(a, b=2, c=3):
        return [c, b, a]

    eq_([2, 4], foo(4, 2))
    _timer_check(cl._sock, 1, proto, 'foo', 'ms|@0.1')

    eq_([3, 2, 5], bar(5))
    _timer_check(cl._sock, 2, proto, 'bar', 'ms|@0.2')


@mock.patch.object(random, 'random', lambda: -1)
def test_timer_decorator_rate_udp():
    """StatsClient.timer can be used as decorator with rate."""
    cl = _udp_client()
    _test_timer_decorator_rate(cl, 'udp')


def _test_timer_context_exceptions(cl, proto):
    with assert_raises(socket.timeout):
        with cl.timer('foo'):
            raise socket.timeout()

    _timer_check(cl._sock, 1, proto, 'foo', 'ms')


def test_timer_context_exceptions_udp():
    cl = _udp_client()
    _test_timer_context_exceptions(cl, 'udp')


def _test_timer_decorator_exceptions(cl, proto):
    @cl.timer('foo')
    def foo():
        raise ValueError()

    with assert_raises(ValueError):
        foo()

    _timer_check(cl._sock, 1, proto, 'foo', 'ms')


def test_timer_decorator_exceptions_udp():
    cl = _udp_client()
    _test_timer_decorator_exceptions(cl, 'udp')


def _test_timer_object(cl, proto):
    t = cl.timer('foo').start()
    t.stop()

    _timer_check(cl._sock, 1, proto, 'foo', 'ms')


def test_timer_object_udp():
    """StatsClient.timer works."""
    cl = _udp_client()
    _test_timer_object(cl, 'udp')


def _test_timer_object_no_send(cl, proto):
    t = cl.timer('foo').start()
    t.stop(send=False)
    _sock_check(cl._sock, 0, proto)

    t.send()
    _timer_check(cl._sock, 1, proto, 'foo', 'ms')


def test_timer_object_no_send_udp():
    """Stop StatsClient.timer without sending."""
    cl = _udp_client()
    _test_timer_object_no_send(cl, 'udp')


def _test_timer_object_rate(cl, proto):
    t = cl.timer('foo', rate=0.5)
    t.start()
    t.stop()

    _timer_check(cl._sock, 1, proto, 'foo', 'ms@0.5')


@mock.patch.object(random, 'random', lambda: -1)
def test_timer_object_rate_udp():
    """StatsClient.timer works with rate."""
    cl = _udp_client()
    _test_timer_object_rate(cl, 'udp')


def _test_timer_object_no_send_twice(cl):
    t = cl.timer('foo').start()
    t.stop()

    with assert_raises(RuntimeError):
        t.send()


def test_timer_object_no_send_twice_udp():
    """StatsClient.timer raises RuntimeError if send is called twice."""
    cl = _udp_client()
    _test_timer_object_no_send_twice(cl)


def _test_timer_send_without_stop(cl):
    with cl.timer('foo') as t:
        assert t.ms is None
        with assert_raises(RuntimeError):
            t.send()

    t = cl.timer('bar').start()
    assert t.ms is None
    with assert_raises(RuntimeError):
        t.send()


def test_timer_send_without_stop_udp():
    """StatsClient.timer raises error if send is called before stop."""
    cl = _udp_client()
    _test_timer_send_without_stop(cl)


def _test_timer_object_stop_without_start(cl):
    with assert_raises(RuntimeError):
        cl.timer('foo').stop()


def test_timer_object_stop_without_start_udp():
    """StatsClient.timer raises error if stop is called before start."""
    cl = _udp_client()
    _test_timer_object_stop_without_start(cl)


def _test_big_numbers(cl, proto):
    num = 1234568901234
    tests = (
        # Explicitly create strings so we avoid the bug we're trying to test.
        ('incr', 'foo:1234568901234|c'),
        ('timing', 'foo:1234568901234.000000|ms'),
    )

    def _check(method, result):
        cl._sock.reset_mock()
        getattr(cl, method)('foo', num)
        _sock_check(cl._sock, 1, proto, result)

    for method, result in tests:
        _check(method, result)


def test_big_numbers_udp():
    """Test big numbers with UDP client."""
    cl = _udp_client()
    _test_big_numbers(cl, 'udp')


def _test_rate_no_send(cl, proto):
    cl.incr('foo', rate=0.5)
    _sock_check(cl._sock, 0, proto)


@mock.patch.object(random, 'random', lambda: 2)
def test_rate_no_send_udp():
    """Rate below random value prevents sending with StatsClient.incr."""
    cl = _udp_client()
    _test_rate_no_send(cl, 'udp')


def test_socket_error():
    """Socket error on StatsClient should be ignored."""
    cl = _udp_client()
    cl._sock.sendto.side_effect = socket.timeout()
    cl.incr('foo')
    _sock_check(cl._sock, 1, 'udp', 'foo:1|c')

from revault import computation
import pytest
import inspect
import time
import concurrent.futures


def test_compute_simple(store):
    counter = {}

    @computation()
    def my_fn(x, y):
        counter.setdefault((x, y), 0)
        counter[(x, y)] += 1
        return x * 10 + y

    with store:
        assert my_fn(10, 1) == 101
        assert my_fn(10, 1) == 101
        assert my_fn(1, 10) == 20
        assert my_fn(1, 10) == 20
        assert my_fn(10, 1) == 101

    assert counter[(10, 1)] == 1
    assert counter[(1, 10)] == 1


class MyException(Exception):
    pass


def test_compute_fail(store):
    flag = True

    @computation()
    def my_fn(x):
        if flag:
            raise MyException()
        return x * 2

    with store:
        with pytest.raises(MyException):
            my_fn(10)
        with pytest.raises(MyException):
            my_fn(10)
        flag = False
        assert my_fn(10) == 20
        flag = True
        assert my_fn(10) == 20


def test_compute_deps(store):
    @computation()
    def my_fn2(x, y):
        a = my_fn1(x)
        b = my_fn1(y)
        return a, b

    @computation()
    def my_fn1(x):
        return x * my_fn0()

    @computation()
    def my_fn0():
        return 10

    with store:
        assert store.load_or_none(my_fn0.ref()) is None
        with pytest.raises(Exception, match="not found"):
            assert store.load(my_fn0.ref())
        assert my_fn2(1, 3) == (10, 30)
        assert my_fn1.load(1) == 10
        assert my_fn1.load_or_none(2) is None
        assert my_fn1.load(3) == 30
        assert store.load(my_fn0.ref()) == 10

        assert set(my_fn1.keys()) == {my_fn1.ref(1).key, my_fn1.ref(3).key}
        assert my_fn2.keys() == [my_fn2.ref(1, 3).key]


def test_compute_none_result(store):
    counter = [0]

    @computation()
    def my_fn():
        counter[0] += 1
        return None

    with store:
        assert my_fn() is None
        assert my_fn() is None
        assert my_fn() is None

    assert counter[0] == 1


def test_compute_replicas(store):
    values = [0]

    @computation()
    def my_fn():
        values[0] += 1
        return values[0]

    with store:
        assert my_fn(replica=1) == 1
        assert my_fn(replica=2) == 2
        assert my_fn(replica=1) == 1
        assert my_fn(replica=1) == 1
        assert my_fn(replica=1) == 1
        assert my_fn(replica=3) == 3

        assert my_fn.replicas(4) == [4, 1, 2, 3]

    assert values[0] == 4


def test_compute_ignored_args(store):
    @computation()
    def my_fn(a, __x, b):
        return a + b + __x

    with store:
        assert my_fn.ref(10, 1, 20).key == my_fn.ref(10, 2, 20).key

        assert my_fn(10, 1, 20) == 31
        assert my_fn(10, 2, 20) == 31

        assert my_fn.ref(10, 1, 20).key.config == {
            "a": 10,
            "b": 20,
        }


def test_compute_key_as_input(store):
    @computation()
    def my_fn1(key):
        ref = my_fn2.ref_from_key(key)
        return store.get(ref) * 10

    @computation()
    def my_fn2(x, y):
        return x + y

    with store:
        r = my_fn2.ref(x=10, y=20)
        assert my_fn1(r.key) == 300


def test_compute_inspect():
    @computation
    def my_fn(x, y=10):
        pass

    assert my_fn.__name__ == "my_fn"
    assert sorted(inspect.signature(my_fn).parameters.keys()) == ["x", "y"]
    assert inspect.signature(my_fn).parameters["y"].default == 10


def test_compute_in_threads_ok(store):
    @computation
    def my_fn(x):
        time.sleep(1)
        return x * 10

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        with store:
            a = executor.submit(store.get, my_fn.ref(10))
            b = executor.submit(store.get, my_fn.ref(10))
            time.sleep(0.3)
            with pytest.raises(Exception, match="not found."):
                store.load_entry(my_fn.ref(10))
            assert b.result() == 100
            assert a.result() == 100
            assert store.load_entry(my_fn.ref(10)).result == 100


def test_compute_in_threads_fail(store):
    class TestException(Exception):
        pass

    @computation
    def my_fn(x):
        time.sleep(0.3)
        raise TestException("TEST")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        with store:
            a = executor.submit(store.get, my_fn.ref(10))
            b = executor.submit(store.get, my_fn.ref(10))
            time.sleep(0.3)
            with pytest.raises(TestException):
                b.result()
            with pytest.raises(TestException):
                a.result()
            assert store.load_entry_or_none(my_fn.ref(10)) is None

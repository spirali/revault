from revault import computation


def test_runtime_insert_new_replica(store):
    @computation()
    def my_fn(x):
        return x * 10

    store.insert_new_replica(my_fn.ref(x=20), "a")
    store.insert_new_replica(my_fn.ref(x=20), "b")

    assert set(ref.replica for ref in store.all_keys()) == {0, 1}

    with store:
        assert my_fn(x=20) == "a"
        assert my_fn(x=20, replica=1) == "b"


def test_remove(store):
    counter = [0]

    @computation
    def my_fn(x):
        counter[0] += 1
        return x * 10

    with store:
        my_fn(10)
        my_fn(20)
        assert counter[0] == 2
        my_fn(10)
        assert counter[0] == 2

        my_fn.remove(10)
        assert counter[0] == 2
        my_fn(10)
        assert counter[0] == 3
        my_fn(20)
        assert counter[0] == 3

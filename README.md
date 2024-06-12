<p align="center">
    <img width="160" src="docs/logo.png">
</p>

# Result Vault

Python module for persisting results of computation in a database.
It is desinged mainly for storing results from experiments, not for usage as cache.
Therefore it supports data quering and not features like time-to-live.

```python
from revault import computation, Store

@computation
def my_computation(x, y):
    return x + y

with Store("sqlite:///path/to/db"):
    assert my_computation(10, 20) == 30  # the function my_computation is performed
    assert my_computation(10, 20) == 30  # the result is taken from DB

    assert my_computation.load(10, 20) == 30  # load from DB, fails if not exists

    my_computation(10, 20, replica=1)  # call the function again and store the results
    my_computation(10, 20, replica=2)  # call the function again and store the results

    assert my_computation.load_replicas(10, 20) == [30, 30, 30]  # Load all replicas for given call
```


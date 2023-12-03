# Priority Expiry Cache

A simple Python package containing an implementation of a Priority Expiry Cache.

A Priority Expiry Cache stores key-value entries. Associated with each entry is an Expire Time and a Priority. 

The Priority Expiry Cache has an `evict` operation. Any expired entries are removed from the cache. If there are no
expired entries then entry with the lowest priority is removed. If there are multiple entries with the lowest priority, 
then the least recently used of those entries is removed.

## Usage
```python
import time

from priority_expiry import Cache

cache = Cache()

# Add an entry to the cache with a priority of 10 and an expiry duration of one second
with cache.context(priority=10, expiry_duration=int(1e9)):
    cache["first_test_key"] = "first_test_value"
    cache["second_test_key"] = "second_test_value"
    cache["third_test_key"] = "third_test_value"

# Retrieve the value of an entry
value = cache["first_test_key"]
assert value == "first_test_value"

# Evict entries. This removes the `second_test_key` entry as it is the least recently used 
# and no entries have expired yet.
cache.evict()
assert set(cache) == {"first_test_key", "third_test_key"}

time.sleep(1)

# Evict entries. This removes all remaining entries as they will have all expired.
cache.evict()
# Confirm that the cache is empty
assert not cache  
```

## Implementation
The [Cache](priority_expiry/mappings.py) has been implemented using a [Quadtree](priority_expiry/data_structures.py) to
store entries on a two-dimensional surface of Expire Times and Priorities. This structure allows entries to be 
efficiently evicted based on their Expire Time and Priority. A double-ended queue is maintained within each node to 
keep track of the least recently used entries.

## Dependencies
This has been tested and developed on Python 3.11. The package itself has no other dependencies.

## Running tests
Test dependencies including [pytest](https://docs.pytest.org) and [hypothesis](https://hypothesis.readthedocs.io) can
be installed into a venv from [requirements.txt](requirements.txt)
```shell
git clone https://github.com/lankylad/priority-expiry-cache
cd priority-expiry-cache
python -m venv priority_expiry_cache_venv  # setup venv
./priority_expiry_cache_venv/bin/activate  # activate venv
pip install -r requirements.txt  # install test dependencies
```
To run tests simply run `pytest`
```shell
pytest
```
Expected output
```text
=============================================================== test session starts ================================================================
platform linux -- Python 3.11.5, pytest-7.4.3, pluggy-1.3.0
rootdir: /home/alec/projects/priority-expiry-cache
plugins: hypothesis-6.91.0
collected 9 items                                                                                                                                  

tests/test_priority_expiry_cache.py .........                                                                                                [100%]

================================================================ 9 passed in 2.60s =================================================================
```
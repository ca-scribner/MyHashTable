import pytest
from MyHashTable import MyHashTable
from collections import deque
from contextlib import contextmanager

# Note: For deterministic testing need to set environment variable PYTHONHASHSEED=1


@contextmanager
def does_not_raise():
    yield


@pytest.mark.parametrize(
    "inp",
    [
        {'key': 'key1',
         'value': 'value1',
         'updated_value': 'value1b',
         },
        {'key': 1,
         'value': 'one',
         'updated_value': 'oneone',
         },
        {'key': 'two',
         'value': 2,
         'updated_value': 4,
         },
    ]
)
def test_MyHashTable_setget(inp):
    ht = MyHashTable()
    key = inp['key']
    value = inp['value']
    updated_value = inp['updated_value']

    ht[key] = value
    assert value == ht[key]

    ht[key] = updated_value
    assert updated_value == ht[key]


AUTOSCALE_KEYS_TESTS_INPUTS = [
    # Does not scale and doesn't need to scale
    {
        'hash_table_settings': {'bucket_increment': 2,
                                'n_buckets': 4,
                                'max_utilization_fraction': 0.5,
                                'autoscale': False,
                                },
        'n': 2,
        'raises': does_not_raise(),
    },
    # Does not scale and doesn't need to scale
    {
        'hash_table_settings': {'bucket_increment': 2,
                                'n_buckets': 4,
                                'max_utilization_fraction': 0.5,
                                'autoscale': False,
                                },
        'n': 4,
        'raises': does_not_raise(),
    },

    # Does not scale and maxes size because of it
    {
        'hash_table_settings': {'bucket_increment': 2,
                                'n_buckets': 4,
                                'max_utilization_fraction': 0.5,
                                },
        'n': 10,
        'raises': pytest.raises(ValueError),
    },

    # # Does scale and is ok because of it
    # {
    #     'hash_table_settings': {'bucket_increment': 2,
    #                             'n_buckets': 4,
    #                             'max_utilization_fraction': 0.5,
    #                             },
    #     'n': 20,
    #     'raises': does_not_raise(),
    # },
]


@pytest.mark.parametrize(
    "inputs",
    [
        # Does not scale and doesn't need to scale
        {
            'hash_table_settings': {'bucket_increment': 2,
                                    'n_buckets': 4,
                                    'max_utilization_fraction': 0.5,
                                    'autoscale': False,
                                    },
            'n': 2,
            'raises': does_not_raise(),
        },
        # Does not scale and doesn't need to scale
        {
            'hash_table_settings': {'bucket_increment': 2,
                                    'n_buckets': 4,
                                    'max_utilization_fraction': 0.5,
                                    'autoscale': False,
                                    },
            'n': 4,
            'raises': does_not_raise(),
        },

        # Does not scale and maxes size because of it
        {
            'hash_table_settings': {'bucket_increment': 2,
                                    'n_buckets': 4,
                                    'max_utilization_fraction': 0.5,
                                    },
            'n': 10,
            'raises': pytest.raises(ValueError),
        },

        # Does scale and is ok because of it
        {
            'hash_table_settings': {'bucket_increment': 2,
                                    'n_buckets': 4,
                                    'max_utilization_fraction': 0.5,
                                    },
            'n': 20,
            'raises': does_not_raise(),
        },
    ]
)
def test_MyHashTable_autoscale(inputs):
    with inputs['raises']:
        ht = MyHashTable(**inputs['hash_table_settings'])
        n = inputs['n']
        data = deque()

        for i in range(n):
            # Using string of i as key to get disorderly hash values
            # Value is +0.01 to know we haven't swapped key and value by mistake
            k = str(i)
            v = i + 0.01

            # Remember for testing later
            data.append((k, v))

            # Do all writes twice to test that a full container can still be written to for existing keys
            for _ in range(2):
                ht[k] = v

        assert_ht_has(ht, data)


@pytest.mark.parametrize(
    "inputs",
    [
        # Does not scale and doesn't need to scale
        {
            'hash_table_settings': {'bucket_increment': 2,
                                    'n_buckets': 4,
                                    'max_utilization_fraction': 0.5,
                                    'autoscale': False,
                                    },
            'n': 2,
            'raises': does_not_raise(),
        },
        # Does not scale and doesn't need to scale
        {
            'hash_table_settings': {'bucket_increment': 2,
                                    'n_buckets': 4,
                                    'max_utilization_fraction': 0.5,
                                    'autoscale': False,
                                    },
            'n': 4,
            'raises': does_not_raise(),
        },

        # Does not scale and maxes size because of it
        {
            'hash_table_settings': {'bucket_increment': 2,
                                    'n_buckets': 4,
                                    'max_utilization_fraction': 0.5,
                                    },
            'n': 10,
            'raises': pytest.raises(ValueError),
        },

        # Does scale and is ok because of it
        {
            'hash_table_settings': {'bucket_increment': 2,
                                    'n_buckets': 4,
                                    'max_utilization_fraction': 0.5,
                                    },
            'n': 20,
            'raises': does_not_raise(),
        },
    ]
)
def test_MyHashTable_keys(inputs):
    print(inputs['raises'])
    with inputs['raises']:
        ht = MyHashTable(**inputs['hash_table_settings'])
        n = inputs['n']
        keys = set()

        for i in range(n):
            # Using string of i as key to get disorderly hash values
            # Value is +0.01 to know we haven't swapped key and value by mistake
            k = str(i)
            v = i + 0.01

            # Remember for testing later
            keys.add(k)

            ht[k] = v

        assert keys == set(ht.keys())


@pytest.mark.parametrize(
    "inputs",
    [
        {
            'hash_table_settings': {'n_buckets': 10,
                                    'autoscale': False,
                                    },
            'n_data': 5,
            'updated_size': 30,
            'raises': does_not_raise(),
        },
    ]
)
def test_MyHashTable_resizedata(inputs):
    ht = MyHashTable(**inputs['hash_table_settings'])
    data = deque()

    for i in range(inputs['n_data']):
        ht[i] = [i]
        data.append((i, i))

    old_size = ht.n_buckets

    ht._resize_data(inputs['updated_size'])

    assert old_size == ht.n_buckets

    assert_ht_has(ht, data)


# Helpers
def assert_ht_has(ht, iterable):
    """
    Assert that a hash table has all the (k,v) tuples in iterable
    """
    for k, v in iterable:
        assert v == ht[k]





# Todo: Test in, keys, iter

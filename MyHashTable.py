# Custom hash table to understand how they work better
#
# Inspired by Fluent Python Ex 3-16 and related code
#
# Specs:
#
# * Mappable that accepts hashable keys and stores values associated with them
# * Should log collisions whenever we get/set so I can see how collisions vary with utilization
# * must be self-resizing (if it fills up past some threshold, extend the internal store to reduce utilization)
# * For hashing and binning, make use of the hash() function for hashing and then mod arithmatic to confine to correct
#   number of bins.
# * For hash collisions, do a simple +1 approach until we find the correct or empty bin

from collections.abc import MutableMapping
from collections import namedtuple, deque
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)
consoleHandler = logging.StreamHandler()
formatter = logging.Formatter('%(name)s - %(funcName)20s() - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)


class MyHashTable(MutableMapping):

    def __init__(self, n_buckets=100, auto_resize=True, max_utilization_fraction=0.7, bucket_increment=100):
        """
        Returns an instance of a hash table which resizes as it grows and lots its collision performance

        Args:
            n_buckets (int): Number of buckets in initial object (creates a numpy list of this length to store data)
            auto_resize (boolean): If True, container will automatically resize the internal data store when utilization
                                   is above max_utilization_fraction
            max_utilization_fraction (float): Utilization fraction above which the container will resize itself to keep
                                              utilization (and thus collisions) low
            bucket_increment (int): Amount by which the internal data store will increased in size whenever the
                                    container is more than max_utilization_fraction full
        """
        self._bucket_increment = bucket_increment
        self._max_utilization_fraction = max_utilization_fraction
        self._n_data = None
        self._data = None
        self._auto_resize = auto_resize

        self._collision_log_template = namedtuple('collisionLog', 'n_buckets len collisions')
        self.collision_log = []
        self.n_collisions = 0

        self._data_record_template = namedtuple('dataRecord', 'key value')

        self._initialize_data(n_buckets)

    def __setitem__(self, key, value):
        """
        Sets the value to a given key, automatically scaling the internal data storage up if necessary (and enabled)

        Args:
            key (hashable): Key to be used to store data
            value: Value to store

        Returns:
            None
        """
        logger.debug(f"set {key}: {value} (start)")
        try:
            i_bucket = self._key_to_bucket(key)
        except KeyError:
            # KeyError means we could not find one of an empty bucket or bucket with this key
            raise ValueError("Cannot set {key}: {value}, HashTable is full!")

        # If _data[i_bucket] is none, we are adding new data and will need to update the count of used buckets
        data_added = self._data[i_bucket] is None
        self._data[i_bucket] = self._data_record_template(key, value)

        # Doing this after committing to .data just in case something goes wrong.  Not sure if an error there would
        # leave the n_data incremented incorrectly or not?
        if data_added:
            # After new record is actually committed to ._data, update the internal count of used buckets
            # This might be safe to do above, but wasn't sure if updating the count first was safe in the case of an
            # exception raised by the setting to ._data
            self._increment_n_data()

        logger.debug(f"set {key}: {value} -> bucket {i_bucket} ({self.utilization_status})")

        # Resize if necessary
        if self._auto_resize and (self.utilization > self._max_utilization_fraction):
            self._resize_data(self.n_buckets + self._bucket_increment)

    def __getitem__(self, key):
        logger.debug(f"get {key}")
        i_bucket = self._key_to_bucket(key)
        whole_record = self._data[i_bucket]
        if whole_record is None:
            raise KeyError(key)
        value = whole_record.value
        logger.debug(f"get {whole_record.key}: {value} from bucket {i_bucket}")
        return value

    def __delitem__(self, key):
        """
        Removes the data at a given key, decrementing the count of used buckets at the same time
        """
        i_bucket = self._key_to_bucket(key)
        self._data[i_bucket] = None
        self._decrement_n_data()

    def __len__(self):
        """
        Returns the number of used buckets (eg: count of data items) in the container
        """
        return self.n_data

    def __iter__(self):
        """
        Returns a generator for keys in the container
        """
        return (whole_record.key for whole_record in self._data
                if whole_record)

    def __repr__(self):
        return f"MyHashTable of size {self.utilization_status}"

    @property
    def n_data(self):
        """
        Returns the number of used buckets (eg: count of data items) in the container
        """
        return self._n_data

    def _increment_n_data(self):
        """
        Increments the count of used buckets in the container
        """
        self._n_data += 1

    def _decrement_n_data(self):
        """
        Decrements the count of used buckets in the container
        """
        self._n_data -= 1

    def _reset_n_data(self):
        self._n_data = 0

    @property
    def utilization_status(self):
        """
        Returns a string status showing the current utulization of the allotted buckets
        """
        return f"{len(self)} / {self.n_buckets} ({self.utilization * 100:.1f}%)"

    @property
    def n_buckets(self):
        """
        Returns the total number of data buckets allotted
        """
        return len(self._data)

    @property
    def utilization(self):
        """
        Returns the utilization fraction of the hash table

        Returns:
            (float): Utilization fraction (filled buckets / total buckets)
        """
        return len(self) / self.n_buckets

    def _key_to_bucket(self, key):
        """
        Returns a bucket index given a hashable key and the current state of the hash table

        This returns the first bucket found that is either already assigned to key or is empty.  Collisions are handled
        by incrementing the hashed key by 1.

        Returns KeyError if all buckets are searched without finding the correct key.

        Args:
            key (hashable): Key in the mappable

        Returns:
            (int): Index of the bucket used for
        """
        n_collisions = 0
        i_bucket = hash(key) % self.n_buckets
        while True:
            logger.debug(f"Looking for key {key} in bucket {i_bucket}")
            if self._data[i_bucket] is None:
                logger.debug(f"Found empty bucket {i_bucket} after {n_collisions} collisions")
                break
                # NEED TO RETURN
            elif self._data[i_bucket].key == key:
                logger.debug(f"Found correct bucket {i_bucket} after {n_collisions} collisions")
                break
            logger.debug(f"Found collision at bucket {i_bucket}")
            i_bucket = (i_bucket + 1) % self.n_buckets
            n_collisions += 1
            if n_collisions > len(self):
                # Key does not exist
                raise KeyError(key)
        self._log_collisions(n_collisions)
        return i_bucket

    def _initialize_data(self, n_buckets):
        """
        Initializes ._data to a given size with all elements == None.  This overwrites any existing data in ._data

        Args:
            n_buckets (int): Number of buckets allotted (eg: length of the internal list used for data storage)

        Returns:
            None
        """
        self._reset_n_data()
        self._data = [None] * n_buckets

    def _log_collisions(self, n):
        """
        Log the number of collisions for a single key_to_bucket

        Maintains a running total of all collisions as well as details about the state of the container at the
        time of logging this entry (should this accept n_buckets and len as args so it can log any arbitrary
        event, or should the emphasis be on the user to use this correctly?  As is now, calling with "n" doesn't
        truly mean it was activated with the n_buckets and len that actually get logged)

        Args:
            n (int): Number of collisions for a particular access event

        """
        collision = self._collision_log_template(self.n_buckets, len(self), n)
        self.collision_log.append(collision)
        self.n_collisions += n
        collision_to_logger(collision)

    def _resize_data(self, n_buckets):
        """
        Resizes self.data to the given number of buckets

        Data in original self.data is assigned to its new bucket location

        Raises ValueError if n_buckets is not large enough to hold all the data currently in self.data

        Args:
            n_buckets (int): Integer number of buckets
        """
        logger.debug(f"resize from {self.n_buckets} to {n_buckets}")
        if n_buckets < len(self):
            raise ValueError(f"Cannot resize - requested size {n_buckets} smaller than stored data {len(self)}")

        # To be a little faster, we could reuse the actual records and not the kv pairs...
        record_kvs = deque((record.key, record.value) for record in self._data if record is not None)

        # Reset data and size counter
        self._initialize_data(n_buckets)
        # Recommit data
        for k, v in record_kvs:
            self[k] = v
        logger.debug(f"resize complete - {self.utilization_status}")


# Helpers
def collision_to_logger(collision):
    logger.debug(f"collision report: {collision!r}")

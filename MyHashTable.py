from collections.abc import MutableMapping
from collections import namedtuple
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
formatter = logging.Formatter('%(name)s - %(funcName)20s() - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)


class MyHashTable(MutableMapping):

    def __init__(self, n_buckets=10, bucket_increment=10, max_utilization_fraction=0.7):
        self.n_buckets = n_buckets
        self.bucket_increment = bucket_increment
        self.max_utilization_fraction = max_utilization_fraction

        self._collision_log_template = namedtuple('collisionLog', 'n_buckets len collisions')
        self.collision_log = []
        self.n_collisions = 0

        self._data_record_template = namedtuple('dataRecord', 'key value')
        self.data = [None] * self.n_buckets

    def __setitem__(self, key, value):
        logger.debug(f"set {key}: {value}")
        i_bucket = self._key_to_bucket(key)
        self.data[i_bucket] = self._data_record_template(key, value)
        self.n_data += 1
        print("WARNING: setitem does not upsize self.data")

    def __getitem__(self, key):
        raise NotImplementedError()

    def __delitem__(self, key):
        raise NotImplementedError()
        self.n_data -= 1

    def __len__(self):
        return self.n_data

    def __iter__(self):
        raise NotImplementedError()

    def _key_to_bucket(self, key):
        """
        Returns a bucket index given a hashable key and the current state of the hash table

        This returns the first bucket found that is either already assigned to key or is empty

        Args:
            key (hashable): Key in the mappable

        Returns:
            (int): Index of the bucket used for
        """
        n_collisions = 0
        i_bucket = hash(key) % len(self)
        while self.data[i_bucket]['key'] != key:
            i_bucket = (i_bucket + 1) % len(self)
            n_collisions += 1
        self._log_collisions(n_collisions)

        return i_bucket

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
        raise NotImplementedError()


# Helpers
def collision_to_logger(collision):
    logger.debug(f"collision report: {collision:!r}")

# Custom hash table to understand how they work better
#
# Inspired by Fluent Python Ex 3-16 and related code
#
# Specs:
#
# * Mappable that accepts hashable keys and stores values associated with them
# * Should print on access/collision so I can see when hash collisions happen
# * must extend internal array when more than X % full
# * For hashing and binning, make use of the hash() function for hashing and then mod arithmatic to confine to correct
#   number of bins.
# * For hash collisions, do a simple +1 approach until we find the correct or empty bin

from collections.abc import MutableMapping
from collections import namedtuple
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
formatter = logging.Formatter('%(name)s - %(funcName)20s() - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)


class MyHashTable(MutableMapping):

    def __init__(self, n_buckets=10, bucket_increment=10, max_utilization_fraction=0.7, autoscale=True):
        self.bucket_increment = bucket_increment
        self.max_utilization_fraction = max_utilization_fraction
        self.n_data = 0
        self.autoscale = autoscale

        self._collision_log_template = namedtuple('collisionLog', 'n_buckets len collisions')
        self.collision_log = []
        self.n_collisions = 0

        self._data_record_template = namedtuple('dataRecord', 'key value')
        self.data = [None] * n_buckets

    def __setitem__(self, key, value):
        logger.debug(f"set {key}: {value} (start)")
        print("WARNING: setitem does not upsize self.data")
        # TODO: Resize first based on len+1, self.autoscale
        try:
            i_bucket = self._key_to_bucket(key)
        except KeyError:
            # KeyError means we could not find one of an empty bucket or bucket with this key
            raise ValueError("Cannot set {key}: {value}, HashTable is full!")
        data_added = self.data[i_bucket] is None
        self.data[i_bucket] = self._data_record_template(key, value)

        # Doing this after committing to .data just in case something goes wrong.  Not sure if an error there would
        # leave the n_data incremented incorrectly or not?
        if data_added:
            # Data is added to collection
            self.n_data += 1

        logger.debug(f"set {key}: {value} -> bucket {i_bucket} ({self.utilization_status})")

    def __getitem__(self, key):
        logger.debug(f"get {key}")
        i_bucket = self._key_to_bucket(key)
        whole_record = self.data[i_bucket]
        if whole_record is None:
            raise KeyError(key)
        value = whole_record.value
        logger.debug(f"get {whole_record.key}: {value} from bucket {i_bucket}")
        return value

    def __delitem__(self, key):
        raise NotImplementedError()
        self.n_data -= 1

    def __len__(self):
        return self.n_data

    def __iter__(self):
        raise NotImplementedError()

    def __repr__(self):
        return f"MyHashTable of size {self.utilization_status}"

    @property
    def utilization_status(self):
        return f"{len(self)} / {self.n_buckets} ({self.utilization*100:.1f}%)"

    @property
    def n_buckets(self):
        return len(self.data)

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
            if self.data[i_bucket] is None:
                logger.debug(f"Found empty bucket {i_bucket} after {n_collisions} collisions")
                break
                # NEED TO RETURN
            elif self.data[i_bucket].key == key:
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
    logger.debug(f"collision report: {collision!r}")

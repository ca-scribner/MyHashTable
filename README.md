# Summary

Custom hash table written to better understand how they work and also experiment with the frequency of collisions depending on how full the hash table is.  Inspired by Fluent Python Ex 3-16 and related code.

Implementation specs:

* Fully constituted Python MutableMapping that accepts hashable keys and stores values associated with them
* Should log collisions whenever we get/set so I can see how collisions vary with utilization
* must be self-resizing (if it fills up past some threshold, extend the internal store to reduce utilization)
* For hashing and binning, make use of the hash() function for hashing and then mod arithmatic to confine to correct
  number of bins.
* For hash collisions, do a simple +1 approach until we find the correct or empty bin

Example of usage included in myHashTable.ipynb

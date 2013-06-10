Trident-CF is a realtime highly scalable recommendation engine.
This library is built on top of [Storm](https://github.com/nathanmarz/storm), a distributed stream processing framework which runs on a cluster of machines and supports horizontal scaling.

This library implements a user-based incremental collaborative filtering algorithm with binary ratings (sometimes also called implicit ratings). 
The collaborative filtering algorithm is not based on any approximation method and gives the potential for high-quality recommendation formulation.

Note that Trident-CF is still in a beta phase and isn't production ready. It only lays the fundamental algorithm.

# Usage

Trident-CF is based on [Trident](https://github.com/nathanmarz/storm/wiki/Trident-tutorial), a high-level abstraction for doing realtime computing.
If you're familiar with high level batch processing tools like Pig or Cascading, the concepts of Trident will be very familiar.

It's recommended to read the [Storm and Trident documentation](https://github.com/nathanmarz/storm/wiki/Documentation).

TODO

# Collaborative Filtering State

TODO : supports only non-transactional and transactional state implementation
TOTO : MemoryCFState, RedisCFState

Trident-CF is a realtime highly scalable recommendation engine.
This library is built on top of [Storm](https://github.com/nathanmarz/storm), a distributed stream processing framework which runs on a cluster of machines and supports horizontal scaling.

This library implements the incremental collaborative filtering algorithm described in [Incremental Collaborative Filtering for Highly-
Scalable Recommendation Algorithms](http://dl.acm.org/citation.cfm?id=2140812) (Manos Papagelis, Ioannis Rousidis, Dimitris Plexousakis, Elias Theoharopoulos).


> Collaborative filtering requires expensive computations that grow polynomially with the number of users and items in the database. Methods proposed for handling this scalability problem and speeding up recommendation formulation are based on approximation mechanisms and, even if they improve performance, most of the time result in accuracy degradation. 
>
> [This algorithm] is based on incremental updates of user-to-user similarities. Our Incremental Collaborative Filtering algorithm is not based on any approximation method and gives the potential for high-quality recommendation formulation.
>
> [It] provides recommendations orders of magnitude faster than classic CF and thus, is suitable for online application.
>
> -- <cite>Manos Papagelis, Ioannis Rousidis, Dimitris Plexousakis, Elias Theoharopoulos</cite>


# Usage

Trident-ML is based on [Trident](https://github.com/nathanmarz/storm/wiki/Trident-tutorial), a high-level abstraction for doing realtime computing.
If you're familiar with high level batch processing tools like Pig or Cascading, the concepts of Trident will be very familiar.

It's recommended to read the [Storm and Trident documentation](https://github.com/nathanmarz/storm/wiki/Documentation).

TODO

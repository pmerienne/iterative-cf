Trident-CF is an highly scalable recommendation engine.
This library is built on top of [Storm](https://github.com/nathanmarz/storm), a distributed stream processing framework which runs on a cluster of machines and supports horizontal scaling.

This library implements a user-based collaborative filtering algorithm with binary ratings.

Note that Trident-CF is still in a beta phase and isn't production ready. It only lays the fundamental algorithm.

# Usage

Trident-CF is based on [Trident](https://github.com/nathanmarz/storm/wiki/Trident-tutorial), a high-level abstraction for doing realtime computing.
If you're familiar with high level batch processing tools like Pig or Cascading, the concepts of Trident will be very familiar.

It's recommended to read the [Storm and Trident documentation](https://github.com/nathanmarz/storm/wiki/Documentation).

## Build collaborative filtering topology
 
The Trident-CF algorithm is build over a TridentTopology and process a stream of binary ratings in order to measure similarity between users.
Use the [TridentCollaborativeFilteringBuilder](https://github.com/pmerienne/trident-cf/blob/master/src/main/java/com/github/pmerienne/trident/cf/TridentCollaborativeFilteringBuilder.java) to build the recommendation engine : 

```java
// Your trident topology
TridentTopology topology = ...;

// Stream which contain the binary ratings
Stream preferenceStream = ...;

// Stream which emit an empty tuple when user similarities must be re-build
Stream updateSimilaritiesStream = ...;

// Create collaborative filtering topology
TridentCollaborativeFiltering tcf = new TridentCollaborativeFilteringBuilder().use(topology).process(preferenceStream).updateSimilaritiesOn(updateSimilaritiesStream).build();
```

Note that the preference stream must contain at least the 2 fields ("user" and "item") while the update similarities stream doesn't need any field.

Trident-CF provides 2 spouts implementations which can be used to create the update similarities stream : DelayedSimilaritiesUpdateLauncher and PermanentSimilaritiesUpdateLauncher.

## Get item recommendations

Item recommendations are generated by aggregating preferences of the most similar users.
Here's the code to process a recommendation query stream to retrieve item recommendations : 


```java
// recommendations parameters
int nbItems = 10;
int neighborhoodSize = 100;

// Your trident topology
TridentTopology topology = ...;

// Stream which contain the binary ratings
Stream preferenceStream = ...;

// Stream which emit an empty tuple when user similarities must be re-build
Stream updateSimilaritiesStream = ...;

// Stream containing recommendation queries
Stream recommendationQueryStream = ...;

// Create collaborative filtering topology
TridentCollaborativeFiltering cf = new TridentCollaborativeFilteringBuilder().use(topology).process(preferenceStream).updateSimilaritiesOn(updateSimilaritiesStream).build();
Stream recommendationStream = cf.createItemRecommendationStream(recommendationQueryStream, nbItems, neighborhoodSize);

```

The recommendation query stream must contain a "user" field containing a user id (long).
The new stream (recommendationStream) contains a single field ("recommendedItems") containing a List of RecommendedItem.

# Configure the trident collaborative topology
TODO

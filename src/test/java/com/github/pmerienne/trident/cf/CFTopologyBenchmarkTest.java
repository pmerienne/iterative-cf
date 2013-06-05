package com.github.pmerienne.trident.cf;

import org.junit.Test;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;

import com.github.pmerienne.trident.cf.CFTopology.Options;
import com.github.pmerienne.trident.cf.state.NonTransactionalRedisCFState;
import com.github.pmerienne.trident.cf.testing.RandomRatingsSpout;

public class CFTopologyBenchmarkTest {

	@Test
	public void benchmark() throws InterruptedException {
		int batchSize = 5;
		int testDuration = 240;
		int nbUsers = 500;
		int nbItems = 500;

		Options options = new Options();
		options.updateUserCacheParallelism = 5;
		options.fetchUsersParallelism = 5;
		options.updateUserPairCacheParallelism = 10;
		options.updateSimilarityParallelism = 20;
		 options.cfStateFactory = new NonTransactionalRedisCFState.Factory();
		double localRedisStateRatingsPerSecond = this.benchmark(options, batchSize, testDuration, nbUsers, nbItems);

		// System.out.println("In Memory : " + inMemoryStateRatingsPerSecond +
		// " ratings/s");
		System.out.println("Local redis : " + localRedisStateRatingsPerSecond + " ratings/s");
	}

	protected double benchmark(Options options, int batchSize, int testDuration, int nbUsers, int nbItems) throws InterruptedException {
		// Start local cluster
		LocalCluster cluster = new LocalCluster();

		double completedBatchCount = 0.0;
		try {
			// Build topology
			TridentTopology topology = new TridentTopology();
			RandomRatingsSpout ratingsSpout = new RandomRatingsSpout(batchSize, nbUsers, nbItems);

			// Create ratings stream
			Stream ratingStream = topology.newStream("ratings", ratingsSpout).parallelismHint(options.updateUserCacheParallelism);

			// Create collaborative filtering topology with an in memory CF
			// state
			new CFTopology(ratingStream, options);

			// Submit and wait topology
			cluster.submitTopology(this.getClass().getSimpleName(), new Config(), topology.build());
			Thread.sleep(testDuration * 1000);

			// Check expected similarity
			completedBatchCount = ratingsSpout.getAndResetCompletedBatchCount();
		} finally {
			cluster.shutdown();
		}
		return completedBatchCount * batchSize / testDuration;
	}
}

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
		int batchSize = 100;
		int testDuration = 60;

		Options inMemoryOptions = new Options();
		long inMemoryStateRatingsPerSecond = this.benchmark(inMemoryOptions, batchSize, testDuration);

		Options localRedisOptions = new Options();
		localRedisOptions.cfStateFactory = new NonTransactionalRedisCFState.Factory();
		long localRedisStateRatingsPerSecond = this.benchmark(localRedisOptions, batchSize, testDuration);

		System.out.println("In Memory : " + inMemoryStateRatingsPerSecond + " ratings/s");
		System.out.println("Local redis : " + localRedisStateRatingsPerSecond + " ratings/s");
	}

	protected long benchmark(Options options, int batchSize, int testDuration) throws InterruptedException {
		// Start local cluster
		LocalCluster cluster = new LocalCluster();
		LocalDRPC localDRPC = new LocalDRPC();

		long emitedBatchCount = 0;
		try {
			// Build topology
			TridentTopology topology = new TridentTopology();
			RandomRatingsSpout ratingsSpout = new RandomRatingsSpout(batchSize, 500, 500);

			// Create ratings stream
			Stream ratingStream = topology.newStream("ratings", ratingsSpout);

			// Create collaborative filtering topology with an in memory CF
			// state
			new CFTopology(ratingStream, options);

			// Submit and wait topology
			cluster.submitTopology(this.getClass().getSimpleName(), new Config(), topology.build());
			Thread.sleep(testDuration * 1000);

			// Check expected similarity
			emitedBatchCount = ratingsSpout.getAndResetCompletedBatchCount();
		} finally {
			cluster.shutdown();
			localDRPC.shutdown();
		}
		return emitedBatchCount * batchSize / testDuration;
	}
}

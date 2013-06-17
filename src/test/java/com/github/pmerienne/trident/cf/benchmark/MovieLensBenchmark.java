package com.github.pmerienne.trident.cf.benchmark;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;

import com.github.pmerienne.trident.cf.TridentCollaborativeFiltering;
import com.github.pmerienne.trident.cf.TridentCollaborativeFiltering.Options;
import com.github.pmerienne.trident.cf.testing.MovieLensPreferencesSpout;
import com.github.pmerienne.trident.cf.testing.SimilaritiesUpdateTracker;

public class MovieLensBenchmark {

	private static final Logger LOGGER = LoggerFactory.getLogger(MovieLensBenchmark.class);
	private static final String TOPOLOGY_NAME = MovieLensBenchmark.class.getSimpleName();

	private LocalCluster cluster;

	@Before
	public void setupCluster() {
		this.cluster = new LocalCluster();
	}

	@After
	public void tearDownCluster() {
		this.cluster.shutdown();
	}

	@Test
	public void benchmarkMemoryStateWithBigBatchs() throws InterruptedException {
		int batchSize = 200;
		Options options = Options.inMemory();
		this.benchmark(options, batchSize);
	}

	@Test
	public void benchmarkRedisStateWithBigBatchs() throws InterruptedException {
		int batchSize = 200;
		Options options = Options.redis();
		this.benchmark(options, batchSize);
	}

	protected void benchmark(Options options, int batchSize) throws InterruptedException {
		TridentTopology topology = new TridentTopology();

		MovieLensPreferencesSpout preferencesSpout = new MovieLensPreferencesSpout(batchSize);
		SimilaritiesUpdateTracker similaritiesUpdateTracker = new SimilaritiesUpdateTracker();
		Stream preferenceStream = topology.newStream("preferences", preferencesSpout);
		Stream updateSimilaritiesStream = topology.newStream(null, similaritiesUpdateTracker);

		// Create collaborative filtering topology
		TridentCollaborativeFiltering cf = new TridentCollaborativeFiltering(topology, options);
		cf.appendCollaborativeFilteringTopology(preferenceStream, updateSimilaritiesStream);

		// Submit topology
		this.cluster.submitTopology(TOPOLOGY_NAME, new Config(), topology.build());

		// Load preferences
		long previous = System.currentTimeMillis();
		while (!preferencesSpout.finished()) {
			Utils.sleep(200);
		}
		long elapsed = (System.currentTimeMillis() - previous) / 1000;
		LOGGER.info("All preferences loaded in " + elapsed + "s");

		// Compute similarities
		similaritiesUpdateTracker.activate();
		while (!similaritiesUpdateTracker.finished()) {
			Utils.sleep(200);
		}
		elapsed = similaritiesUpdateTracker.getElapsedSeconds();
		LOGGER.info("Similarities computed in " + elapsed + "s");
	}
}

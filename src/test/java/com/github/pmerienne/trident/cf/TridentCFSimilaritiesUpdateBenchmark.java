/**
 * Copyright 2013-2015 Pierre Merienne
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.pmerienne.trident.cf;

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

import com.github.pmerienne.trident.cf.TridentCollaborativeFiltering.Options;
import com.github.pmerienne.trident.cf.testing.AllBinaryPreferencesSpout;
import com.github.pmerienne.trident.cf.testing.SimilaritiesUpdateTracker;

public class TridentCFSimilaritiesUpdateBenchmark {

	private static final Logger LOGGER = LoggerFactory.getLogger(TridentCFSimilaritiesUpdateBenchmark.class);
	private static final String TOPOLOGY_NAME = TridentCFSimilaritiesUpdateBenchmark.class.getSimpleName();

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
		int nbUsers = 100;
		int nbItems = 100;
		Options options = Options.inMemory();

		long elapased = this.benchmark(options, batchSize, nbUsers, nbItems);
		System.out.println("Similarities for " + nbUsers + " users and " + nbItems + " items build in " + elapased + "seconds");
	}

	@Test
	public void benchmarkRedisStateWithBigBatchs() throws InterruptedException {
		int batchSize = 200;
		int nbUsers = 100;
		int nbItems = 100;
		Options options = Options.redis();

		long elapased = this.benchmark(options, batchSize, nbUsers, nbItems);
		System.out.println("Similarities for " + nbUsers + " users and " + nbItems + " items build in " + elapased + "seconds");
	}

	protected long benchmark(Options options, int batchSize, int nbUsers, int nbItems) throws InterruptedException {
		TridentTopology topology = new TridentTopology();

		AllBinaryPreferencesSpout preferencesSpout = new AllBinaryPreferencesSpout(batchSize, nbUsers, nbItems);
		SimilaritiesUpdateTracker similaritiesUpdateTracker = new SimilaritiesUpdateTracker();
		Stream preferenceStream = topology.newStream("preferences", preferencesSpout);
		Stream updateSimilaritiesStream = topology.newStream(null, similaritiesUpdateTracker);

		// Create collaborative filtering topology
		TridentCollaborativeFiltering cf = new TridentCollaborativeFiltering(topology, options);
		cf.appendCollaborativeFilteringTopology(preferenceStream, updateSimilaritiesStream);

		// Submit topology
		this.cluster.submitTopology(TOPOLOGY_NAME, new Config(), topology.build());

		// Wait for all preferences
		while (!preferencesSpout.finished()) {
			Utils.sleep(200);
		}

		// Start updating similarities and wait
		LOGGER.info("All preferences loaded, starting similarities update ...");
		similaritiesUpdateTracker.activate();
		while (!similaritiesUpdateTracker.finished()) {
			Utils.sleep(200);
		}
		LOGGER.info("Update finished ...");

		return similaritiesUpdateTracker.getElapsedSeconds();
	}
}

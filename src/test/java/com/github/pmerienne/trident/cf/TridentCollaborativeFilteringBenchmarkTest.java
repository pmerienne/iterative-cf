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

import org.junit.Test;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;

import com.github.pmerienne.trident.cf.TridentCollaborativeFiltering.Options;
import com.github.pmerienne.trident.cf.state.RedisCFState;
import com.github.pmerienne.trident.cf.testing.RandomBinaryPreferencesSpout;

public class TridentCollaborativeFilteringBenchmarkTest {

	@Test
	public void benchmarkMemoryStateWithSmallBatchs() throws InterruptedException {
		int batchSize = 10;
		long duration = 10;
		int nbUsers = 100;
		int nbItems = 1000000;
		Options options = new Options();

		double completedBatchs = this.benchmark(options, batchSize, duration, nbUsers, nbItems);
		double throughput = completedBatchs * batchSize / duration;
		System.out.println("Throughput : " + throughput + " preferences/s");
	}

	@Test
	public void benchmarkMemoryStateWithMediumBatchs() throws InterruptedException {
		int batchSize = 50;
		long duration = 10;
		int nbUsers = 100;
		int nbItems = 100000;
		Options options = new Options();

		double completedBatchs = this.benchmark(options, batchSize, duration, nbUsers, nbItems);
		double throughput = completedBatchs * batchSize / duration;
		System.out.println("Throughput : " + throughput + " preferences/s");
	}

	@Test
	public void benchmarkRedisStateWithSmallBatchs() throws InterruptedException {
		int batchSize = 10;
		long duration = 10;
		int nbUsers = 100;
		int nbItems = 1000000;
		Options options = new Options();
		options.cfStateFactory = new RedisCFState.Factory();

		double completedBatchs = this.benchmark(options, batchSize, duration, nbUsers, nbItems);
		double throughput = completedBatchs * batchSize / duration;
		System.out.println("Throughput : " + throughput + " preferences/s");
	}

	@Test
	public void benchmarkRedisStateWithMediumBatchs() throws InterruptedException {
		int batchSize = 50;
		long duration = 10;
		int nbUsers = 100;
		int nbItems = 100000;
		Options options = new Options();
		options.cfStateFactory = new RedisCFState.Factory();

		double completedBatchs = this.benchmark(options, batchSize, duration, nbUsers, nbItems);
		double throughput = completedBatchs * batchSize / duration;
		System.out.println("Throughput : " + throughput + " preferences/s");
	}

	@Test
	public void benchmarkRedisStateWithBigBatchs() throws InterruptedException {
		int batchSize = 500;
		long duration = 10;
		int nbUsers = 100;
		int nbItems = 100000;
		Options options = new Options();
		options.cfStateFactory = new RedisCFState.Factory();

		double completedBatchs = this.benchmark(options, batchSize, duration, nbUsers, nbItems);
		double throughput = completedBatchs * batchSize / duration;
		System.out.println("Throughput : " + throughput + " preferences/s");
	}

	protected long benchmark(Options options, int batchSize, long duration, int nbUsers, int nbItems) throws InterruptedException {
		long completedBatchs = 0;
		// Start local cluster
		LocalCluster cluster = new LocalCluster();
		LocalDRPC localDRPC = new LocalDRPC();

		try {
			TridentTopology topology = new TridentTopology();

			RandomBinaryPreferencesSpout preferencesSpout = new RandomBinaryPreferencesSpout(batchSize, nbUsers, nbItems);
			Stream ratingStream = topology.newStream("preferences", preferencesSpout);

			TridentCollaborativeFiltering cf = new TridentCollaborativeFiltering(options);
			cf.initSimilarityTopology(topology, ratingStream);

			// Submit and wait topology
			cluster.submitTopology(this.getClass().getSimpleName(), new Config(), topology.build());
			Thread.sleep(duration * 1000);

			completedBatchs = preferencesSpout.getCompletedBatchCount();
			RandomBinaryPreferencesSpout.resetStaticCounts();
		} finally {
			cluster.shutdown();
			localDRPC.shutdown();
		}

		return completedBatchs;
	}
}

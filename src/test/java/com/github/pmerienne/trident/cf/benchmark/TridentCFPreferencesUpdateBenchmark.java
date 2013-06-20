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
package com.github.pmerienne.trident.cf.benchmark;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;

import com.github.pmerienne.trident.cf.TridentCollaborativeFiltering.Options;
import com.github.pmerienne.trident.cf.TridentCollaborativeFilteringBuilder;
import com.github.pmerienne.trident.cf.testing.RandomBinaryPreferencesSpout;

public class TridentCFPreferencesUpdateBenchmark {

	private static final String TOPOLOGY_NAME = TridentCFPreferencesUpdateBenchmark.class.getSimpleName();

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
	public void benchmarkMemoryStateWithSmallBatchs() throws InterruptedException {
		int batchSize = 10;
		long duration = 10;
		int nbUsers = 1000;
		int nbItems = 10000;
		Options options = Options.inMemory();

		double completedBatchs = this.benchmark(options, batchSize, duration, nbUsers, nbItems);
		double throughput = completedBatchs * batchSize / duration;
		System.out.println("Throughput : " + throughput + " preferences/s");
	}

	@Test
	public void benchmarkMemoryStateWithMediumBatchs() throws InterruptedException {
		int batchSize = 50;
		long duration = 10;
		int nbUsers = 1000;
		int nbItems = 10000;
		Options options = Options.inMemory();

		double completedBatchs = this.benchmark(options, batchSize, duration, nbUsers, nbItems);
		double throughput = completedBatchs * batchSize / duration;
		System.out.println("Throughput : " + throughput + " preferences/s");
	}

	@Test
	public void benchmarkMemoryStateWithBigBatchs() throws InterruptedException {
		int batchSize = 200;
		long duration = 10;
		int nbUsers = 1000;
		int nbItems = 10000;
		Options options = Options.inMemory();

		double completedBatchs = this.benchmark(options, batchSize, duration, nbUsers, nbItems);
		double throughput = completedBatchs * batchSize / duration;
		System.out.println("Throughput : " + throughput + " preferences/s");
	}

	@Test
	public void benchmarkRedisStateWithSmallBatchs() throws InterruptedException {
		int batchSize = 10;
		long duration = 10;
		int nbUsers = 1000;
		int nbItems = 10000;
		Options options = Options.redis();

		double completedBatchs = this.benchmark(options, batchSize, duration, nbUsers, nbItems);
		double throughput = completedBatchs * batchSize / duration;
		System.out.println("Throughput : " + throughput + " preferences/s");
	}

	@Test
	public void benchmarkRedisStateWithMediumBatchs() throws InterruptedException {
		int batchSize = 50;
		long duration = 10;
		int nbUsers = 1000;
		int nbItems = 10000;
		Options options = Options.redis();

		double completedBatchs = this.benchmark(options, batchSize, duration, nbUsers, nbItems);
		double throughput = completedBatchs * batchSize / duration;
		System.out.println("Throughput : " + throughput + " preferences/s");
	}

	@Test
	public void benchmarkRedisStateWithBigBatchs() throws InterruptedException {
		int batchSize = 200;
		long duration = 10;
		int nbUsers = 1000;
		int nbItems = 10000;
		Options options = Options.redis();

		double completedBatchs = this.benchmark(options, batchSize, duration, nbUsers, nbItems);
		double throughput = completedBatchs * batchSize / duration;
		System.out.println("Throughput : " + throughput + " preferences/s");
	}

	protected long benchmark(Options options, int batchSize, long duration, int nbUsers, int nbItems) throws InterruptedException {
		long completedBatchs = 0;

		TridentTopology topology = new TridentTopology();

		RandomBinaryPreferencesSpout preferencesSpout = new RandomBinaryPreferencesSpout(batchSize, nbUsers, nbItems);
		Stream preferenceStream = topology.newStream("preferences", preferencesSpout);

		// Create collaborative filtering topology
		TridentCollaborativeFilteringBuilder builder = new TridentCollaborativeFilteringBuilder();
		builder.use(topology).with(options).process(preferenceStream).build();

		// Submit and wait topology
		cluster.submitTopology(TOPOLOGY_NAME, new Config(), topology.build());
		Thread.sleep(duration * 1000);

		completedBatchs = preferencesSpout.getCompletedBatchCount();
		RandomBinaryPreferencesSpout.resetStaticCounts();

		return completedBatchs;
	}
}

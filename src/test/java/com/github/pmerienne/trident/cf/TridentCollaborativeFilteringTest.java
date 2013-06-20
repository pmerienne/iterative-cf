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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.builtin.PermanentSimilaritiesUpdateLauncher;
import com.github.pmerienne.trident.cf.testing.DRPCUtils;

public class TridentCollaborativeFilteringTest {

	@Test
	public void testSimilarityUpdate() throws InterruptedException {
		// Start local cluster
		LocalCluster cluster = new LocalCluster();

		try {
			// Build topology
			TridentTopology topology = new TridentTopology();

			// Create rating spout
			Values[] ratings = new Values[8];
			ratings[0] = new Values(0L, 10L);
			ratings[1] = new Values(1L, 10L);
			ratings[2] = new Values(2L, 12L);
			ratings[3] = new Values(3L, 13L);
			ratings[4] = new Values(1L, 14L);

			ratings[5] = new Values(0L, 15L);
			ratings[6] = new Values(1L, 16L);
			ratings[7] = new Values(2L, 13L);
			FixedBatchSpout ratingsSpout = new FixedBatchSpout(new Fields(TridentCollaborativeFiltering.USER_FIELD, TridentCollaborativeFiltering.ITEM_FIELD), 5, ratings);

			// Create needed streams
			Stream preferenceStream = topology.newStream("preferences", ratingsSpout);
			Stream updateSimilaritiesStream = topology.newStream(null, new PermanentSimilaritiesUpdateLauncher());

			// Create collaborative filtering topology
			TridentCollaborativeFilteringBuilder builder = new TridentCollaborativeFilteringBuilder();
			builder.use(topology).process(preferenceStream).updateSimilaritiesOn(updateSimilaritiesStream).build();

			// Submit and wait topology
			cluster.submitTopology(this.getClass().getSimpleName(), new Config(), topology.build());
			Thread.sleep(8000);
		} finally {
			cluster.shutdown();
		}
	}

	@Test
	public void testUserSimilarity() throws InterruptedException {
		// Start local cluster
		LocalCluster cluster = new LocalCluster();
		LocalDRPC localDRPC = new LocalDRPC();

		try {
			// Build topology
			TridentTopology topology = new TridentTopology();

			// Create rating spout
			Values[] ratings = new Values[10];
			ratings[0] = new Values(0L, 0L);
			ratings[1] = new Values(0L, 1L);
			ratings[2] = new Values(0L, 2L);
			ratings[3] = new Values(0L, 3L);

			ratings[4] = new Values(1L, 2L);
			ratings[5] = new Values(1L, 3L);
			ratings[6] = new Values(1L, 4L);

			ratings[7] = new Values(2L, 4L);
			ratings[8] = new Values(2L, 5L);
			ratings[9] = new Values(2L, 6L);
			FixedBatchSpout ratingsSpout = new FixedBatchSpout(new Fields(TridentCollaborativeFiltering.USER_FIELD, TridentCollaborativeFiltering.ITEM_FIELD), 5, ratings);

			// Create needed streams
			Stream preferenceStream = topology.newStream("preferences", ratingsSpout);
			Stream updateSimilaritiesStream = topology.newStream(null, new PermanentSimilaritiesUpdateLauncher());
			Stream similarityQueryStream = topology.newDRPCStream("userSimilarity", localDRPC).each(new Fields("args"), new ExtractUsers(),
					new Fields(TridentCollaborativeFiltering.USER_FIELD, TridentCollaborativeFiltering.USER2_FIELD));

			// Create collaborative filtering topology
			TridentCollaborativeFilteringBuilder builder = new TridentCollaborativeFilteringBuilder();
			TridentCollaborativeFiltering cf = builder.use(topology).process(preferenceStream).updateSimilaritiesOn(updateSimilaritiesStream).build();
			cf.createUserSimilarityStream(similarityQueryStream);

			// Submit and wait topology
			cluster.submitTopology(this.getClass().getSimpleName(), new Config(), topology.build());
			Thread.sleep(8000);

			// Check expected similarities
			double actualSimilarity01 = (Double) DRPCUtils.extractSingleValue(localDRPC.execute("userSimilarity", "0 1"));
			double actualSimilarity12 = (Double) DRPCUtils.extractSingleValue(localDRPC.execute("userSimilarity", "1 2"));
			double actualSimilarity02 = (Double) DRPCUtils.extractSingleValue(localDRPC.execute("userSimilarity", "0 2"));
			assertTrue(actualSimilarity01 > actualSimilarity12);
			assertEquals(0.0, actualSimilarity02, 10e-3);
		} finally {
			cluster.shutdown();
			localDRPC.shutdown();
		}
	}

	@Test
	public void testItemRecommendation() throws InterruptedException {
		// Start local cluster
		LocalCluster cluster = new LocalCluster();
		LocalDRPC localDRPC = new LocalDRPC();

		try {
			// Build topology
			TridentTopology topology = new TridentTopology();

			// Create rating spout
			Values[] ratings = new Values[12];
			ratings[0] = new Values(0L, 0L);
			ratings[1] = new Values(0L, 2L);
			ratings[2] = new Values(0L, 8L);

			ratings[3] = new Values(1L, 1L);
			ratings[4] = new Values(1L, 3L);
			ratings[5] = new Values(1L, 5L);
			ratings[6] = new Values(1L, 7L);

			ratings[7] = new Values(2L, 0L);
			ratings[8] = new Values(2L, 2L);
			ratings[9] = new Values(2L, 4L);
			ratings[10] = new Values(2L, 6L);
			ratings[11] = new Values(2L, 8L);
			FixedBatchSpout ratingsSpout = new FixedBatchSpout(new Fields(TridentCollaborativeFiltering.USER_FIELD, TridentCollaborativeFiltering.ITEM_FIELD), 5, ratings);

			// Create needed streams
			Stream preferenceStream = topology.newStream("preferences", ratingsSpout);
			Stream updateSimilaritiesStream = topology.newStream(null, new PermanentSimilaritiesUpdateLauncher());
			Stream recommendationQueryStream = topology.newDRPCStream("recommendation", localDRPC).each(new Fields("args"), new ExtractUser(), new Fields(TridentCollaborativeFiltering.USER_FIELD));

			// Create collaborative filtering topology
			TridentCollaborativeFilteringBuilder builder = new TridentCollaborativeFilteringBuilder();
			TridentCollaborativeFiltering cf = builder.use(topology).process(preferenceStream).updateSimilaritiesOn(updateSimilaritiesStream).build();
			cf.createItemRecommendationStream(recommendationQueryStream, 2, 10);

			// Submit and wait topology
			cluster.submitTopology(this.getClass().getSimpleName(), new Config(), topology.build());
			Thread.sleep(8000);

			// Check expected similarities
			List<Long> recommendedItems = extractRecommendedItems(localDRPC.execute("recommendation", "0"));
			assertTrue(recommendedItems.contains(6L));
			assertTrue(recommendedItems.contains(4L));
		} finally {
			cluster.shutdown();
			localDRPC.shutdown();
		}
	}

	/**
	 * Sorry for this! It could be better to use some REGEX
	 * 
	 * @param drpcResult
	 * @return
	 */
	protected static List<Long> extractRecommendedItems(String drpcResult) {
		List<Long> recommendedItems = new ArrayList<Long>();

		int index = -1;
		do {
			index = drpcResult.indexOf("RecommendedItem [item=", index + 1);
			if (index != -1) {
				long item = Long.parseLong(drpcResult.substring(index + 22, index + 23));
				recommendedItems.add(item);
			}
		} while (index != -1);
		return recommendedItems;
	}

	private static class ExtractUsers extends BaseFunction {

		private static final long serialVersionUID = 7171566985006542069L;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String[] args = tuple.getString(0).split(" ");
			long user1 = Long.parseLong(args[0]);
			long user2 = Long.parseLong(args[1]);
			collector.emit(new Values(user1, user2));
		}
	}

	private static class ExtractUser extends BaseFunction {

		private static final long serialVersionUID = 1863834953816610484L;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String arg = tuple.getString(0);
			long user = Long.parseLong(arg);
			collector.emit(new Values(user));
		}
	}
}

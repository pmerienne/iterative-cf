package com.github.pmerienne.trident.cf;

import org.junit.Test;

import com.github.pmerienne.trident.cf.CFTopology;
import com.github.pmerienne.trident.cf.state.InMemoryCFState;
import com.github.pmerienne.trident.cf.state.InMemoryCFState.Factory;

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

public class CFTopologyTest {

	@Test
	public void testInTopology() throws InterruptedException {
		// Start local cluster
		LocalCluster cluster = new LocalCluster();
		LocalDRPC localDRPC = new LocalDRPC();

		try {
			// Build topology
			TridentTopology topology = new TridentTopology();

			// Create rating stream from DRPC call
			Values[] ratings = new Values[10];
			ratings[0] = new Values(0L, 0L, 0.0);
			ratings[1] = new Values(0L, 1L, 0.5);
			ratings[2] = new Values(0L, 2L, 0.9);
			ratings[3] = new Values(0L, 3L, 0.6);

			ratings[4] = new Values(1L, 0L, 0.1);
			ratings[5] = new Values(1L, 1L, 0.4);
			ratings[6] = new Values(1L, 3L, 0.7);

			ratings[7] = new Values(2L, 0L, 0.8);
			ratings[8] = new Values(2L, 2L, 0.2);
			ratings[9] = new Values(2L, 3L, 0.1);

			Stream ratingStream = topology.newStream("ratings", new FixedBatchSpout(new Fields(CFTopology.DEFAULT_USER1_FIELD, CFTopology.DEFAULT_ITEM_FIELD, CFTopology.DEFAULT_RATING_FIELD), 5,
					ratings));
			Stream similarityQueryStream = topology.newDRPCStream("userSimilarity", localDRPC).each(new Fields("args"), new ExtractUsers(),
					new Fields(CFTopology.DEFAULT_USER1_FIELD, CFTopology.DEFAULT_USER2_FIELD));
			Stream recommendationQueryStream = topology.newDRPCStream("recommendedItems", localDRPC).each(new Fields("args"), new ExtractUser(), new Fields(CFTopology.DEFAULT_USER1_FIELD));

			CFTopology cfTopology = new CFTopology(ratingStream, new InMemoryCFState.Factory());
			cfTopology.createUserSimilarityStream(similarityQueryStream);
			cfTopology.createRecommendationStream(recommendationQueryStream);

			// Wait for the topology creation
			cluster.submitTopology(this.getClass().getSimpleName(), new Config(), topology.build());

			Thread.sleep(5000);

			//
			System.out.println("### Similarity");
			System.out.println(localDRPC.execute("userSimilarity", "0 1"));
			System.out.println(localDRPC.execute("userSimilarity", "1 2"));
			System.out.println(localDRPC.execute("userSimilarity", "0 2"));

			/**
			 * [[0.8320502943378436]] [[-0.9728062146853667]]
			 * [[-0.8934051474415642]]
			 */

			System.out.println("### Similarity");
			System.out.println(localDRPC.execute("recommendedItems", "0"));
			System.out.println(localDRPC.execute("recommendedItems", "1"));
			System.out.println(localDRPC.execute("recommendedItems", "2"));

			System.out.println("### END");
		} finally {
			cluster.shutdown();
			localDRPC.shutdown();
		}
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

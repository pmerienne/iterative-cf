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

import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class Sandbox {

	@Test
	public void test() throws InterruptedException {
		// Start local cluster
		LocalCluster cluster = new LocalCluster();
		LocalDRPC localDRPC = new LocalDRPC();

		try {
			// Build topology
			TridentTopology topology = new TridentTopology();
			
			// Create rating spout
			Values[] ratings = new Values[4];
			ratings[0] = new Values(0L, 1L, 1L);
			ratings[1] = new Values(0L, 2L, 1L);
			ratings[2] = new Values(0L, 1L, 1L);
			ratings[3] = new Values(0L, 2L, 2L);
			FixedBatchSpout ratingsSpout = new FixedBatchSpout(new Fields("user1", "user2", "item"), 5, ratings);

			topology.newStream("ratings", ratingsSpout)
			.each(new Fields("user1", "user2", "item"), new Debug("Before"))
			.groupBy(new Fields("user1", "user2", "item"))
			.aggregate(new Fields("user1", "user2", "item"), new KeepFirst(), new Fields("unique"))
			.each(new Fields("unique"), new Debug("unique!"));
			
			
			// Submit and wait topology
			cluster.submitTopology(this.getClass().getSimpleName(), new Config(), topology.build());
			Thread.sleep(8000);

		} finally {
			cluster.shutdown();
			localDRPC.shutdown();
		}
	}
}

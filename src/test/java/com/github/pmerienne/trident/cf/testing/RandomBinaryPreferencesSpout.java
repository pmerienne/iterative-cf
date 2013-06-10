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
package com.github.pmerienne.trident.cf.testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.TridentCollaborativeFiltering;

@SuppressWarnings("rawtypes")
public class RandomBinaryPreferencesSpout implements IBatchSpout {

	private static final long serialVersionUID = -621257332363277006L;

	private final Random random = new Random();

	private int batchSize = 10;
	private int nbUsers = 100;
	private int nbItems = 100;

	private static Map<Long, List<Long>> pastPreferences = new ConcurrentHashMap<Long, List<Long>>();

	private static AtomicLong completedBatchCount = new AtomicLong(0);

	public RandomBinaryPreferencesSpout() {
	}

	public RandomBinaryPreferencesSpout(int batchSize, int nbUsers, int nbItems) {
		this.batchSize = batchSize;
		this.nbUsers = nbUsers;
		this.nbItems = nbItems;
	}

	@Override
	public void open(Map conf, TopologyContext context) {
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		long user, item;
		for (int i = 0; i < this.batchSize; i++) {
			long[] randomPreference = this.randomPreference();
			user = randomPreference[0];
			item = randomPreference[1];
			collector.emit(new Values(user, item));
		}
	}

	private long[] randomPreference() {
		long user;
		List<Long> userPastPreferences;
		do {
			user = this.random.nextInt(this.nbUsers);
			userPastPreferences = pastPreferences.get(user);
			if (userPastPreferences == null) {
				userPastPreferences = new ArrayList<Long>();
			}
		} while (userPastPreferences.size() >= this.nbItems);

		long item;
		do {
			item = this.random.nextInt(this.nbItems);
		} while (userPastPreferences.contains(item));

		userPastPreferences.add(item);
		pastPreferences.put(user, userPastPreferences);

		return new long[] { user, item };
	}

	@Override
	public void ack(long batchId) {
		completedBatchCount.incrementAndGet();
	}

	@Override
	public void close() {
	}

	@Override
	public Map getComponentConfiguration() {
		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		return conf;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields(TridentCollaborativeFiltering.USER_FIELD, TridentCollaborativeFiltering.ITEM_FIELD);
	}

	public long getCompletedBatchCount() {
		long completed = completedBatchCount.get();
		return completed;
	}

	public static void resetStaticCounts() {
		completedBatchCount.set(0);
		pastPreferences = new HashMap<Long, List<Long>>();
	}

}

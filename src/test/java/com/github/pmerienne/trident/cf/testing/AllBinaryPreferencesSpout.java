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

import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.TridentCollaborativeFiltering;

@SuppressWarnings("rawtypes")
public class AllBinaryPreferencesSpout implements IBatchSpout {

	private static final long serialVersionUID = -180727442291490144L;

	private int batchSize = 10;
	private int nbUsers = 100;
	private int nbItems = 100;

	private static int i = 0;

	public AllBinaryPreferencesSpout() {
	}

	public AllBinaryPreferencesSpout(int batchSize, int nbUsers, int nbItems) {
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

		int k = 0;
		while (k < this.batchSize && (i < this.nbUsers * this.nbItems)) {
			user = i % this.nbItems;
			item = (i - user) / this.nbItems;
			collector.emit(new Values(user, item));
			i++;
			k++;
		}
	}

	@Override
	public void ack(long batchId) {
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

	public boolean finished() {
		return i >= this.nbUsers * this.nbItems;
	}
}

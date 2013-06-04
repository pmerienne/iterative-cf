package com.github.pmerienne.trident.cf.testing;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.CFTopology;

@SuppressWarnings("rawtypes")
public class RandomRatingsSpout implements IBatchSpout {

	private static final long serialVersionUID = -621257332363277006L;

	private final Random random = new Random();

	private int batchSize = 10;
	private int nbUsers = 100;
	private int nbItems = 100;

	private static AtomicLong completedBatchCount = new AtomicLong(0);

	public RandomRatingsSpout() {
	}

	public RandomRatingsSpout(int batchSize, int nbUsers, int nbItems) {
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
		double rating;
		for (int i = 0; i < this.batchSize; i++) {
			user = this.random.nextInt(this.nbUsers);
			item = this.random.nextInt(this.nbItems);
			rating = this.random.nextDouble();
			collector.emit(new Values(user, item, rating));
		}
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
		return new Fields(CFTopology.DEFAULT_USER1_FIELD, CFTopology.DEFAULT_ITEM_FIELD, CFTopology.DEFAULT_RATING_FIELD);
	}

	public long getAndResetCompletedBatchCount() {
		long completed = completedBatchCount.get();
		completedBatchCount.set(0);
		return completed;
	}

}

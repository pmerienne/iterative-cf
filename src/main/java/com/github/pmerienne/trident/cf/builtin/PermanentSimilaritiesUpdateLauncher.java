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
package com.github.pmerienne.trident.cf.builtin;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class PermanentSimilaritiesUpdateLauncher implements IBatchSpout {

	private static final long serialVersionUID = -2179825508965427832L;
	private static final Logger LOGGER = LoggerFactory.getLogger(PermanentSimilaritiesUpdateLauncher.class);

	private long startMs;
	private boolean running = false;

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		// Wait for states to be initialized
		Utils.sleep(500);

		if (!this.running) {
			this.running = true;

			this.startMs = System.currentTimeMillis();
			LOGGER.info("Full similarities update started");

			collector.emit(new Values());
		}
	}

	@Override
	public void close() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		return conf;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context) {
	}

	@Override
	public void ack(long batchId) {
		long elapsedSeconds = (System.currentTimeMillis() - this.startMs) / 1000;
		LOGGER.info("Full similarities update finished in " + elapsedSeconds + " seconds");
		this.running = false;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields();
	}

}
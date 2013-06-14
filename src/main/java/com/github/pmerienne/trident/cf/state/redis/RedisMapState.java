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
package com.github.pmerienne.trident.cf.state.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.ValueUpdater;
import storm.trident.state.map.MapState;
import backtype.storm.task.IMetricsContext;

public class RedisMapState<T> extends AbstractRedisState implements MapState<T> {

	public RedisMapState(String id) {
		super(id);
	}

	public RedisMapState(String id, String host, int port) {
		super(id, host, port);
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<T> multiGet(List<List<Object>> keys) {
		List<T> results = new ArrayList<T>();

		// Create redis String keys
		String[] stringKeys = new String[keys.size()];
		for (int i = 0; i < keys.size(); i++) {
			stringKeys[i] = this.generateKey(keys.get(i));
		}

		// Call redis server
		Jedis jedis = this.pool.getResource();
		try {
			List<String> resultsAsString = jedis.mget(stringKeys);
			for (String result : resultsAsString) {
				if (result == null || result.isEmpty()) {
					results.add(null);
				} else {
					results.add((T) this.serializer.deserialize(result.getBytes()));
				}
			}
		} finally {
			this.pool.returnResource(jedis);
		}

		return results;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<T> vals) {
		String[] keyValues = new String[keys.size() * 2];
		for (int i = 0; i < keys.size(); i++) {
			keyValues[i * 2] = this.generateKey(keys.get(i));
			keyValues[i * 2 + 1] = new String(this.serializer.serialize(vals.get(i)));
		}

		// Call redis server
		Jedis jedis = this.pool.getResource();
		try {
			jedis.mset(keyValues);
		} finally {
			this.pool.returnResource(jedis);
		}

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
		List<T> curr = this.multiGet(keys);
		List<T> ret = new ArrayList<T>(curr.size());
		for (int i = 0; i < curr.size(); i++) {
			T currVal = curr.get(i);
			ValueUpdater<T> updater = updaters.get(i);
			ret.add(updater.update(currVal));
		}
		this.multiPut(keys, ret);
		return ret;
	}

	public static class Factory implements StateFactory {

		private static final long serialVersionUID = 4718043951532492603L;

		private String id;

		public Factory(String id) {
			this.id = id;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			State state;
			String host = getHost(conf);
			Integer port = getPort(conf);

			if (host != null && port != null) {
				state = new RedisMapState(this.id, host, port);
			} else {
				state = new RedisMapState(this.id);
			}

			return state;
		}
	}
}

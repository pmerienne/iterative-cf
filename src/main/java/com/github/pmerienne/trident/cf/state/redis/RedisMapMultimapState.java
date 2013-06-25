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

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.cf.state.MapMultimapState;

public class RedisMapMultimapState<K1, K2, V> extends AbstractRedisState implements MapMultimapState<K1, K2, V> {

	public RedisMapMultimapState(String id) {
		super(id);
	}

	public RedisMapMultimapState(String id, String host, int port) {
		super(id, host, port);
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}

	@Override
	public boolean put(K1 key, K2 subkey, V value) {
		Jedis jedis = this.pool.getResource();
		long result;
		try {
			String stringKey = this.generateKey(key);
			String stringSubKey = new String(this.serializer.serialize(subkey));
			result = jedis.hset(stringKey, stringSubKey, new String(this.serializer.serialize(value)));
		} finally {
			this.pool.returnResource(jedis);
		}

		return result >= 1;
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(K1 key, K2 subkey) {
		Jedis jedis = this.pool.getResource();
		V result = null;
		try {
			String stringKey = this.generateKey(key);
			String stringSubKey = new String(this.serializer.serialize(subkey));
			String resultAsString = jedis.hget(stringKey, stringSubKey);
			if (resultAsString != null && !resultAsString.isEmpty()) {
				result = (V) this.serializer.deserialize(resultAsString.getBytes());
			}
		} finally {
			this.pool.returnResource(jedis);
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<K2, V> getAll(K1 key) {
		Map<K2, V> results = new HashMap<K2, V>();

		Jedis jedis = this.pool.getResource();
		try {
			String stringKey = this.generateKey(key);
			Map<String, String> resultsAsString = jedis.hgetAll(stringKey);

			K2 subkey;
			V value;
			for (String stringSubkey : resultsAsString.keySet()) {
				subkey = (K2) this.serializer.deserialize(stringSubkey.getBytes());
				value = (V) this.serializer.deserialize(resultsAsString.get(stringSubkey).getBytes());
				results.put(subkey, value);
			}

		} finally {
			this.pool.returnResource(jedis);
		}

		return results;
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
				state = new RedisMapMultimapState(this.id, host, port);
			} else {
				state = new RedisMapMultimapState(this.id);
			}

			return state;
		}
	}
}

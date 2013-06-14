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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.cf.state.SetMultiMapState;

public class RedisSetMultiMapState<K, V> extends AbstractRedisState implements SetMultiMapState<K, V> {

	public RedisSetMultiMapState(String id) {
		super(id);
	}

	public RedisSetMultiMapState(String id, String host, int port) {
		super(id, host, port);
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}

	@Override
	public long sizeOf(K key) {
		Jedis jedis = this.pool.getResource();
		long result;
		try {
			String stringKey = this.generateKey(key);
			result = jedis.scard(stringKey);
		} finally {
			this.pool.returnResource(jedis);
		}

		return result;
	}

	@Override
	public boolean put(K key, V value) {
		Jedis jedis = this.pool.getResource();
		long result;
		try {
			String stringKey = this.generateKey(key);
			result = jedis.sadd(stringKey, new String(this.serializer.serialize(value)));
		} finally {
			this.pool.returnResource(jedis);
		}

		return result == 1;

	}

	@SuppressWarnings("unchecked")
	@Override
	public Set<V> get(K key) {
		Set<V> results = new HashSet<V>();

		Jedis jedis = this.pool.getResource();
		try {
			String stringKey = this.generateKey(key);
			Set<String> resultsAsString = jedis.smembers(stringKey);
			for (String result : resultsAsString) {
				if (result == null || result.isEmpty()) {
					results.add(null);
				} else {
					results.add((V) this.serializer.deserialize(result.getBytes()));
				}
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
				state = new RedisSetMultiMapState(this.id, host, port);
			} else {
				state = new RedisSetMultiMapState(this.id);
			}

			return state;
		}
	}
}

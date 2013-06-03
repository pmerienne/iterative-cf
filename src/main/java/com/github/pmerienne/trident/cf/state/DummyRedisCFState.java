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
package com.github.pmerienne.trident.cf.state;

import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import storm.trident.redis.RedisState;
import storm.trident.redis.RedisState.KeyFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.MapState;
import storm.trident.state.map.TransactionalMap;
import backtype.storm.task.IMetricsContext;

public class DummyRedisCFState extends DelegateCFState {

	public final static String DEFAULT_HOST = "localhost";
	public final static int DEFAULT_PORT = 6379;
	private static final Object REDIS_HOST = "redis.port";
	private static final Object REDIS_PORT = "redis.host";

	protected String host;
	protected int port;

	public DummyRedisCFState(String host, int port) {
		this.host = host;
		this.port = port;
		this.initMapStates();
	}

	public DummyRedisCFState() {
		this(DEFAULT_HOST, DEFAULT_PORT);
	}

	@Override
	public void drop() {
		Jedis jedis = new Jedis(this.host, this.port);
		jedis.flushDB();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected <T> MapState<T> createMapState(String id) {
		// Create redis state
		JedisPool pool = new JedisPool(new JedisPoolConfig(), this.host, this.port, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE);
		RedisState state = new RedisState(pool, null, new KryoTransactionalValueSerializer(), new StateNameAndHashCodeKeyFactory(id));
		return TransactionalMap.build(state);
	}

	public static class Factory implements StateFactory {

		private static final long serialVersionUID = 4718043951532492603L;

		@SuppressWarnings("rawtypes")
		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			DummyRedisCFState cfState;
			String host = getHost(conf);
			Integer port = getPort(conf);
			if (host != null && port != null) {
				cfState = new DummyRedisCFState(host, port);
			} else {
				cfState = new DummyRedisCFState();
			}
			return cfState;
		}

	}

	@SuppressWarnings("rawtypes")
	protected static String getHost(Map conf) {
		Object value = conf.get(REDIS_HOST);
		return value == null || !(value instanceof String) ? null : (String) value;
	}

	@SuppressWarnings("rawtypes")
	protected static Integer getPort(Map conf) {
		Object value = conf.get(REDIS_PORT);
		return value == null || !(value instanceof Integer) ? null : (Integer) value;
	}

	public static class StateNameAndHashCodeKeyFactory implements KeyFactory {

		private static final long serialVersionUID = -6772832239644940448L;
		private static final String PREFIX = "cf:";

		private String stateName;

		public StateNameAndHashCodeKeyFactory(String stateName) {
			this.stateName = stateName;
		}

		@Override
		public String build(List<Object> keys) {
			if (keys.size() != 1) {
				throw new RuntimeException("HashCodeKeyFactory does not support compound keys");
			}

			Object key = keys.get(0);
			String hashCode = key == null ? "null" : String.valueOf(key.hashCode());

			return new StringBuilder(PREFIX).append(this.stateName).append(":").append(hashCode).toString();
		}
	}

}

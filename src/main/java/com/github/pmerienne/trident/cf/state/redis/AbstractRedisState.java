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

import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import storm.trident.state.Serializer;
import storm.trident.state.State;

public abstract class AbstractRedisState implements State {

	public final static String DEFAULT_HOST = "localhost";
	public final static int DEFAULT_PORT = 6379;

	public static final String REDIS_HOST_CONF = "redis.port";
	public static final String REDIS_PORT_CONF = "redis.host";

	private final static String KEY_PREFIX = "cf";
	private final static String KEY_SEPARATOR = ":";

	protected String id;
	protected String host;
	protected int port;
	protected JedisPool pool;

	protected Serializer<Object> serializer = new KryoValueSerializer();

	public AbstractRedisState(String id, String host, int port) {
		this.id = id;
		this.host = host;
		this.port = port;
		this.pool = new JedisPool(new JedisPoolConfig(), this.host, this.port, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE);
	}

	public AbstractRedisState(String id) {
		this(id, DEFAULT_HOST, DEFAULT_PORT);
	}

	protected String generateKey() {
		StringBuilder sb = new StringBuilder(KEY_PREFIX).append(KEY_SEPARATOR).append(this.id);
		return sb.toString();
	}

	protected String generateKey(Object key) {
		StringBuilder sb = new StringBuilder(KEY_PREFIX).append(KEY_SEPARATOR).append(this.id).append(KEY_SEPARATOR).append(key.toString());
		return sb.toString();
	}

	protected String generateKey(List<Object> keys) {
		StringBuilder sb = new StringBuilder(KEY_PREFIX).append(KEY_SEPARATOR).append(this.id);
		for (Object key : keys) {
			sb.append(KEY_SEPARATOR).append(key.toString());
		}
		return sb.toString();
	}

	public void flushAll() {
		Jedis jedis = this.pool.getResource();
		try {
			jedis.flushDB();
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@SuppressWarnings("rawtypes")
	protected static String getHost(Map conf) {
		Object value = conf.get(REDIS_HOST_CONF);
		return value == null || !(value instanceof String) ? null : (String) value;
	}

	@SuppressWarnings("rawtypes")
	protected static Integer getPort(Map conf) {
		Object value = conf.get(REDIS_PORT_CONF);
		return value == null || !(value instanceof Integer) ? null : (Integer) value;
	}
	
}

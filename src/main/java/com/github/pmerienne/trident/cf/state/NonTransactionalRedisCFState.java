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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.github.pmerienne.trident.cf.model.SimilarUser;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Tuple;
import storm.trident.redis.RedisState;
import storm.trident.redis.RedisState.KeyFactory;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import backtype.storm.task.IMetricsContext;

public class NonTransactionalRedisCFState extends DelegateCFState {

	private final static String ALL_USERS_KEY = "users";
	private final static String USER_SIMILARITY_KEY_PREFIX = "cf:similarity:";
	private final static String USER_RATING_KEY_PREFIX = "cf:ratings:";

	public final static String DEFAULT_HOST = "localhost";
	public final static int DEFAULT_PORT = 6379;
	private static final Object REDIS_HOST = "redis.port";
	private static final Object REDIS_PORT = "redis.host";

	protected String host;
	protected int port;
	protected JedisPool pool;

	private Serializer<Object> valueSerializer;

	public NonTransactionalRedisCFState(String host, int port) {
		this.host = host;
		this.port = port;
		this.pool = new JedisPool(new JedisPoolConfig(), this.host, this.port, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE);
		this.valueSerializer = new KryoValueSerializer();
		this.initMapStates();
	}

	public NonTransactionalRedisCFState() {
		this(DEFAULT_HOST, DEFAULT_PORT);
	}

	@Override
	public void drop() {
		Jedis jedis = this.pool.getResource();
		try {
			jedis.flushDB();
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public Set<Long> getUsers() {
		Set<Long> users = new HashSet<Long>();
		Jedis jedis = this.pool.getResource();
		try {
			Set<String> result = jedis.smembers(ALL_USERS_KEY);
			for (String userId : result) {
				users.add(Long.parseLong(userId));
			}
		} finally {
			this.pool.returnResource(jedis);
		}
		return users;
	}

	@Override
	public void addUser(long user) {
		Jedis jedis = this.pool.getResource();
		try {
			jedis.sadd(ALL_USERS_KEY, Long.toString(user));
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public long userCount() {
		long count;

		Jedis jedis = this.pool.getResource();
		try {
			count = jedis.scard(ALL_USERS_KEY);
		} finally {
			this.pool.returnResource(jedis);
		}

		return count;
	}

	@Override
	public void setA(long user1, long user2, double a) {
		Jedis jedis = this.pool.getResource();
		try {
			jedis.zadd(USER_SIMILARITY_KEY_PREFIX + user1, a, Long.toString(user2));
			jedis.zadd(USER_SIMILARITY_KEY_PREFIX + user2, a, Long.toString(user1));
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public double getA(long user1, long user2) {
		Double score;

		Jedis jedis = this.pool.getResource();
		try {
			score = jedis.zscore(USER_SIMILARITY_KEY_PREFIX + user1, Long.toString(user2));
		} finally {
			this.pool.returnResource(jedis);
		}

		return score == null ? -1 : score;
	}

	@Override
	public Set<SimilarUser> getMostSimilarUsers(long user, int count) {
		Set<SimilarUser> similarUsers = new HashSet<SimilarUser>();

		Jedis jedis = this.pool.getResource();
		try {
			Set<Tuple> results = jedis.zrevrangeWithScores(USER_SIMILARITY_KEY_PREFIX + user, 0, count - 1);
			for (Tuple result : results) {
				similarUsers.add(new SimilarUser(Long.parseLong(result.getElement()), result.getScore()));
			}
		} finally {
			this.pool.returnResource(jedis);
		}

		return similarUsers;
	}

	@Override
	public void addRating(long user, long item, double rating) {
		Jedis jedis = this.pool.getResource();
		try {
			jedis.hset(USER_RATING_KEY_PREFIX + user, Long.toString(item), Double.toString(rating));
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public Double getRating(long user, long item) {
		Double rating = null;

		Jedis jedis = this.pool.getResource();
		try {
			String result = jedis.hget(USER_RATING_KEY_PREFIX + user, Long.toString(item));
			rating = result == null ? null : Double.parseDouble(result);
		} finally {
			this.pool.returnResource(jedis);
		}

		return rating;
	}

	@Override
	public Map<Long, Double> getRatings(long user) {
		Map<Long, Double> ratings = new HashMap<Long, Double>();

		Jedis jedis = this.pool.getResource();
		try {
			Map<String, String> results = jedis.hgetAll(USER_RATING_KEY_PREFIX + user);
			for (Entry<String, String> entry : results.entrySet()) {
				ratings.put(Long.parseLong(entry.getKey()), Double.parseDouble(entry.getValue()));
			}
		} finally {
			this.pool.returnResource(jedis);
		}

		return ratings;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected <T> MapState<T> createMapState(String id) {
		// Create redis state
		RedisState state = new RedisState(this.pool, null, this.valueSerializer, new StateNameAndHashCodeKeyFactory(id));
		return NonTransactionalMap.build(state);
	}

	public static class Factory implements StateFactory {

		private static final long serialVersionUID = 4718043951532492603L;

		@SuppressWarnings("rawtypes")
		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			NonTransactionalRedisCFState cfState;
			String host = getHost(conf);
			Integer port = getPort(conf);
			if (host != null && port != null) {
				cfState = new NonTransactionalRedisCFState(host, port);
			} else {
				cfState = new NonTransactionalRedisCFState();
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

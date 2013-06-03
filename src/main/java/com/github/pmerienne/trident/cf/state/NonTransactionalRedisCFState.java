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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Tuple;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.cf.model.SimilarUser;

public class NonTransactionalRedisCFState implements CFState {

	private final static String ALL_USERS_KEY = "users";
	private final static String USER_SIMILARITY_KEY_PREFIX = "similarity";
	private final static String USER_RATING_KEY_PREFIX = "ratings";
	private final static String M_KEY_PREFIX = "m";
	private final static String USER_PAIR_CACHE_KEY_PREFIX = "userpaircache";
	private final static String AVERAGE_RATING_CACHE_KEY_PREFIX = "averagerating";
	private final static String CO_RATED_SUMS_CACHE_KEY_PREFIX = "coratedsums";

	public final static String DEFAULT_HOST = "localhost";
	public final static int DEFAULT_PORT = 6379;
	private static final Object REDIS_HOST = "redis.port";
	private static final Object REDIS_PORT = "redis.host";

	protected String host;
	protected int port;
	protected JedisPool pool;

	public NonTransactionalRedisCFState(String host, int port) {
		this.host = host;
		this.port = port;
		this.pool = new JedisPool(new JedisPoolConfig(), this.host, this.port, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE);
	}

	public NonTransactionalRedisCFState() {
		this(DEFAULT_HOST, DEFAULT_PORT);
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
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
			String key1 = getKey(USER_SIMILARITY_KEY_PREFIX, user1);
			jedis.zadd(key1, a, Long.toString(user2));
			String key2 = getKey(USER_SIMILARITY_KEY_PREFIX, user2);
			jedis.zadd(key2, a, Long.toString(user1));
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public double getA(long user1, long user2) {
		Double score;

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(USER_SIMILARITY_KEY_PREFIX, user1);
			score = jedis.zscore(key, Long.toString(user2));
		} finally {
			this.pool.returnResource(jedis);
		}

		return score == null ? -1.0 : score;
	}

	@Override
	public Set<SimilarUser> getMostSimilarUsers(long user, int count) {
		Set<SimilarUser> similarUsers = new HashSet<SimilarUser>();

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(USER_SIMILARITY_KEY_PREFIX, user);
			Set<Tuple> results = jedis.zrevrangeWithScores(key, 0, count - 1);
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
			String key = getKey(USER_RATING_KEY_PREFIX, user);
			jedis.hset(key, Long.toString(item), Double.toString(rating));
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public Double getRating(long user, long item) {
		Double rating = null;

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(USER_RATING_KEY_PREFIX, user);
			String result = jedis.hget(key, Long.toString(item));
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
			String key = getKey(USER_RATING_KEY_PREFIX, user);
			Map<String, String> results = jedis.hgetAll(key);
			for (Entry<String, String> entry : results.entrySet()) {
				ratings.put(Long.parseLong(entry.getKey()), Double.parseDouble(entry.getValue()));
			}
		} finally {
			this.pool.returnResource(jedis);
		}

		return ratings;
	}

	@Override
	public void setAverageRating(long user, double newAverageRating) {
		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(AVERAGE_RATING_CACHE_KEY_PREFIX, user);
			jedis.set(key, Double.toString(newAverageRating));
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public double getAverageRating(long user) {
		double averageRating;

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(AVERAGE_RATING_CACHE_KEY_PREFIX, user);
			String result = jedis.get(key);
			averageRating = result == null ? 0.0 : Double.parseDouble(result);
		} finally {
			this.pool.returnResource(jedis);
		}

		return averageRating;
	}

	@Override
	public long getCoRatedCount(long user1, long user2) {
		long coRatedCount;

		long minUser = Math.min(user1, user2);
		long maxUser = Math.max(user1, user2);

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(USER_PAIR_CACHE_KEY_PREFIX, minUser, maxUser);
			String result = jedis.hget(key, "coRatedCount");
			coRatedCount = result == null ? 0 : Long.parseLong(result);
		} finally {
			this.pool.returnResource(jedis);
		}

		return coRatedCount;
	}

	@Override
	public void setCoRatedCount(long user1, long user2, long count) {
		long minUser = Math.min(user1, user2);
		long maxUser = Math.max(user1, user2);

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(USER_PAIR_CACHE_KEY_PREFIX, minUser, maxUser);
			jedis.hset(key, "coRatedCount", Long.toString(count));
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public Map<Long, Double> getCoRatedSums(long user1, long user2) {
		Map<Long, Double> coRatedSums = new HashMap<Long, Double>();
		coRatedSums.put(user1, 0.0);
		coRatedSums.put(user2, 0.0);

		long minUser = Math.min(user1, user2);
		long maxUser = Math.max(user1, user2);

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(CO_RATED_SUMS_CACHE_KEY_PREFIX, minUser, maxUser);
			Map<String, String> results = jedis.hgetAll(key);
			for (Entry<String, String> coRatedSum : results.entrySet()) {
				coRatedSums.put(Long.parseLong(coRatedSum.getKey()), Double.parseDouble(coRatedSum.getValue()));
			}
		} finally {
			this.pool.returnResource(jedis);
		}

		return coRatedSums;
	}

	@Override
	public void setCoRatedSums(long user1, long user2, Map<Long, Double> coRatedSums) {
		long minUser = Math.min(user1, user2);
		long maxUser = Math.max(user1, user2);

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(CO_RATED_SUMS_CACHE_KEY_PREFIX, minUser, maxUser);
			for (Entry<Long, Double> coRatedSum : coRatedSums.entrySet()) {
				jedis.hset(key, coRatedSum.getKey().toString(), coRatedSum.getValue().toString());
			}
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public long getM(long user) {
		long m;

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(M_KEY_PREFIX, user);
			String result = jedis.get(key);
			m = result == null ? 0 : Long.parseLong(result);
		} finally {
			this.pool.returnResource(jedis);
		}

		return m;
	}

	@Override
	public void setM(long user, long count) {
		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(M_KEY_PREFIX, user);
			jedis.set(key, Long.toString(count));
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public double getB(long user1, long user2) {
		return this.getUserPairCache("b", user1, user2);
	}

	@Override
	public void setB(long user1, long user2, double b) {
		this.setUserPairCache("b", user1, user2, b);
	}

	@Override
	public double getC(long user1, long user2) {
		return this.getUserPairCache("c", user1, user2);
	}

	@Override
	public void setC(long user1, long user2, double c) {
		this.setUserPairCache("c", user1, user2, c);
	}

	@Override
	public double getD(long user1, long user2) {
		return this.getUserPairCache("d", user1, user2);
	}

	@Override
	public void setD(long user1, long user2, double d) {
		this.setUserPairCache("d", user1, user2, d);
	}

	protected void setUserPairCache(String field, long user1, long user2, double value) {
		long minUser = Math.min(user1, user2);
		long maxUser = Math.max(user1, user2);

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(USER_PAIR_CACHE_KEY_PREFIX, minUser, maxUser);
			jedis.hset(key, field, Double.toString(value));
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	protected double getUserPairCache(String field, long user1, long user2) {
		double value;

		long minUser = Math.min(user1, user2);
		long maxUser = Math.max(user1, user2);

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(USER_PAIR_CACHE_KEY_PREFIX, minUser, maxUser);
			String result = jedis.hget(key, field);
			value = result == null ? 0.0 : Double.parseDouble(result);
		} finally {
			this.pool.returnResource(jedis);
		}

		return value;
	}

	protected static String getKey(String prefix, Object... keys) {
		StringBuilder sb = new StringBuilder("cf:").append(prefix);
		for (Object key : keys) {
			sb.append(":").append(key.toString());
		}
		return sb.toString();
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

}

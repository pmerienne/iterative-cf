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

import java.util.HashSet;
import java.util.Map;
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

public class RedisCFState implements CFState {

	private final static String ALL_USERS_KEY = "users";
	private final static String ALL_ITEMS_KEY = "items";
	private final static String USER_SIMILARITY_KEY_PREFIX = "similarity";
	private final static String CO_RATED_SUMS_KEY_PREFIX = "coratedsums";
	private final static String PREFERED_ITEMS_KEY_PREFIX = "preferedItems";
	private final static String USER_PREFERENCES_KEY_PREFIX = "userPreference";

	public final static String DEFAULT_HOST = "localhost";
	public final static int DEFAULT_PORT = 6379;
	private static final String REDIS_HOST = "redis.port";
	private static final String REDIS_PORT = "redis.host";

	protected String host;
	protected int port;
	protected JedisPool pool;

	public RedisCFState(String host, int port) {
		this.host = host;
		this.port = port;
		this.pool = new JedisPool(new JedisPoolConfig(), this.host, this.port, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE);
	}

	public RedisCFState() {
		this(DEFAULT_HOST, DEFAULT_PORT);
	}

	@Override
	public void beginCommit(Long txid) {
		// not transactional!
	}

	@Override
	public void commit(Long txid) {
		// not transactional!
	}

	@Override
	public void addItems(Set<Long> items) {
		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(ALL_ITEMS_KEY);
			for (long item : items) {
				jedis.sadd(key, Long.toString(item));
			}
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public Set<Long> getItems() {
		Set<Long> items = new HashSet<Long>();
		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(ALL_ITEMS_KEY);
			Set<String> result = jedis.smembers(key);
			for (String itemId : result) {
				items.add(Long.parseLong(itemId));
			}
		} finally {
			this.pool.returnResource(jedis);
		}
		return items;
	}

	@Override
	public long itemCount() {
		long count;

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(ALL_ITEMS_KEY);
			count = jedis.scard(key);
		} finally {
			this.pool.returnResource(jedis);
		}

		return count;
	}

	@Override
	public long getNumItemsPreferedBy(long user1, long user2) {
		long count = 0;

		long minUser = Math.min(user1, user2);
		long maxUser = Math.max(user1, user2);

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(CO_RATED_SUMS_KEY_PREFIX, minUser, maxUser);
			String countAsString = jedis.get(key);
			if (countAsString != null && !countAsString.isEmpty()) {
				count = Long.parseLong(countAsString);
			}
		} finally {
			this.pool.returnResource(jedis);
		}

		return count;
	}

	@Override
	public void setNumItemsPreferedBy(long user1, long user2, long numItems) {
		long minUser = Math.min(user1, user2);
		long maxUser = Math.max(user1, user2);

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(CO_RATED_SUMS_KEY_PREFIX, minUser, maxUser);
			jedis.set(key, Long.toString(numItems));
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public void addUsers(Set<Long> users) {
		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(ALL_USERS_KEY);
			for (long user : users) {
				jedis.sadd(key, Long.toString(user));
			}
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public Set<Long> getUsers() {
		Set<Long> users = new HashSet<Long>();
		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(ALL_USERS_KEY);
			Set<String> result = jedis.smembers(key);
			for (String userId : result) {
				users.add(Long.parseLong(userId));
			}
		} finally {
			this.pool.returnResource(jedis);
		}
		return users;
	}

	@Override
	public long userCount() {
		long count;

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(ALL_USERS_KEY);
			count = jedis.scard(key);
		} finally {
			this.pool.returnResource(jedis);
		}

		return count;
	}

	@Override
	public void setUserPreference(long user, long item) {
		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(PREFERED_ITEMS_KEY_PREFIX, item);
			jedis.sadd(key, Long.toString(user));

			key = getKey(USER_PREFERENCES_KEY_PREFIX, user);
			jedis.sadd(key, Long.toString(item));
		} finally {
			this.pool.returnResource(jedis);
		}

	}

	@Override
	public Set<Long> getUserPreferences(long user) {
		Set<Long> items = new HashSet<Long>();

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(USER_PREFERENCES_KEY_PREFIX, user);
			Set<String> results = jedis.smembers(key);
			for (String itemId : results) {
				items.add(Long.parseLong(itemId));
			}
		} finally {
			this.pool.returnResource(jedis);
		}
		return items;
	}

	@Override
	public Set<Long> getUsersWithPreferenceFor(long item) {
		Set<Long> users = new HashSet<Long>();

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(PREFERED_ITEMS_KEY_PREFIX, item);
			Set<String> results = jedis.smembers(key);
			for (String userId : results) {
				users.add(Long.parseLong(userId));
			}
		} finally {
			this.pool.returnResource(jedis);
		}
		return users;
	}

	@Override
	public long getPreferenceCount(long user) {
		Long count = null;
		Jedis jedis = this.pool.getResource();

		try {
			String key = getKey(USER_PREFERENCES_KEY_PREFIX, user);
			count = jedis.scard(key);
		} finally {
			this.pool.returnResource(jedis);
		}

		return count == null ? 0 : count;
	}

	@Override
	public void setSimilarity(long user1, long user2, double similarity) {
		Jedis jedis = this.pool.getResource();
		try {
			String key1 = getKey(USER_SIMILARITY_KEY_PREFIX, user1);
			jedis.zadd(key1, similarity, Long.toString(user2));
			String key2 = getKey(USER_SIMILARITY_KEY_PREFIX, user2);
			jedis.zadd(key2, similarity, Long.toString(user1));
		} finally {
			this.pool.returnResource(jedis);
		}

	}

	@Override
	public double getSimilarity(long user1, long user2) {
		Double score;

		Jedis jedis = this.pool.getResource();
		try {
			String key = getKey(USER_SIMILARITY_KEY_PREFIX, user1);
			score = jedis.zscore(key, Long.toString(user2));
		} finally {
			this.pool.returnResource(jedis);
		}

		return score == null ? 0.0 : score;
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
	public void drop() {
		Jedis jedis = this.pool.getResource();
		try {
			jedis.flushDB();
		} finally {
			this.pool.returnResource(jedis);
		}
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
			RedisCFState cfState;
			String host = getHost(conf);
			Integer port = getPort(conf);
			if (host != null && port != null) {
				cfState = new RedisCFState(host, port);
			} else {
				cfState = new RedisCFState();
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

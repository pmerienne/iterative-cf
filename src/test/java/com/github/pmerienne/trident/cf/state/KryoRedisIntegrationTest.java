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

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import storm.trident.redis.RedisState;
import storm.trident.redis.RedisState.Options;
import storm.trident.state.StateFactory;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.MapState;

import com.github.pmerienne.trident.cf.state.DummyRedisCFState.StateNameAndHashCodeKeyFactory;
import com.github.pmerienne.trident.cf.util.KeysUtil;

public class KryoRedisIntegrationTest {

	public final static String DEFAULT_HOST = "localhost";
	public final static int DEFAULT_PORT = 6379;

	@SuppressWarnings("rawtypes")
	private MapState<TransactionalValue> state;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Before
	public void setup() {
		Jedis jedis = new Jedis(DEFAULT_HOST, DEFAULT_PORT);
		jedis.flushDB();

		InetSocketAddress server = new InetSocketAddress(DEFAULT_HOST, DEFAULT_PORT);

		Options<TransactionalValue> options = new Options<TransactionalValue>();
		options.serializer = new KryoTransactionalValueSerializer();
		options.localCacheSize = 0;

		StateFactory stateFactory = RedisState.transactional(server, options, new StateNameAndHashCodeKeyFactory("test"));
		this.state = (MapState<TransactionalValue>) stateFactory.makeState(null, null, 0, 0);
	}

	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testKryoSerializationIntegration() {
		// Given
		Long txid = 1L;
		List<List<Object>> keys = KeysUtil.toKeys("test");
		List values = Arrays.asList(new TransactionalValue<Double>(txid, 0.8));

		// When
		this.state.beginCommit(txid);
		this.state.multiPut(keys, values);
		this.state.commit(txid);
		List<TransactionalValue> actualValues = this.state.multiGet(keys);

		// Then
		assertEquals(1, actualValues.size());
		TransactionalValue actualValue = actualValues.get(0);
		assertEquals(txid, actualValue.getTxid());
		assertEquals(0.8, (Double) actualValue.getVal(), 10e-3);
	}

	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testKryoSerializationIntegrationWithMap() {
		// Given
		Long txid = 1L;
		List<List<Object>> keys = KeysUtil.toKeys("test");
		Map<Long, Double> map = new HashMap<Long, Double>();
		map.put(32L, 0.8);
		map.put(3L, 0.5);
		List values = Arrays.asList(new TransactionalValue<Map<Long, Double>>(txid, map));

		// When
		this.state.beginCommit(txid);
		this.state.multiPut(keys, values);
		this.state.commit(txid);
		List<TransactionalValue> actualValues = this.state.multiGet(keys);

		// Then
		assertEquals(1, actualValues.size());
		TransactionalValue actualValue = actualValues.get(0);
		assertEquals(txid, actualValue.getTxid());
		assertEquals(map, (Map<Long, Double>) actualValue.getVal());
	}
}

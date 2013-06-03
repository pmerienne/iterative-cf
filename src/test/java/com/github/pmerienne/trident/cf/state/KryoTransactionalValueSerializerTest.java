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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import storm.trident.state.Serializer;
import storm.trident.state.TransactionalValue;

public class KryoTransactionalValueSerializerTest {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void test() {
		// Given
		Serializer<TransactionalValue> serializer = new KryoTransactionalValueSerializer();

		Long txid = 54654L;
		TransactionalValue<Double> transactionalValue = new TransactionalValue<Double>(txid, 654.123);

		// When
		byte[] bytes = serializer.serialize(transactionalValue);
		TransactionalValue<Map<Long, Double>> actual = serializer.deserialize(bytes);

		// Then
		assertEquals(txid, actual.getTxid());
		assertEquals(transactionalValue.getVal(), actual.getVal());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testWithNullTxid() {
		// Given
		Serializer<TransactionalValue> serializer = new KryoTransactionalValueSerializer();

		Long txid = null;
		TransactionalValue<Double> transactionalValue = new TransactionalValue<Double>(txid, 654.123);

		// When
		byte[] bytes = serializer.serialize(transactionalValue);
		TransactionalValue<Map<Long, Double>> actual = serializer.deserialize(bytes);

		// Then
		assertEquals(txid, actual.getTxid());
		assertEquals(transactionalValue.getVal(), actual.getVal());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testWithNullVal() {
		// Given
		Serializer<TransactionalValue> serializer = new KryoTransactionalValueSerializer();

		Long txid = null;
		TransactionalValue<Object> transactionalValue = new TransactionalValue<Object>(txid, null);

		// When
		byte[] bytes = serializer.serialize(transactionalValue);
		TransactionalValue<Object> actual = serializer.deserialize(bytes);

		// Then
		assertEquals(txid, actual.getTxid());
		assertEquals(transactionalValue.getVal(), actual.getVal());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testWithSet() {
		// Given
		Serializer<TransactionalValue> serializer = new KryoTransactionalValueSerializer();

		Long txid = null;
		Set<Long> val = new HashSet<Long>(Arrays.asList(1L, 2L, 6L, 10L));
		TransactionalValue<Set<Long>> transactionalValue = new TransactionalValue<Set<Long>>(txid, val);

		// When
		byte[] bytes = serializer.serialize(transactionalValue);
		TransactionalValue<Map<Long, Double>> actual = serializer.deserialize(bytes);

		// Then
		assertEquals(txid, actual.getTxid());
		assertEquals(transactionalValue.getVal(), actual.getVal());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testWithMap() {
		// Given
		Serializer<TransactionalValue> serializer = new KryoTransactionalValueSerializer();

		Long txid = 54654L;
		Map<Long, Double> map = new HashMap<Long, Double>();
		map.put(1L, 0.9);
		map.put(2L, 0.8);
		map.put(3L, 0.7);
		TransactionalValue<Map<Long, Double>> transactionalValue = new TransactionalValue<Map<Long, Double>>(txid, map);

		// When
		byte[] bytes = serializer.serialize(transactionalValue);
		TransactionalValue<Map<Long, Double>> actual = serializer.deserialize(bytes);

		// Then
		assertEquals(txid, actual.getTxid());
		assertEquals(map, actual.getVal());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testWithStringEncoding() {
		// Given
		Serializer<TransactionalValue> serializer = new KryoTransactionalValueSerializer();

		Long txid = 54654L;
		TransactionalValue<Double> transactionalValue = new TransactionalValue<Double>(txid, 654.123);

		// When
		byte[] bytes = serializer.serialize(transactionalValue);
		String data = new String(bytes);
		TransactionalValue<Map<Long, Double>> actual = serializer.deserialize(data.getBytes());

		// Then
		assertEquals(txid, actual.getTxid());
		assertEquals(transactionalValue.getVal(), actual.getVal());
	}
}

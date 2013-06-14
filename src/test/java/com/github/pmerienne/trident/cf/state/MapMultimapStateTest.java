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
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

import com.github.pmerienne.trident.cf.testing.TestValue;

public abstract class MapMultimapStateTest {

	protected MapMultimapState<String, String, TestValue> state;

	@Test
	public void testPut() {
		// Given
		String key = "key";
		String subkey = "subkey";
		TestValue expectedValue = TestValue.random();

		// When
		this.state.put(key, subkey, expectedValue);

		// Then
		TestValue acualValue = this.state.get(key, subkey);
		assertEquals(expectedValue, acualValue);
	}

	@Test
	public void testGet() {
		// Given
		String key = "key";
		String subkey = "subkey";
		TestValue expectedValue = TestValue.random();

		// When
		this.state.put(key, subkey, expectedValue);
		TestValue acualValue = this.state.get(key, subkey);

		// Then
		assertEquals(expectedValue, acualValue);
	}

	@Test
	public void testGetWithoutValue() {
		// Given
		String key = "key";
		String subkey = "subkey";

		// When
		TestValue acualValue = this.state.get(key, subkey);

		// Then
		assertEquals(null, acualValue);
	}

	@Test
	public void testGetAll() {
		// Given
		String key = "key";
		String subkey1 = "subkey1";
		String subkey2 = "subkey2";
		String subkey3 = "subkey3";
		TestValue expectedValue1 = TestValue.random();
		TestValue expectedValue2 = TestValue.random();
		TestValue expectedValue3 = TestValue.random();

		// When
		this.state.put(key, subkey1, expectedValue1);
		this.state.put(key, subkey2, expectedValue2);
		this.state.put(key, subkey3, expectedValue3);
		Map<String, TestValue> acualValues = this.state.getAll(key);

		// Then
		assertEquals(3, acualValues.size());
		assertEquals(expectedValue1, acualValues.get(subkey1));
		assertEquals(expectedValue2, acualValues.get(subkey2));
		assertEquals(expectedValue3, acualValues.get(subkey3));
	}

	@Test
	public void testGetAllWithoutValue() {
		// Given
		String key = "key";

		// When
		Map<String, TestValue> acualValues = this.state.getAll(key);

		// Then
		assertTrue(acualValues.isEmpty());
	}
}

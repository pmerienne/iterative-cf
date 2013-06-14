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

import static org.junit.Assert.*;

import java.util.Set;

import org.junit.Test;

import com.github.pmerienne.trident.cf.testing.TestValue;

public abstract class SetMultiMapStateTest {

	protected SetMultiMapState<String, TestValue> state;

	@Test
	public void testPutOneValue() {
		// Given
		String key = "test";
		TestValue testValue = TestValue.random();

		// When
		this.state.put(key, testValue);

		// Then
		Set<TestValue> set = this.state.get(key);
		assertEquals(1, set.size());
		assertTrue(set.contains(testValue));
	}

	@Test
	public void testPutMultiValues() {
		// Given
		String key = "test";
		TestValue testValue1 = TestValue.random();
		TestValue testValue2 = TestValue.random();
		TestValue testValue3 = TestValue.random();

		// When
		this.state.put(key, testValue1);
		this.state.put(key, testValue2);
		this.state.put(key, testValue3);

		// Then
		Set<TestValue> set = this.state.get(key);
		assertEquals(3, set.size());
		assertTrue(set.contains(testValue1));
		assertTrue(set.contains(testValue2));
		assertTrue(set.contains(testValue3));
	}

	@Test
	public void testSizeOf() {
		// Given
		String key = "test";
		TestValue testValue1 = TestValue.random();
		TestValue testValue2 = TestValue.random();
		TestValue testValue3 = TestValue.random();

		// When
		this.state.put(key, testValue1);
		this.state.put(key, testValue2);
		this.state.put(key, testValue3);
		long actualSize = this.state.sizeOf(key);

		// Then
		assertEquals(3, actualSize);
	}

	@Test
	public void testSizeOfWithNoSet() {
		// Given
		String key = "test";

		// When
		long actualSize = this.state.sizeOf(key);

		// Then
		assertEquals(0, actualSize);
	}

	@Test
	public void testGet() {
		// Given
		String key = "test";
		TestValue testValue = TestValue.random();

		// When
		this.state.put(key, testValue);

		// Then
		Set<TestValue> set = this.state.get(key);
		assertEquals(1, set.size());
		assertTrue(set.contains(testValue));
	}

	@Test
	public void testGetWithNoSet() {
		// Given
		String key = "test";

		// When
		Set<TestValue> set = this.state.get(key);

		// Then
		assertTrue(set.isEmpty());
	}
}

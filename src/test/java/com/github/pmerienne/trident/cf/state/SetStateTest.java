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

import java.util.Arrays;
import java.util.Set;

import org.junit.Test;

import com.github.pmerienne.trident.cf.testing.TestValue;

public abstract class SetStateTest {

	protected SetState<TestValue> state;

	@Test
	public void testAdd() {
		// Given
		TestValue testValue = TestValue.random();

		// When
		this.state.add(testValue);

		// Then
		Set<TestValue> actualSet = this.state.get();
		assertEquals(1, actualSet.size());
		assertTrue(actualSet.contains(testValue));
	}

	@Test
	public void testAddAll() {
		// Given
		TestValue testValue1 = TestValue.random();
		TestValue testValue2 = TestValue.random();
		TestValue testValue3 = TestValue.random();

		// When
		this.state.addAll(Arrays.asList(testValue1, testValue2, testValue3));

		// Then
		Set<TestValue> actualSet = this.state.get();
		assertEquals(3, actualSet.size());
		assertTrue(actualSet.contains(testValue1));
		assertTrue(actualSet.contains(testValue2));
		assertTrue(actualSet.contains(testValue3));
	}

	@Test
	public void testGet() {
		// Given
		TestValue testValue1 = TestValue.random();
		TestValue testValue2 = TestValue.random();
		TestValue testValue3 = TestValue.random();

		// When
		this.state.addAll(Arrays.asList(testValue1, testValue2, testValue3));
		Set<TestValue> actualSet = this.state.get();

		// Then
		assertEquals(3, actualSet.size());
		assertTrue(actualSet.contains(testValue1));
		assertTrue(actualSet.contains(testValue2));
		assertTrue(actualSet.contains(testValue3));
	}

	@Test
	public void testGetWithNoValue() {
		// When
		Set<TestValue> actualSet = this.state.get();

		// Then
		assertTrue(actualSet.isEmpty());
	}

	@Test
	public void testClear() {
		TestValue testValue1 = TestValue.random();
		TestValue testValue2 = TestValue.random();
		TestValue testValue3 = TestValue.random();

		// When
		this.state.addAll(Arrays.asList(testValue1, testValue2, testValue3));
		this.state.clear();

		// Then
		Set<TestValue> actualSet = this.state.get();
		assertTrue(actualSet.isEmpty());
	}

}

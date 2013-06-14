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

import java.util.List;

import org.junit.Test;

import com.github.pmerienne.trident.cf.state.SortedSetMultiMapState.ScoredValue;
import com.github.pmerienne.trident.cf.testing.TestValue;

public abstract class SortedSetMultiMapStateTest {

	protected SortedSetMultiMapState<String, TestValue> state;

	@Test
	public void testPutOneValue() {
		// Given
		String key = "test";
		TestValue testValue = TestValue.random();
		double expectedScore = 0.1;

		// When
		this.state.put(key, new ScoredValue<TestValue>(expectedScore, testValue));

		// Then
		double actualScore = this.state.getScore(key, testValue);
		assertEquals(expectedScore, actualScore, 10e-3);
	}

	@Test
	public void testPutMultiValues() {
		// Given
		String key = "test";
		double expectedScore1 = 0.1;
		double expectedScore2 = 0.2;
		double expectedScore3 = 0.3;
		ScoredValue<TestValue> testValue1 = new ScoredValue<TestValue>(expectedScore1, TestValue.random());
		ScoredValue<TestValue> testValue2 = new ScoredValue<TestValue>(expectedScore2, TestValue.random());
		ScoredValue<TestValue> testValue3 = new ScoredValue<TestValue>(expectedScore3, TestValue.random());

		// When
		this.state.put(key, testValue1);
		this.state.put(key, testValue2);
		this.state.put(key, testValue3);

		// Then
		List<ScoredValue<TestValue>> actual = this.state.getSorted(key, 3);
		assertEquals(3, actual.size());
		assertTrue(actual.contains(testValue1));
		assertTrue(actual.contains(testValue2));
		assertTrue(actual.contains(testValue3));
	}

	@Test
	public void testSizeOf() {
		// Given
		String key = "test";
		TestValue testValue1 = TestValue.random();
		TestValue testValue2 = TestValue.random();
		TestValue testValue3 = TestValue.random();

		// When
		this.state.put(key, new ScoredValue<TestValue>(0.1, testValue1));
		this.state.put(key, new ScoredValue<TestValue>(0.2, testValue2));
		this.state.put(key, new ScoredValue<TestValue>(0.3, testValue3));
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
	public void testGetScore() {
		// Given
		String key = "test";
		TestValue testValue = TestValue.random();
		double expectedScore = 0.1;

		// When
		this.state.put(key, new ScoredValue<TestValue>(expectedScore, testValue));
		double actualScore = this.state.getScore(key, testValue);

		// Then
		assertEquals(expectedScore, actualScore, 10e-3);
	}

	@Test
	public void testGetScoreWithNoValue() {
		// Given
		String key = "test";
		double expectedScore = 0.0;

		// When
		double actualScore = this.state.getScore(key, TestValue.random());

		// Then
		assertEquals(expectedScore, actualScore, 10e-3);
	}

	@Test
	public void testGetSorted() {
		// Given
		String key = "test";
		double expectedScore1 = 0.9;
		double expectedScore2 = 0.2;
		double expectedScore3 = 0.3;
		ScoredValue<TestValue> testValue1 = new ScoredValue<TestValue>(expectedScore1, TestValue.random());
		ScoredValue<TestValue> testValue2 = new ScoredValue<TestValue>(expectedScore2, TestValue.random());
		ScoredValue<TestValue> testValue3 = new ScoredValue<TestValue>(expectedScore3, TestValue.random());

		// When
		this.state.put(key, testValue1);
		this.state.put(key, testValue2);
		this.state.put(key, testValue3);
		List<ScoredValue<TestValue>> actual = this.state.getSorted(key, 3);

		// Then
		assertEquals(3, actual.size());
		assertTrue(actual.contains(testValue1));
		assertTrue(actual.contains(testValue2));
		assertTrue(actual.contains(testValue3));

		double previousScore = Double.POSITIVE_INFINITY;
		for (ScoredValue<TestValue> current : actual) {
			double currentScore = current.getScore();
			assertTrue(previousScore > currentScore);
			previousScore = currentScore;
		}
	}

	@Test
	public void testGetSortedWithNoValue() {
		// Given
		String key = "test";

		// When
		List<ScoredValue<TestValue>> actual = this.state.getSorted(key, 3);

		// Then
		assertTrue(actual.isEmpty());
	}

}

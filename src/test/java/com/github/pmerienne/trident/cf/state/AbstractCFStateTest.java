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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.github.pmerienne.trident.cf.model.SimilarUser;

public abstract class AbstractCFStateTest {

	protected CFState state;

	@Test
	public void testAddAndGetUsers() {
		// When
		this.state.addUser(1);
		this.state.addUser(2);
		this.state.addUser(3);

		// Then
		long actualCount = this.state.userCount();
		assertEquals(3, actualCount);

		Set<Long> actualUsers = this.state.getUsers();
		assertTrue(actualUsers.contains(1L));
		assertTrue(actualUsers.contains(2L));
		assertTrue(actualUsers.contains(3L));
	}

	@Test
	public void testAddRating() {
		// When
		this.state.addRating(1, 1, 0.5);

		// Then
		double actualRating = this.state.getRating(1, 1);
		assertEquals(0.5, actualRating, 10e-3);
	}

	@Test
	public void testGetRatings() {
		// When
		this.state.addRating(1L, 1L, 0.5);
		this.state.addRating(1L, 2L, 0.6);
		this.state.addRating(1L, 3L, 0.9);

		// Then
		Map<Long, Double> actualRatings = this.state.getRatings(1);
		assertEquals(3, actualRatings.size());
		assertEquals(0.5, actualRatings.get(1L), 10e-3);
		assertEquals(0.6, actualRatings.get(2L), 10e-3);
		assertEquals(0.9, actualRatings.get(3L), 10e-3);
	}

	@Test
	public void testSetAndGetM() {
		// When
		this.state.setM(1, 32);

		// Then
		long actualCount = this.state.getM(1);
		assertEquals(32, actualCount);

		actualCount = this.state.getM(6541);
		assertEquals(0, actualCount);
	}

	@Test
	public void testSetAndGetA() {
		// When
		this.state.setA(1, 2, 0.8);

		// Then
		double actualSimilarity = this.state.getA(1, 2);
		assertEquals(0.8, actualSimilarity, 10e-3);

		actualSimilarity = this.state.getA(2, 1);
		assertEquals(0.8, actualSimilarity, 10e-3);

		assertEquals(-1.0, this.state.getA(1, 6541), 10e-3);
	}

	@Test
	public void testSetAndGetB() {
		// When
		double expectedB = 0.8;
		this.state.setB(1, 2, expectedB);

		// Then
		double actualB = this.state.getB(1, 2);
		assertEquals(expectedB, actualB, 10e-3);

		actualB = this.state.getB(2, 1);
		assertEquals(expectedB, actualB, 10e-3);
	}

	@Test
	public void testSetAndGetC() {
		// When
		double expectedC = 0.8;
		this.state.setC(1, 2, expectedC);

		// Then
		double actualC = this.state.getC(1, 2);
		assertEquals(expectedC, actualC, 10e-3);

		actualC = this.state.getC(2, 1);
		assertEquals(expectedC, actualC, 10e-3);
	}

	@Test
	public void testSetAndGetD() {
		// When
		double expectedD = 0.8;
		this.state.setD(1, 2, expectedD);

		// Then
		double actualD = this.state.getD(1, 2);
		assertEquals(expectedD, actualD, 10e-3);

		actualD = this.state.getD(2, 1);
		assertEquals(expectedD, actualD, 10e-3);
	}

	@Test
	public void testGetMostSimilarUsers() {
		// When
		this.state.addUser(1);
		this.state.addUser(2);
		this.state.addUser(3);
		this.state.addUser(4);
		this.state.addUser(5);

		this.state.setA(1, 2, 0.1);
		this.state.setA(1, 3, 0.8);
		this.state.setA(1, 4, 0.3);
		this.state.setA(1, 5, 0.9);

		// Then
		Set<SimilarUser> similarUsers = this.state.getMostSimilarUsers(1, 2);
		assertEquals(2, similarUsers.size());
		assertTrue(similarUsers.contains(new SimilarUser(5, 0.9)));
		assertTrue(similarUsers.contains(new SimilarUser(3, 0.8)));
	}

	@Test
	public void testGetAndSetAverageRating() {
		// When
		double expectedAverageRating = 0.56;
		this.state.setAverageRating(1, expectedAverageRating);

		// Then
		double actualAverageRating = this.state.getAverageRating(1);
		assertEquals(expectedAverageRating, actualAverageRating, 10e-3);

		assertEquals(0.0, this.state.getAverageRating(6541), 10e-3);
	}

	@Test
	public void testSetAndGetCoRatedCount() {
		// When
		long expectedCount = 12;
		this.state.setCoRatedCount(1, 2, expectedCount);

		// Then
		long actualCoRatedCount = this.state.getCoRatedCount(1, 2);
		assertEquals(expectedCount, actualCoRatedCount);

		actualCoRatedCount = this.state.getCoRatedCount(2, 1);
		assertEquals(expectedCount, actualCoRatedCount);

		assertEquals(0, this.state.getCoRatedCount(1, 564));
	}

	@Test
	public void testSetAndGetCoRatedSums() {
		// When
		Map<Long, Double> expectedCoRatedSums = new HashMap<Long, Double>();
		expectedCoRatedSums.put(1L, 1.25d);
		expectedCoRatedSums.put(2L, 2.05d);
		this.state.setCoRatedSums(1, 2, expectedCoRatedSums);

		// Then
		Map<Long, Double> actualCoRatedSums = this.state.getCoRatedSums(1, 2);
		assertEquals(expectedCoRatedSums, actualCoRatedSums);

		actualCoRatedSums = this.state.getCoRatedSums(2, 1);
		assertEquals(expectedCoRatedSums, actualCoRatedSums);

		actualCoRatedSums = this.state.getCoRatedSums(1, 6541);
		assertEquals(0.0, actualCoRatedSums.get(1L), 10e-3);
		assertEquals(0.0, actualCoRatedSums.get(6541L), 10e-3);
	}
}

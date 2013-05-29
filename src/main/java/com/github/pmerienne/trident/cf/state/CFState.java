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

import java.util.Map;
import java.util.Set;

import storm.trident.state.State;

import com.github.pmerienne.trident.cf.model.SimilarUser;

public interface CFState extends State {

	/**
	 * Retrieve all users
	 * 
	 * @return
	 */
	Set<Long> getUsers();

	/**
	 * Retrieve the most similar users of a given user.
	 * 
	 * @param user
	 * @param count
	 * @return a {@link Set} of the most {@link SimilarUser}
	 */
	Set<SimilarUser> getMostSimilarUsers(long user, int count);

	/**
	 * Add a new user if it doesn't exist yet
	 * 
	 * @param user
	 */
	void addUser(long user);

	/**
	 * Return the number of users.
	 * 
	 * @return
	 */
	long userCount();

	/**
	 * Retrieve the cached number of rated items for a given user. Be aware that
	 * this value can be different from the size of the {@link Set} returned by
	 * {@link CFState#getUsers()}. This value is only modified by the method
	 * {@link CFState#setM(long, long)}!
	 * 
	 * @param user
	 * @return
	 */
	long getM(long user);

	/**
	 * Set the number of rated items for a given user. This value is cached
	 * 
	 * @param user
	 * @param count
	 */
	void setM(long user, long count);

	/**
	 * Set the cached average rating for a given user.
	 * 
	 * @param user
	 * @param newAverageRating
	 */
	void setAverageRating(long user, double newAverageRating);

	/**
	 * Retrieve the cached average rating for a given user.
	 * 
	 * @param user
	 * @return
	 */
	double getAverageRating(long user);

	/**
	 * Retrieve a user rating
	 * 
	 * @param user
	 * @param item
	 * @return
	 */
	Double getRating(long user, long item);

	/**
	 * Add a user rating
	 * 
	 * @param user
	 * @param item
	 * @param rating
	 */
	void addRating(long user, long item, double rating);

	/**
	 * Get all user ratings
	 * 
	 * @param user
	 * @return
	 */
	Map<Long, Double> getRatings(long user);

	/**
	 * Retrieve the cached co-rated count for a pair of user
	 * 
	 * @param user1
	 * @param user2
	 * @return
	 */
	long getCoRatedCount(long user1, long user2);

	/**
	 * Set the cached co-rated count for a pair of user
	 * 
	 * @param user1
	 * @param user2
	 * @param count
	 */
	void setCoRatedCount(long user1, long user2, long count);

	/**
	 * Retrieve cached co-rated sums for a pair of user
	 * 
	 * @param user1
	 * @param user2
	 * @return
	 */
	Map<Long, Double> getCoRatedSums(long user1, long user2);

	/**
	 * Set cached co-rated sums for a pair of user
	 * 
	 * @param user1
	 * @param user2
	 * @param coRatedSums
	 */
	void setCoRatedSums(long user1, long user2, Map<Long, Double> coRatedSums);

	/**
	 * Retrieve the cached similarity between 2 users
	 * 
	 * @param user1
	 * @param user2
	 * @return
	 */
	double getA(long user1, long user2);

	/**
	 * Set the cached similarity between 2 users
	 * 
	 * @param user1
	 * @param user2
	 * @param a
	 */
	void setA(long user1, long user2, double a);

	/**
	 * Retrieve cached B for a pair of user
	 * 
	 * @param user1
	 * @param user2
	 * @return
	 */
	double getB(long user1, long user2);

	/**
	 * Set cached B for a pair of user
	 * 
	 * @param user1
	 * @param user2
	 * @param b
	 */
	void setB(long user1, long user2, double b);

	/**
	 * Retrieve cached C for a pair of user
	 * 
	 * @param user1
	 * @param user2
	 * @return
	 */
	double getC(long user1, long user2);

	/**
	 * Set cached C for a pair of user
	 * 
	 * @param user1
	 * @param user2
	 * @param c
	 */
	void setC(long user1, long user2, double c);

	/**
	 * Retrieve cached D for a pair of user
	 * 
	 * @param user1
	 * @param user2
	 * @return
	 */
	double getD(long user1, long user2);

	/**
	 * Set cached D for a pair of user
	 * 
	 * @param user1
	 * @param user2
	 * @param d
	 */
	void setD(long user1, long user2, double d);

}

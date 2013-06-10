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

import java.util.Set;

import storm.trident.state.State;

import com.github.pmerienne.trident.cf.model.SimilarUser;

public interface CFState extends State {

	void addItems(Set<Long> items);

	Set<Long> getItems();

	long itemCount();

	long getNumItemsPreferedBy(long user1, long user2);

	void setNumItemsPreferedBy(long user1, long user2, long numItems);

	void addUsers(Set<Long> users);

	Set<Long> getUsers();

	long userCount();

	void setUserPreference(long user, long item);

	Set<Long> getUserPreferences(long user);

	Set<Long> getUsersWithPreferenceFor(long item);

	long getPreferenceCount(long user);

	void setSimilarity(long user1, long user2, double similarity);

	double getSimilarity(long user1, long user2);

	/**
	 * Retrieve the most similar users of a given user.
	 * 
	 * @param user
	 * @param count
	 * @return a {@link Set} of the most similar users
	 */
	Set<SimilarUser> getMostSimilarUsers(long user, int count);

	void drop();
}

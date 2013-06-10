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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.MapState;
import storm.trident.state.snapshot.Snapshottable;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.cf.model.SimilarUser;
import com.github.pmerienne.trident.cf.model.UserPair;
import com.github.pmerienne.trident.cf.util.KeysUtil;

public class MemoryCFState implements CFState {

	private Snapshottable<Set<Long>> userList;
	private Snapshottable<Set<Long>> itemList;
	private MapState<Long> coPreferenceCounts;
	private MapState<Set<Long>> userPreferences;
	private MapState<Double> similarities;

	public MemoryCFState() {
		this.itemList = new TransactionalMemoryMapState<Set<Long>>("itemList");
		this.userList = new TransactionalMemoryMapState<Set<Long>>("userList");
		this.coPreferenceCounts = new TransactionalMemoryMapState<Long>("coPreferenceCounts");
		this.userPreferences = new TransactionalMemoryMapState<Set<Long>>("coPreferenceCounts");
		this.similarities = new TransactionalMemoryMapState<Double>("similarities");
	}

	@Override
	public void beginCommit(Long txid) {
		this.itemList.beginCommit(txid);
		this.userList.beginCommit(txid);
		this.coPreferenceCounts.beginCommit(txid);
		this.userPreferences.beginCommit(txid);
		this.similarities.beginCommit(txid);
	}

	@Override
	public void commit(Long txid) {
		this.itemList.commit(txid);
		this.userList.commit(txid);
		this.coPreferenceCounts.commit(txid);
		this.userPreferences.commit(txid);
		this.similarities.commit(txid);
	}

	@Override
	public void addItems(Set<Long> items) {
		Set<Long> currentItems = this.getItems();
		currentItems.addAll(items);
		this.itemList.set(currentItems);
	}

	@Override
	public Set<Long> getItems() {
		Set<Long> items = this.itemList.get();
		return items == null ? new HashSet<Long>() : items;
	}

	@Override
	public long itemCount() {
		Set<Long> items = this.getItems();
		return items == null ? 0 : items.size();
	}

	@Override
	public void addUsers(Set<Long> users) {
		Set<Long> currentUsers = this.getUsers();
		currentUsers.addAll(users);
		this.userList.set(currentUsers);
	}

	@Override
	public Set<Long> getUsers() {
		Set<Long> users = this.userList.get();
		return users == null ? new HashSet<Long>() : users;
	}

	@Override
	public long userCount() {
		Set<Long> users = this.getUsers();
		return users == null ? 0 : users.size();
	}

	@Override
	public long getNumItemsPreferedBy(long user1, long user2) {
		Long count = get(this.coPreferenceCounts, new UserPair(user1, user2));
		return count == null ? 0 : count;
	}

	@Override
	public void setNumItemsPreferedBy(long user1, long user2, long numItems) {
		put(this.coPreferenceCounts, new UserPair(user1, user2), numItems);
	}

	@Override
	public void setUserPreference(long user, long item) {
		Set<Long> preferences = get(this.userPreferences, user);
		if (preferences == null) {
			preferences = new HashSet<Long>();
		}
		preferences.add(item);
		put(this.userPreferences, user, preferences);
	}

	@Override
	public Set<Long> getUserPreferences(long user) {
		Set<Long> preferences = get(this.userPreferences, user);
		return preferences == null ? new HashSet<Long>() : preferences;
	}

	@Override
	public boolean hasUserPreferenceFor(long user, long item) {
		Set<Long> preferences = get(this.userPreferences, user);
		return preferences == null ? false : preferences.contains(item);
	}
	
	@Override
	public long getPreferenceCount(long user) {
		Set<Long> preferences = get(this.userPreferences, user);
		return preferences == null ? 0 : preferences.size();
	}

	@Override
	public void setSimilarity(long user1, long user2, double similarity) {
		put(this.similarities, new UserPair(user1, user2), similarity);
	}

	@Override
	public double getSimilarity(long user1, long user2) {
		Double similarity = get(this.similarities, new UserPair(user1, user2));
		return similarity == null ? 0.0 : similarity;
	}

	@Override
	public Set<SimilarUser> getMostSimilarUsers(long user, int count) {
		// Inspired from
		// org.apache.mahout.cf.taste.impl.recommender.TopItems.topUsers()
		Queue<SimilarUser> topUsers = new PriorityQueue<SimilarUser>(count + 1, Collections.reverseOrder());

		boolean full = false;
		double lowestTopValue = Double.NEGATIVE_INFINITY;
		Iterator<Long> allUserIDs = this.getUsers().iterator();
		while (allUserIDs.hasNext()) {
			long user2 = allUserIDs.next();
			double similarity = this.getSimilarity(user, user2);
			if (!Double.isNaN(similarity) && (!full || similarity > lowestTopValue)) {
				topUsers.add(new SimilarUser(user2, similarity));
				if (full) {
					topUsers.poll();
				} else if (topUsers.size() > count) {
					full = true;
					topUsers.poll();
				}
				lowestTopValue = topUsers.peek().getSimilarity();
			}
		}

		int size = topUsers.size();
		if (size == 0) {
			return new HashSet<SimilarUser>();
		}

		List<SimilarUser> sorted = new ArrayList<SimilarUser>(topUsers);
		Collections.sort(sorted);
		Set<SimilarUser> result = new HashSet<SimilarUser>(sorted);

		return result;
	}

	@Override
	public void drop() {
		TransactionalMemoryMapState.MemoryMapStateBacking.clearAll();
	}

	protected static <T> T get(MapState<T> delegate, Object key) {
		List<List<Object>> keys = KeysUtil.toKeys(key);
		List<T> values = delegate.multiGet(keys);
		T value = KeysUtil.singleValue(values);
		return value;
	}

	protected static <T> void put(MapState<T> delegate, Object key, T value) {
		List<List<Object>> keys = KeysUtil.toKeys(key);
		List<T> values = new ArrayList<T>(1);
		values.add(value);
		delegate.multiPut(keys, values);
	}

	public static class Factory implements StateFactory {

		private static final long serialVersionUID = 4718043951532492603L;

		@SuppressWarnings("rawtypes")
		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new MemoryCFState();
		}
	}
}

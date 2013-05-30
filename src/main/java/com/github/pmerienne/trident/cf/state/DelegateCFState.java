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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import storm.trident.state.map.MapState;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.snapshot.Snapshottable;
import util.KeysUtil;

import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.model.SimilarUser;

public abstract class DelegateCFState implements CFState {

	protected MapState<Double> as;
	protected MapState<Double> bs;
	protected MapState<Double> cs;
	protected MapState<Double> ds;
	protected MapState<Map<Long, Double>> coRatedSums;
	protected MapState<Long> coRatedCounts;

	protected MapState<Long> ms;
	protected MapState<Double> averageRatings;

	protected MapState<Double> ratings;
	protected MapState<Map<Long, Double>> perUserRatings;

	private Snapshottable<Set<Long>> users;
	
	public DelegateCFState() {
		this.initMapStates();
	}

	protected void initMapStates() {
		this.as = this.createMapState("as");
		this.bs = this.createMapState("bs");
		this.cs = this.createMapState("cs");
		this.ds = this.createMapState("ds");
		this.coRatedSums = this.createMapState("coRatedSums");
		this.coRatedCounts = this.createMapState("coRatedCounts");
		this.ms = this.createMapState("ms");
		this.averageRatings = this.createMapState("averageRatings");
		this.ratings = this.createMapState("ratings");
		this.perUserRatings = this.createMapState("perUserRatings");
		this.users = this.createSnapshottableMapState("users");
	}

	@Override
	public void beginCommit(Long txid) {
		this.as.beginCommit(txid);
		this.bs.beginCommit(txid);
		this.cs.beginCommit(txid);
		this.ds.beginCommit(txid);
		this.coRatedSums.beginCommit(txid);
		this.coRatedCounts.beginCommit(txid);
		this.ms.beginCommit(txid);
		this.averageRatings.beginCommit(txid);
		this.ratings.beginCommit(txid);
		this.perUserRatings.beginCommit(txid);
		this.users.beginCommit(txid);
	}

	@Override
	public void commit(Long txid) {
		this.as.commit(txid);
		this.bs.commit(txid);
		this.cs.commit(txid);
		this.ds.commit(txid);
		this.coRatedSums.commit(txid);
		this.coRatedCounts.commit(txid);
		this.ms.commit(txid);
		this.averageRatings.commit(txid);
		this.ratings.commit(txid);
		this.perUserRatings.commit(txid);
		this.users.commit(txid);
	}

	@Override
	public Set<Long> getUsers() {
		Set<Long> users = this.users.get();
		if (users == null) {
			users = new HashSet<Long>();
		}
		return users;
	}

	@Override
	public void addUser(long user) {
		Set<Long> users = this.users.get();
		if (users == null) {
			users = new HashSet<Long>();
		}
		users.add(user);
		this.users.set(users);
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
			double similarity = this.getA(user, user2);
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
	public long userCount() {
		return this.getUsers().size();
	}

	@Override
	public long getM(long user) {
		Long m = get(this.ms, user);
		return m == null ? 0 : m;
	}

	@Override
	public void setM(long user, long count) {
		put(this.ms, user, count);
	}

	@Override
	public void setAverageRating(long user, double newAverageRating) {
		put(this.averageRatings, user, newAverageRating);
	}

	@Override
	public double getAverageRating(long user) {
		Double averageRating = get(this.averageRatings, user);
		return averageRating == null ? 0.0 : averageRating;
	}

	@Override
	public Double getRating(long user, long item) {
		return get(this.ratings, new UserItem(user, item));
	}

	@Override
	public void addRating(long user, long item, double rating) {
		put(this.ratings, new UserItem(user, item), rating);

		Map<Long, Double> userRatings = get(this.perUserRatings, user);
		if (userRatings == null) {
			userRatings = new HashMap<Long, Double>();
		}
		userRatings.put(item, rating);
		put(this.perUserRatings, user, userRatings);
	}

	@Override
	public Map<Long, Double> getRatings(long user) {
		Map<Long, Double> userRatings = get(this.perUserRatings, user);
		return userRatings == null ? new HashMap<Long, Double>() : userRatings;
	}

	@Override
	public long getCoRatedCount(long user1, long user2) {
		Long count = get(this.coRatedCounts, new UserPair(user1, user2));
		return count == null ? 0 : count;
	}

	@Override
	public void setCoRatedCount(long user1, long user2, long count) {
		put(this.coRatedCounts, new UserPair(user1, user2), count);
	}

	@Override
	public Map<Long, Double> getCoRatedSums(long user1, long user2) {
		Map<Long, Double> coRatedSums = get(this.coRatedSums, new UserPair(user1, user2));
		if (coRatedSums == null) {
			coRatedSums = new HashMap<Long, Double>();
			coRatedSums.put(user1, 0.0);
			coRatedSums.put(user2, 0.0);
		}
		return coRatedSums;
	}

	@Override
	public void setCoRatedSums(long user1, long user2, Map<Long, Double> coRatedSums) {
		put(this.coRatedSums, new UserPair(user1, user2), coRatedSums);
	}

	@Override
	public double getA(long user1, long user2) {
		Double a = get(this.as, new UserPair(user1, user2));
		return a == null ? -1 : a;
	}

	@Override
	public void setA(long user1, long user2, double a) {
		put(this.as, new UserPair(user1, user2), a);
	}

	@Override
	public double getB(long user1, long user2) {
		Double value = get(this.bs, new UserPair(user1, user2));
		return value == null ? 0 : value;
	}

	@Override
	public void setB(long user1, long user2, double b) {
		put(this.bs, new UserPair(user1, user2), b);
	}

	@Override
	public double getC(long user1, long user2) {
		Double value = get(this.cs, new UserPair(user1, user2));
		return value == null ? 0 : value;
	}

	@Override
	public void setC(long user1, long user2, double c) {
		put(this.cs, new UserPair(user1, user2), c);
	}

	@Override
	public double getD(long user1, long user2) {
		Double value = get(this.ds, new UserPair(user1, user2));
		return value == null ? 0 : value;
	}

	@Override
	public void setD(long user1, long user2, double d) {
		put(this.ds, new UserPair(user1, user2), d);
	}

	protected abstract <T> MapState<T> createMapState(String id);

	protected <T> Snapshottable<T> createSnapshottableMapState(String id) {
		MapState<T> delegate = this.createMapState(id);
		return new SnapshottableMap<T>(delegate, new Values("$CF-MAP-STATE-GLOBAL$"));
	}

	protected static <T> T get(MapState<T> mapState, Object key) {
		List<List<Object>> keys = KeysUtil.toKeys(key);
		List<T> values = mapState.multiGet(keys);
		T value = singleValue(values);
		return value;
	}

	protected static <T> void put(MapState<T> mapState, Object key, T value) {
		List<List<Object>> keys = KeysUtil.toKeys(key);
		List<T> values = new ArrayList<T>(1);
		values.add(value);
		mapState.multiPut(keys, values);
	}

	protected static <T> T singleValue(List<T> values) {
		return values.get(0);
	}

	private static class UserPair {

		protected final long user1;
		protected final long user2;

		public UserPair(long user1, long user2) {
			this.user1 = Math.min(user1, user2);
			this.user2 = Math.max(user1, user2);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (user1 ^ (user1 >>> 32));
			result = prime * result + (int) (user2 ^ (user2 >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			UserPair other = (UserPair) obj;
			if (user1 != other.user1)
				return false;
			if (user2 != other.user2)
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "UserPair [user1=" + user1 + ", user2=" + user2 + "]";
		}

	}

	private static class UserItem {

		private final long user;
		private final long item;

		public UserItem(long user, long item) {
			this.user = user;
			this.item = item;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (item ^ (item >>> 32));
			result = prime * result + (int) (user ^ (user >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			UserItem other = (UserItem) obj;
			if (item != other.item)
				return false;
			if (user != other.user)
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "UserItem [user=" + user + ", item=" + item + "]";
		}

	}

}

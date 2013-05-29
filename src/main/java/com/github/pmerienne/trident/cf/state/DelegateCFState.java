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
import storm.trident.state.snapshot.Snapshottable;
import storm.trident.testing.MemoryMapState;
import util.KeysUtil;

import com.github.pmerienne.trident.cf.model.SimilarUser;

public abstract class DelegateCFState implements CFState {

	@Override
	public void beginCommit(Long txid) {
		this.getAsMapState().beginCommit(txid);
		this.getBsMapState().beginCommit(txid);
		this.getCsMapState().beginCommit(txid);
		this.getDsMapState().beginCommit(txid);
		this.getCoRatedSumsMapState().beginCommit(txid);
		this.getCoRatedCountsMapState().beginCommit(txid);
		this.getMsMapState().beginCommit(txid);
		this.getAverageRatingsMapState().beginCommit(txid);
		this.getRatingsMapState().beginCommit(txid);
		this.getPerUserRatingsMapState().beginCommit(txid);
		this.getUsersMapState().beginCommit(txid);
	}

	@Override
	public void commit(Long txid) {
		this.getAsMapState().commit(txid);
		this.getBsMapState().commit(txid);
		this.getCsMapState().commit(txid);
		this.getDsMapState().commit(txid);
		this.getCoRatedSumsMapState().commit(txid);
		this.getCoRatedCountsMapState().commit(txid);
		this.getMsMapState().commit(txid);
		this.getAverageRatingsMapState().commit(txid);
		this.getRatingsMapState().commit(txid);
		this.getPerUserRatingsMapState().commit(txid);
		this.getUsersMapState().commit(txid);
	}

	@Override
	public Set<Long> getUsers() {
		Snapshottable<Set<Long>> snapshottable = this.getUsersMapState();
		Set<Long> users = snapshottable.get();
		if(users == null) {
			users = new HashSet<Long>();
		}
		return users;
	}

	@Override
	public void addUser(long user) {
		Snapshottable<Set<Long>> snapshottable = this.getUsersMapState();
		Set<Long> users = snapshottable.get();
		if(users == null) {
			users = new HashSet<Long>();
		}
		users.add(user);
		snapshottable.set(users);
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
		MapState<Long> mapState = this.getMsMapState();
		Long m = get(mapState, user);
		return m == null ? 0 : m;
	}

	@Override
	public void setM(long user, long count) {
		MapState<Long> mapState = this.getMsMapState();
		put(mapState, user, count);
	}

	@Override
	public void setAverageRating(long user, double newAverageRating) {
		MapState<Double> mapState = this.getAverageRatingsMapState();
		put(mapState, user, newAverageRating);
	}

	@Override
	public double getAverageRating(long user) {
		MapState<Double> mapState = this.getAverageRatingsMapState();
		Double averageRating = get(mapState, user);
		return averageRating == null ? 0.0 : averageRating;
	}

	@Override
	public Double getRating(long user, long item) {
		MapState<Double> mapState = this.getRatingsMapState();
		return get(mapState, new UserItem(user, item));
	}

	@Override
	public void addRating(long user, long item, double rating) {
		MapState<Double> ratingsMapState = this.getRatingsMapState();
		put(ratingsMapState, new UserItem(user, item), rating);

		MapState<Map<Long, Double>> perUserRatingsMapState = this.getPerUserRatingsMapState();
		Map<Long, Double> userRatings = get(perUserRatingsMapState, user);
		if (userRatings == null) {
			userRatings = new HashMap<Long, Double>();
		}
		userRatings.put(item, rating);
		put(perUserRatingsMapState, user, userRatings);
	}

	@Override
	public Map<Long, Double> getRatings(long user) {
		MapState<Map<Long, Double>> perUserRatingsMapState = this.getPerUserRatingsMapState();
		Map<Long, Double> userRatings = get(perUserRatingsMapState, user);
		return userRatings == null ? new HashMap<Long, Double>() : userRatings;
	}

	@Override
	public long getCoRatedCount(long user1, long user2) {
		MapState<Long> mapState = this.getCoRatedCountsMapState();
		Long count = get(mapState, new UserPair(user1, user2));
		return count == null ? 0 : count;
	}

	@Override
	public void setCoRatedCount(long user1, long user2, long count) {
		MapState<Long> mapState = this.getCoRatedCountsMapState();
		put(mapState, new UserPair(user1, user2), count);
	}

	@Override
	public Map<Long, Double> getCoRatedSums(long user1, long user2) {
		MapState<Map<Long, Double>> mapState = this.getCoRatedSumsMapState();
		Map<Long, Double> coRatedSums = get(mapState, new UserPair(user1, user2));
		if (coRatedSums == null) {
			coRatedSums = new HashMap<Long, Double>();
			coRatedSums.put(user1, 0.0);
			coRatedSums.put(user2, 0.0);
		}
		return coRatedSums;
	}

	@Override
	public void setCoRatedSums(long user1, long user2, Map<Long, Double> coRatedSums) {
		MapState<Map<Long, Double>> mapState = this.getCoRatedSumsMapState();
		put(mapState, new UserPair(user1, user2), coRatedSums);
	}

	@Override
	public double getA(long user1, long user2) {
		MapState<Double> mapState = this.getAsMapState();
		Double a = get(mapState, new UserPair(user1, user2));
		return a == null ? -1 : a;
	}

	@Override
	public void setA(long user1, long user2, double a) {
		MapState<Double> mapState = this.getAsMapState();
		put(mapState, new UserPair(user1, user2), a);
	}

	@Override
	public double getB(long user1, long user2) {
		MapState<Double> mapState = this.getBsMapState();
		Double value = get(mapState, new UserPair(user1, user2));
		return value == null ? 0 : value;
	}

	@Override
	public void setB(long user1, long user2, double b) {
		MapState<Double> mapState = this.getBsMapState();
		put(mapState, new UserPair(user1, user2), b);
	}

	@Override
	public double getC(long user1, long user2) {
		MapState<Double> mapState = this.getCsMapState();
		Double value = get(mapState, new UserPair(user1, user2));
		return value == null ? 0 : value;
	}

	@Override
	public void setC(long user1, long user2, double c) {
		MapState<Double> mapState = this.getCsMapState();
		put(mapState, new UserPair(user1, user2), c);
	}

	@Override
	public double getD(long user1, long user2) {
		MapState<Double> mapState = this.getDsMapState();
		Double value = get(mapState, new UserPair(user1, user2));
		return value == null ? 0 : value;
	}

	@Override
	public void setD(long user1, long user2, double d) {
		MapState<Double> mapState = this.getDsMapState();
		put(mapState, new UserPair(user1, user2), d);
	}

	protected abstract MemoryMapState<Double> getAsMapState();

	protected abstract MemoryMapState<Double> getBsMapState();

	protected abstract MemoryMapState<Double> getCsMapState();

	protected abstract MemoryMapState<Double> getDsMapState();

	protected abstract MemoryMapState<Map<Long, Double>> getCoRatedSumsMapState();

	protected abstract MemoryMapState<Long> getCoRatedCountsMapState();

	protected abstract MemoryMapState<Long> getMsMapState();

	protected abstract MemoryMapState<Double> getAverageRatingsMapState();

	protected abstract MemoryMapState<Double> getRatingsMapState();

	protected abstract MemoryMapState<Map<Long, Double>> getPerUserRatingsMapState();

	protected abstract Snapshottable<Set<Long>> getUsersMapState();

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

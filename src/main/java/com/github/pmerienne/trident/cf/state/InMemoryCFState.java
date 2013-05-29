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

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.cf.model.SimilarUser;

public class InMemoryCFState implements CFState {

	protected Map<UserPair, Double> as = new HashMap<UserPair, Double>();
	protected Map<UserPair, Double> bs = new HashMap<UserPair, Double>();
	protected Map<UserPair, Double> cs = new HashMap<UserPair, Double>();
	protected Map<UserPair, Double> ds = new HashMap<UserPair, Double>();
	protected Map<UserPair, Map<Long, Double>> coRatedSum = new HashMap<UserPair, Map<Long, Double>>();
	protected Map<UserPair, Long> coRatedCount = new HashMap<UserPair, Long>();

	protected Map<Long, Long> ms = new HashMap<Long, Long>();
	protected Map<Long, Double> averageRatings = new HashMap<Long, Double>();

	protected Map<Pair<Long>, Double> ratings = new HashMap<Pair<Long>, Double>();
	protected Map<Long, Map<Long, Double>> perUserRatings = new HashMap<Long, Map<Long, Double>>();

	private Set<Long> users = new HashSet<Long>();

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}

	@Override
	public Set<Long> getUsers() {
		return this.users;
	}

	@Override
	public Set<SimilarUser> getMostSimilarUsers(long user1, int count) {
		// Inspired from
		// org.apache.mahout.cf.taste.impl.recommender.TopItems.topUsers()
		Queue<SimilarUser> topUsers = new PriorityQueue<SimilarUser>(count + 1, Collections.reverseOrder());

		boolean full = false;
		double lowestTopValue = Double.NEGATIVE_INFINITY;
		Iterator<Long> allUserIDs = this.users.iterator();
		while (allUserIDs.hasNext()) {
			long user2 = allUserIDs.next();
			double similarity = this.getA(user1, user2);
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
	public void addUser(long user) {
		this.users.add(user);
	}

	@Override
	public long userCount() {
		return this.users.size();
	}

	@Override
	public long getM(long user) {
		Long ratedItemCount = this.ms.get(user);
		if (ratedItemCount == null) {
			ratedItemCount = 0L;
		}
		return ratedItemCount;
	}

	@Override
	public void setM(long user, long count) {
		this.ms.put(user, count);
	}

	@Override
	public void setAverageRating(long user, double newAverageRating) {
		this.averageRatings.put(user, newAverageRating);
	}

	@Override
	public double getAverageRating(long user) {
		Double averageRating = this.averageRatings.get(user);
		if (averageRating == null) {
			averageRating = 0.0;
		}
		return averageRating;
	}

	@Override
	public Double getRating(long user, long item) {
		return this.ratings.get(new Pair<Long>(user, item));
	}

	@Override
	public void addRating(long user, long item, double rating) {
		this.ratings.put(new Pair<Long>(user, item), rating);

		Map<Long, Double> userRatings = this.perUserRatings.get(user);
		if (userRatings == null) {
			userRatings = new HashMap<Long, Double>();
		}
		userRatings.put(item, rating);
		this.perUserRatings.put(user, userRatings);
	}

	@Override
	public Map<Long, Double> getRatings(long user) {
		return this.perUserRatings.get(user);
	}

	@Override
	public long getCoRatedCount(long user1, long user2) {
		Long coRatedCount = this.coRatedCount.get(new UserPair(user1, user2));
		if (coRatedCount == null) {
			coRatedCount = 0L;
		}
		return coRatedCount;
	}

	@Override
	public void setCoRatedCount(long user1, long user2, long count) {
		this.coRatedCount.put(new UserPair(user1, user2), count);
	}

	@Override
	public Map<Long, Double> getCoRatedSums(long user1, long user2) {
		Map<Long, Double> sums = this.coRatedSum.get(new UserPair(user1, user2));
		if (sums == null) {
			sums = new HashMap<Long, Double>();
			sums.put(user1, 0.0);
			sums.put(user2, 0.0);
		}
		return sums;
	}

	@Override
	public void setCoRatedSums(long user1, long user2, Map<Long, Double> coRatedSums) {
		this.coRatedSum.put(new UserPair(user1, user2), coRatedSums);
	}

	@Override
	public double getA(long user1, long user2) {
		Double a = this.as.get(new UserPair(user1, user2));
		if (a == null) {
			a = -1.0;
		}
		return a;
	}

	@Override
	public void setA(long user1, long user2, double a) {
		this.as.put(new UserPair(user1, user2), a);
	}

	@Override
	public double getB(long user1, long user2) {
		Double b = this.bs.get(new UserPair(user1, user2));
		if (b == null) {
			b = 0.0;
		}
		return b;
	}

	@Override
	public void setB(long user1, long user2, double b) {
		this.bs.put(new UserPair(user1, user2), b);
	}

	@Override
	public double getC(long user1, long user2) {
		Double c = this.cs.get(new UserPair(user1, user2));
		if (c == null) {
			c = 0.0;
		}
		return c;
	}

	@Override
	public void setC(long user1, long user2, double c) {
		this.cs.put(new UserPair(user1, user2), c);
	}

	@Override
	public double getD(long user1, long user2) {
		Double d = this.ds.get(new UserPair(user1, user2));
		if (d == null) {
			d = 0.0;
		}
		return d;
	}

	@Override
	public void setD(long user1, long user2, double d) {
		this.ds.put(new UserPair(user1, user2), d);
	}

	@SuppressWarnings("serial")
	public static class Factory implements StateFactory {

		private final static CFState STATE = new InMemoryCFState();

		@SuppressWarnings("rawtypes")
		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return STATE;
		}
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

	private static class Pair<L> {

		private final L o1;
		private final L o2;

		public Pair(L o1, L o2) {
			this.o1 = o1;
			this.o2 = o2;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((o1 == null) ? 0 : o1.hashCode());
			result = prime * result + ((o2 == null) ? 0 : o2.hashCode());
			return result;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Pair other = (Pair) obj;
			if (o1 == null) {
				if (other.o1 != null)
					return false;
			} else if (!o1.equals(other.o1))
				return false;
			if (o2 == null) {
				if (other.o2 != null)
					return false;
			} else if (!o2.equals(other.o2))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "Pair [o1=" + o1 + ", o2=" + o2 + "]";
		}

	}

}

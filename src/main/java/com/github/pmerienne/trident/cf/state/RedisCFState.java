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
import storm.trident.state.StateFactory;

import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.cf.model.SimilarUser;

public class RedisCFState extends DummyRedisCFState implements CFState {

	public RedisCFState() {
		super();
	}

	public RedisCFState(String host, int port) {
		super(host, port);
	}

	@SuppressWarnings("rawtypes")
	public RedisCFState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
		super(conf, metrics, partitionIndex, numPartitions);
	}

	@Override
	public void addRating(long user, long item, double rating) {
		super.addRating(user, item, rating);
	}

	@Override
	public Double getRating(long user, long item) {
		return super.getRating(user, item);
	}

	@Override
	public Map<Long, Double> getRatings(long user) {
		return super.getRatings(user);
	}

	@Override
	public Set<Long> getUsers() {
		return super.getUsers();
	}

	@Override
	public void addUser(long user) {
		super.addUser(user);
	}

	@Override
	public double getA(long user1, long user2) {
		return super.getA(user1, user2);
	}

	@Override
	public void setA(long user1, long user2, double a) {
		super.setA(user1, user2, a);
	}

	@Override
	public Set<SimilarUser> getMostSimilarUsers(long user, int count) {
		return super.getMostSimilarUsers(user, count);
	}

	public static class Factory implements StateFactory {

		private String host;
		private Integer port;

		public Factory() {
		}

		public Factory(String host, Integer port) {
			this.host = host;
			this.port = port;
		}

		private static final long serialVersionUID = 4718043951532492603L;

		@SuppressWarnings("rawtypes")
		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			RedisCFState cfState;
			if (this.host != null && this.port != null) {
				cfState = new RedisCFState(this.host, this.port);
			} else {
				cfState = new RedisCFState();
			}
			return cfState;
		}
	}

}

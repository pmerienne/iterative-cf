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
package com.github.pmerienne.trident.cf;

import java.util.List;
import java.util.Map;

import com.github.pmerienne.trident.cf.state.CFState;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class UpdateUserPairCache extends BaseStateUpdater<CFState> {

	private static final long serialVersionUID = -7860608839049663486L;

	@Override
	public void updateState(CFState state, List<TridentTuple> tuples, TridentCollector collector) {
		for (TridentTuple tuple : tuples) {
			this.updateCoRatings(state, tuple, collector);
		}
	}

	protected void updateCoRatings(CFState state, TridentTuple tuple, TridentCollector collector) {
		long user1 = this.getUser1(tuple);
		long user2 = this.getUser2(tuple);
		long item = this.getItem(tuple);
		Double user1OldRating = this.getOldRating(tuple);
		double user1NewRating = this.getNewRating(tuple);

		Double user2Rating = state.getRating(user2, item);

		// Update co-rating
		Map<Long, Double> coRatedSums = state.getCoRatedSums(user1, user2);
		long oldCoRatedCount = state.getCoRatedCount(user1, user2);
		double oldCoRatedSum1 = coRatedSums.get(user1);
		double oldCoRatedSum2 = coRatedSums.get(user2);
		if (user2Rating != null) {
			if (user1OldRating == null) {
				// Update count
				state.setCoRatedCount(user1, user2, oldCoRatedCount + 1);

				// Update co rated sums
				coRatedSums.put(user1, oldCoRatedSum1 + user1NewRating);
				coRatedSums.put(user2, oldCoRatedSum2 + user2Rating);
				state.setCoRatedSums(user1, user2, coRatedSums);
			} else {
				// Update co rated sums
				coRatedSums.put(user1, oldCoRatedSum1 + (user1NewRating - user1OldRating));
				state.setCoRatedSums(user1, user2, coRatedSums);
			}
		}

		// TODO should be here, but there no other choice ...
		// Update rating
		state.addRating(user1, item, user1NewRating);
		
		// Emit values for new value stream
		double newAverageRating = this.getNewAverageRating(tuple);
		double oldAverageRating = this.getOldAverageRating(tuple);
		collector.emit(new Values(user1, user2, item, user1NewRating, user1OldRating, user2Rating, newAverageRating , oldAverageRating , oldCoRatedCount, oldCoRatedSum1, oldCoRatedSum2));
	}

	protected long getUser1(TridentTuple tuple) {
		return tuple.getLong(0);
	}

	protected long getUser2(TridentTuple tuple) {
		return tuple.getLong(1);
	}

	protected long getItem(TridentTuple tuple) {
		return tuple.getLong(2);
	}

	private double getNewRating(TridentTuple tuple) {
		return tuple.getDouble(3);
	}

	private Double getOldRating(TridentTuple tuple) {
		return tuple.getDouble(4);
	}

	private double getNewAverageRating(TridentTuple tuple) {
		return tuple.getDouble(5);
	}

	private Double getOldAverageRating(TridentTuple tuple) {
		return tuple.getDouble(6);
	}
}

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

import com.github.pmerienne.trident.cf.state.CFState;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

public class UpdateUserSimilarity extends BaseStateUpdater<CFState> {

	private static final long serialVersionUID = -6168810550629879261L;

	@Override
	public void updateState(CFState state, List<TridentTuple> tuples, TridentCollector collector) {
		for (TridentTuple tuple : tuples) {
			this.updateUserSimilarity(state, tuple, collector);
		}
	}

	public void updateUserSimilarity(CFState state, TridentTuple tuple, TridentCollector collector) {
		long user1 = this.getUser1(tuple);
		long user2 = this.getUser2(tuple);
		@SuppressWarnings("unused")
		long item = this.getItem(tuple);

		double user1NewRating = this.getUser1NewRating(tuple);
		Double user1OldRating = this.getUser1OldRating(tuple);
		Double user2Rating = this.getUser2Rating(tuple);

		double user1NewAverageRating = this.getNewAverageRating(tuple);
		double user1OldAverageRating = this.getOldAverageRating(tuple);
		double averageRatingDifference = user1NewAverageRating - user1OldAverageRating;
		double user2AverageRating = state.getAverageRating(user2);

		long coRatedCount = this.getOldCoRatedCount(tuple);
		double coRatedSum1 = this.getOldCoRatedSum1(tuple);
		double coRatedSum2 = this.getOldCoRatedSum2(tuple);

		double magic1 = averageRatingDifference * (coRatedSum1 - coRatedCount * user1OldAverageRating);
		double magic2 = averageRatingDifference * (coRatedSum2 - coRatedCount * user2AverageRating);
		double e = 0, f = 0, g = 0;
		if (user1OldRating == null) {
			if (user2Rating != null) {
				// Submission of a new rating and uy had rated ia
				e = (user1NewRating - user1NewAverageRating) * (user2Rating - user2AverageRating) - magic2;
				f = Math.pow(user1NewRating - user1NewAverageRating, 2) + coRatedCount * Math.pow(averageRatingDifference, 2) - 2 * magic1;
				g = Math.pow(user2Rating - user2AverageRating, 2);
			} else {
				// Submission of a new rating and uy had not rated ia
				e = -magic2;
				f = coRatedCount * Math.pow(averageRatingDifference, 2) - 2 * magic1;
				g = 0.0;
			}
		} else {
			double druaia = user1NewRating - user1OldRating;
			if (user2Rating != null) {
				// Update of an existing rating and uy had rated ia
				e = druaia * (user2Rating - user2AverageRating) - magic2;
				f = Math.pow(druaia, 2) + 2 * druaia * (user1OldRating - user1NewAverageRating) + coRatedCount * Math.pow(averageRatingDifference, 2) - 2 * magic1;
				g = 0.0;
			} else {
				// Update of an existing rating and uy had not rated ia
				e = -magic2;
				f = coRatedCount * Math.pow(averageRatingDifference, 2) - 2 * magic1;
				g = 0.0;
			}
		}

		// Update cached a, b, c, d
		Double b = state.getB(user1, user2);
		Double c = state.getC(user1, user2);
		Double d = state.getD(user1, user2);
		b = b + e;
		c = c + f;
		d = d + g;
		state.setA(user1, user2, b / (Math.sqrt(c) * Math.sqrt(d)));
		state.setB(user1, user2, b);
		state.setC(user1, user2, c);
		state.setD(user1, user2, d);
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

	private double getUser1NewRating(TridentTuple tuple) {
		return tuple.getDouble(3);
	}

	private Double getUser1OldRating(TridentTuple tuple) {
		return tuple.getDouble(4);
	}

	private Double getUser2Rating(TridentTuple tuple) {
		return tuple.getDouble(5);
	}

	private double getNewAverageRating(TridentTuple tuple) {
		return tuple.getDouble(6);
	}

	private Double getOldAverageRating(TridentTuple tuple) {
		return tuple.getDouble(7);
	}

	private long getOldCoRatedCount(TridentTuple tuple) {
		return tuple.getLong(8);
	}

	private double getOldCoRatedSum1(TridentTuple tuple) {
		return tuple.getDouble(9);
	}

	private double getOldCoRatedSum2(TridentTuple tuple) {
		return tuple.getDouble(10);
	}
}

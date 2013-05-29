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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

import com.github.pmerienne.trident.cf.model.WeightedRating;
import com.github.pmerienne.trident.cf.model.WeightedRatings;

public class UnratedItemsCombiner implements CombinerAggregator<WeightedRatings> {

	private static final long serialVersionUID = 7721467172409954246L;

	@SuppressWarnings("unchecked")
	@Override
	public WeightedRatings init(TridentTuple tuple) {
		Map<Long, Double> user1Ratings = (Map<Long, Double>) tuple.get(0);
		Map<Long, Double> user2Ratings = (Map<Long, Double>) tuple.get(1);
		double similarity = tuple.getDouble(2);

		WeightedRatings weightedRatings = new WeightedRatings();
		for (Long item : user2Ratings.keySet()) {
			if (user1Ratings == null || !user1Ratings.containsKey(item)) {
				weightedRatings.addRating(item, user2Ratings.get(item), similarity);
			}
		}

		return weightedRatings;
	}

	@Override
	public WeightedRatings combine(WeightedRatings val1, WeightedRatings val2) {
		WeightedRatings newWeightedRatings = new WeightedRatings();

		Set<Long> items = new HashSet<Long>();
		items.addAll(val1.getItems());
		items.addAll(val2.getItems());

		WeightedRating weightedRating1, weightedRating2, newWeightedRating;
		for (long item : items) {
			weightedRating1 = val1.getRatings().get(item);
			weightedRating2 = val2.getRatings().get(item);

			if (weightedRating1 == null) {
				newWeightedRating = weightedRating2;
			} else if (weightedRating2 == null) {
				newWeightedRating = weightedRating2;
			} else {
				newWeightedRating = new WeightedRating(weightedRating1.normalizingFactor + weightedRating2.normalizingFactor, weightedRating1.weightedSum + weightedRating2.weightedSum);
			}

			newWeightedRatings.getRatings().put(item, newWeightedRating);
		}

		return newWeightedRatings;
	}

	@Override
	public WeightedRatings zero() {
		return new WeightedRatings();
	}

}

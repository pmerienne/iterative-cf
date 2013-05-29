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
package com.github.pmerienne.trident.cf.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WeightedRatings implements Serializable {

	private static final long serialVersionUID = -3814769516554929611L;

	private final Map<Long, WeightedRating> ratings = new HashMap<Long, WeightedRating>();

	public void addRating(long item, double rating, double similarity) {
		WeightedRating weightedRating = this.ratings.get(item);
		if (weightedRating == null) {
			weightedRating = new WeightedRating();
		}
		this.ratings.put(item, weightedRating.addRatting(rating, similarity));
	}

	public Set<Long> getItems() {
		return this.ratings.keySet();
	}

	public Double getRecommendation(long item) {
		WeightedRating weightedRating = this.ratings.get(item);
		return weightedRating == null ? null : weightedRating.getWeightedRating();
	}

	public Map<Long, WeightedRating> getRatings() {
		return ratings;
	}

	@Override
	public String toString() {
		return "WeightedRatings [ratings=" + ratings + "]";
	}

}

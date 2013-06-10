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
import java.util.Map.Entry;
import java.util.Set;

public class WeightedPreferences implements Serializable {

	private static final long serialVersionUID = -3814769516554929611L;

	protected Map<Long, Double> preferenceScores;
	protected double scoreSum = 0.0;

	public WeightedPreferences() {
		this(new HashMap<Long, Double>());
	}

	public WeightedPreferences(Map<Long, Double> preferenceScores) {
		this.preferenceScores = preferenceScores;
	}

	public WeightedPreferences(Map<Long, Double> preferenceScores, double scoreSum) {
		this.preferenceScores = preferenceScores;
		this.scoreSum = scoreSum;
	}

	public void addAll(Set<Long> items, double similarity) {
		for (long item : items) {
			this.add(item, similarity);
		}
	}

	public void add(long item, double similarity) {
		this.scoreSum += similarity;
		Double actualScore = this.preferenceScores.get(item);
		if (actualScore == null) {
			actualScore = 0.0;
		}

		this.preferenceScores.put(item, actualScore + similarity);
	}

	public Set<Long> getItems() {
		return this.preferenceScores.keySet();
	}

	public Double getRecommendation(long item) {
		Double actualScore = this.preferenceScores.get(item);
		if (actualScore == null) {
			actualScore = 0.0;
		}
		return this.scoreSum == 0.0 ? 0.0 : actualScore / this.scoreSum;
	}

	public static WeightedPreferences combine(WeightedPreferences val1, WeightedPreferences val2) {
		WeightedPreferences newWeightedPreferences = new WeightedPreferences(val1.preferenceScores, val1.scoreSum);

		for (Entry<Long, Double> otherScore : val2.preferenceScores.entrySet()) {
			newWeightedPreferences.add(otherScore.getKey(), otherScore.getValue());
		}

		return newWeightedPreferences;
	}

}

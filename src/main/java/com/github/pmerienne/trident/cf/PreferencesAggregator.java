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

import java.util.Set;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

import com.github.pmerienne.trident.cf.model.WeightedPreferences;

public class PreferencesAggregator implements CombinerAggregator<WeightedPreferences> {

	private static final long serialVersionUID = -3333666254066006365L;

	@SuppressWarnings("unchecked")
	@Override
	public WeightedPreferences init(TridentTuple tuple) {
		Set<Long> user1Preferences = (Set<Long>) tuple.get(0);
		Set<Long> user2Preferences = (Set<Long>) tuple.get(1);
		double similarity = tuple.getDouble(2);

		user2Preferences.removeAll(user1Preferences);
		WeightedPreferences weightedPreferences = new WeightedPreferences();
		weightedPreferences.addAll(user2Preferences, similarity);

		return weightedPreferences;
	}

	@Override
	public WeightedPreferences combine(WeightedPreferences val1, WeightedPreferences val2) {
		return WeightedPreferences.combine(val1, val2);
	}

	@Override
	public WeightedPreferences zero() {
		return new WeightedPreferences();
	}

}

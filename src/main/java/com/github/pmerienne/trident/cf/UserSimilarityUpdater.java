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

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import com.github.pmerienne.trident.cf.core.TanimotoCoefficientSimilarity;
import com.github.pmerienne.trident.cf.core.UserSimilarity;
import com.github.pmerienne.trident.cf.state.CFState;

public class UserSimilarityUpdater extends BaseStateUpdater<CFState> {

	private static final long serialVersionUID = -5176326639509447054L;

	private UserSimilarity userSimilarity = new TanimotoCoefficientSimilarity();

	public UserSimilarityUpdater() {
	}

	public UserSimilarityUpdater(UserSimilarity userSimilarity) {
		this.userSimilarity = userSimilarity;
	}

	@Override
	public void updateState(CFState state, List<TridentTuple> tuples, TridentCollector collector) {

		long user1, user2, user1PreferenceCount, user2PreferenceCount, coPreferenceCount;
		double similarity;
		for (TridentTuple tuple : tuples) {
			user1 = tuple.getLong(0);
			user2 = tuple.getLong(1);
			user1PreferenceCount = tuple.getLong(2);
			user2PreferenceCount = tuple.getLong(3);
			coPreferenceCount = tuple.getLong(4);

			similarity = this.userSimilarity.userSimilarity(user1PreferenceCount, user2PreferenceCount, coPreferenceCount);
			if (!Double.isNaN(similarity)) {
				state.setSimilarity(user1, user2, similarity);
			}
		}
	}
}

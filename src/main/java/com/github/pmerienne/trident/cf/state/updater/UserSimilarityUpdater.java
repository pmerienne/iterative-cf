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
package com.github.pmerienne.trident.cf.state.updater;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import com.github.pmerienne.trident.cf.state.SortedSetMultiMapState;
import com.github.pmerienne.trident.cf.state.SortedSetMultiMapState.ScoredValue;

public class UserSimilarityUpdater extends BaseStateUpdater<SortedSetMultiMapState<Long, Long>> {

	private static final long serialVersionUID = -5176326639509447054L;
	private static final Logger LOGGER = LoggerFactory.getLogger(UserSimilarityUpdater.class);

	@Override
	public void updateState(SortedSetMultiMapState<Long, Long> state, List<TridentTuple> tuples, TridentCollector collector) {

		double similarity;
		long user1, user2;
		for (TridentTuple tuple : tuples) {
			user1 = tuple.getLong(0);
			user2 = tuple.getLong(1);
			similarity = tuple.getDouble(2);

			state.put(user1, new ScoredValue<Long>(similarity, user2));
			state.put(user2, new ScoredValue<Long>(similarity, user2));
			LOGGER.debug("Update similarity between " + user1 + " and " + user2 + " to " + similarity);
		}
	}
}

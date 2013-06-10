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
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.model.UserPair;
import com.github.pmerienne.trident.cf.state.CFState;

public class CoPreferenceCountUpdater extends BaseStateUpdater<CFState> {

	private static final long serialVersionUID = -4573528570416406157L;

	@Override
	public void updateState(CFState state, List<TridentTuple> tuples, TridentCollector collector) {
		long user1, user2, item, coPreferenceCount;
		UserPair userPair;
		for (TridentTuple tuple : tuples) {
			userPair = (UserPair) tuple.get(0);
			user1 = userPair.getUser1();
			user2 = userPair.getUser2();
			item = tuple.getLong(1);

			coPreferenceCount = state.getNumItemsPreferedBy(user1, user2);
			if (state.hasUserPreferenceFor(user2, item)) {
				coPreferenceCount++;
				state.setNumItemsPreferedBy(user1, user2, coPreferenceCount);
			}

			// used for the new values stream
			collector.emit(new Values(user1, user2, coPreferenceCount, item, tuple.get(2), tuple.get(3)));
		}

	}
}

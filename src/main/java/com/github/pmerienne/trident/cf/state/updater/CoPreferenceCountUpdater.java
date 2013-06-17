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
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.model.UserPair;
import com.github.pmerienne.trident.cf.state.MapMultimapState;

public class CoPreferenceCountUpdater extends BaseStateUpdater<MapMultimapState<Long, Long, Long>> {

	private static final long serialVersionUID = -4573528570416406157L;
	private static final Logger LOGGER = LoggerFactory.getLogger(CoPreferenceCountUpdater.class);

	@Override
	public void updateState(MapMultimapState<Long, Long, Long> state, List<TridentTuple> tuples, TridentCollector collector) {

		UserPair userPair;
		Long count;
		long user1, user2;
		for (TridentTuple tuple : tuples) {
			userPair = (UserPair) tuple.get(0);

			user1 = userPair.getUser1();
			user2 = userPair.getUser2();
			count = state.get(user1, user2);
			if (count == null) {
				count = 1L;
			} else {
				count += 1L;
			}

			state.put(user1, user2, count);
			state.put(user2, user1, count);

			// For new values stream
			collector.emit(new Values(userPair, count));

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Increment co preference count between " + user1 + " and " + user2 + " to " + count);
			}
		}
	}

}

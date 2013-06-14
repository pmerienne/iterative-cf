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
package com.github.pmerienne.trident.cf.state.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.state.MapMultimapState;

public class UsersWithCoPreferenceCountQuery extends BaseQueryFunction<MapMultimapState<Long, Long, Long>, Map<Long, Long>> {

	private static final long serialVersionUID = 3700955398704099456L;

	@Override
	public List<Map<Long, Long>> batchRetrieve(MapMultimapState<Long, Long, Long> state, List<TridentTuple> tuples) {
		List<Map<Long, Long>> results = new ArrayList<Map<Long, Long>>();

		long user;
		Map<Long, Long> usersWithCoPreferenceCount;
		for (TridentTuple tuple : tuples) {
			user = tuple.getLong(0);
			usersWithCoPreferenceCount = state.getAll(user);
			results.add(usersWithCoPreferenceCount);
		}

		return results;
	}

	@Override
	public void execute(TridentTuple tuple, Map<Long, Long> usersWithCoPreferenceCount, TridentCollector collector) {
		long coPreferenceCount;
		for (long user : usersWithCoPreferenceCount.keySet()) {
			coPreferenceCount = usersWithCoPreferenceCount.get(user);
			collector.emit(new Values(user, coPreferenceCount));
		}

	}

}

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.state.CFState;

public class UserRatingsQuery extends BaseQueryFunction<CFState, Map<Long, Double>> {

	private static final long serialVersionUID = -3640119024471641688L;

	@Override
	public List<Map<Long, Double>> batchRetrieve(CFState state, List<TridentTuple> args) {
		List<Map<Long, Double>> ratings = new ArrayList<Map<Long, Double>>(args.size());

		long user;
		for (TridentTuple tuple : args) {
			user = tuple.getLong(0);
			ratings.add(state.getRatings(user));
		}

		return ratings;
	}

	@Override
	public void execute(TridentTuple tuple, Map<Long, Double> result, TridentCollector collector) {
		collector.emit(new Values(result));
	}

}

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

import com.github.pmerienne.trident.cf.state.CFState;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class UserSimilarityQuery extends BaseQueryFunction<CFState, Double> {

	private static final long serialVersionUID = -3959281861317353583L;

	@Override
	public List<Double> batchRetrieve(CFState state, List<TridentTuple> args) {
		List<Double> similarities = new ArrayList<Double>(args.size());

		for (TridentTuple tuple : args) {
			long user1 = tuple.getLong(0);
			long user2 = tuple.getLong(1);
			similarities.add(state.getA(user1, user2));
		}

		return similarities;
	}

	@Override
	public void execute(TridentTuple tuple, Double result, TridentCollector collector) {
		collector.emit(new Values(result));
	}

}

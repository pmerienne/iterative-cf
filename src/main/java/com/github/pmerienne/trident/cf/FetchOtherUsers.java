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
import java.util.Set;

import com.github.pmerienne.trident.cf.state.CFState;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class FetchOtherUsers extends BaseQueryFunction<CFState, Set<Long>> {

	private static final long serialVersionUID = -8188458622514047837L;

	@Override
	public List<Set<Long>> batchRetrieve(CFState state, List<TridentTuple> args) {
		Set<Long> users = state.getUsers();

		List<Set<Long>> results = new ArrayList<Set<Long>>(args.size());
		for (int i = 0; i < args.size(); i++) {
			results.add(users);
		}

		return results;
	}

	@Override
	public void execute(TridentTuple tuple, Set<Long> result, TridentCollector collector) {
		long user1 = tuple.getLong(0);

		for (Long user : result) {
			if (user != user1) {
				collector.emit(new Values(user));
			}
		}
	}

}

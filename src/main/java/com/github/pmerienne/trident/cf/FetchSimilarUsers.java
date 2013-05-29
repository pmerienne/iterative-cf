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

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.model.SimilarUser;
import com.github.pmerienne.trident.cf.state.CFState;

public class FetchSimilarUsers extends BaseQueryFunction<CFState, Set<SimilarUser>> {

	private static final long serialVersionUID = -8188458622514047837L;

	public final static int DEFAULT_COUNT = 10;
	private int count = DEFAULT_COUNT;

	public FetchSimilarUsers() {
	}

	public FetchSimilarUsers(int count) {
		this.count = count;
	}

	@Override
	public List<Set<SimilarUser>> batchRetrieve(CFState state, List<TridentTuple> args) {
		List<Set<SimilarUser>> results = new ArrayList<Set<SimilarUser>>(args.size());

		long user1;
		for (TridentTuple tuple : args) {
			user1 = tuple.getLong(0);
			results.add(state.getMostSimilarUsers(user1, this.count));
		}

		return results;
	}

	@Override
	public void execute(TridentTuple tuple, Set<SimilarUser> result, TridentCollector collector) {
		long user1 = tuple.getLong(0);
		for (SimilarUser similarUser : result) {
			if (similarUser.getUser() != user1) {
				collector.emit(new Values(similarUser.getUser(), similarUser.getSimilarity()));
			}
		}
	}

}

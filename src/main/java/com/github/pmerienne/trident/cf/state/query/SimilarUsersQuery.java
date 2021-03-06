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

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.model.SimilarUser;
import com.github.pmerienne.trident.cf.state.SortedSetMultiMapState;
import com.github.pmerienne.trident.cf.state.SortedSetMultiMapState.ScoredValue;

public class SimilarUsersQuery extends BaseQueryFunction<SortedSetMultiMapState<Long, Long>, List<SimilarUser>> {

	private static final long serialVersionUID = -8188458622514047837L;

	public final static int DEFAULT_COUNT = 10;
	private int count = DEFAULT_COUNT;

	public SimilarUsersQuery() {
	}

	public SimilarUsersQuery(int count) {
		this.count = count;
	}

	@Override
	public List<List<SimilarUser>> batchRetrieve(SortedSetMultiMapState<Long, Long> state, List<TridentTuple> args) {
		List<List<SimilarUser>> results = new ArrayList<List<SimilarUser>>(args.size());

		long user1;
		List<ScoredValue<Long>> scoredValues;
		for (TridentTuple tuple : args) {
			user1 = tuple.getLong(0);
			scoredValues = state.getSorted(user1, this.count);
			results.add(toSimilarUsers(scoredValues));
		}

		return results;
	}

	@Override
	public void execute(TridentTuple tuple, List<SimilarUser> similarUsers, TridentCollector collector) {
		long user1 = tuple.getLong(0);
		for (SimilarUser similarUser : similarUsers) {
			if (similarUser.getUser() != user1) {
				collector.emit(new Values(similarUser.getUser(), similarUser.getSimilarity()));
			}
		}
	}

	protected List<SimilarUser> toSimilarUsers(List<ScoredValue<Long>> scoredValues) {
		List<SimilarUser> similarUsers = new ArrayList<SimilarUser>(scoredValues.size());
		for (ScoredValue<Long> scoredValue : scoredValues) {
			similarUsers.add(new SimilarUser(scoredValue.getValue(), scoredValue.getScore()));
		}
		return similarUsers;
	}
}

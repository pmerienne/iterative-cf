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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.model.UserPair;
import com.github.pmerienne.trident.cf.state.SetState;

public class AllUserPairQuery extends BaseQueryFunction<SetState<Long>, Set<UserPair>> {

	private static final long serialVersionUID = -7677538937604670988L;
	private static final Logger LOGGER = LoggerFactory.getLogger(AllUserPairQuery.class);

	@Override
	public List<Set<UserPair>> batchRetrieve(SetState<Long> state, List<TridentTuple> args) {
		Set<Long> allUsers = state.get();
		int nbUsers = allUsers.size();
		int userPairsSize = nbUsers * nbUsers / 2;

		List<Set<UserPair>> results = new ArrayList<Set<UserPair>>(args.size());
		Set<UserPair> userPairs = new HashSet<UserPair>(userPairsSize);
		LOGGER.info(nbUsers + " users found. " + userPairsSize + " similarities will be updated");

		long[] usersArray = new long[nbUsers];
		int k = 0;
		for (long user : allUsers) {
			usersArray[k] = user;
			k++;
		}

		for (int i = 0; i < nbUsers; i++) {
			for (int j = i + 1; j < nbUsers; j++) {
				userPairs.add(new UserPair(usersArray[i], usersArray[j]));
			}
		}

		for (int i = 0; i < args.size(); i++) {
			results.add(userPairs);
		}

		return results;
	}

	@Override
	public void execute(TridentTuple tuple, Set<UserPair> result, TridentCollector collector) {
		for (UserPair userPair : result) {
			collector.emit(new Values(userPair.getUser1(), userPair.getUser2()));
		}
	}

}

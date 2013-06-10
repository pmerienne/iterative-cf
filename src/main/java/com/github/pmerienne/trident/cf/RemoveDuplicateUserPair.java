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

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

import com.github.pmerienne.trident.cf.model.UserPair;

public class RemoveDuplicateUserPair implements CombinerAggregator<UserPair> {

	private static final long serialVersionUID = -2825795244954595081L;

	@Override
	public UserPair init(TridentTuple tuple) {
		UserPair userPair = (UserPair) tuple.get(0);
		return userPair;
	}

	@Override
	public UserPair combine(UserPair val1, UserPair val2) {
		return val1;
	}

	@Override
	public UserPair zero() {
		return null;
	}

}

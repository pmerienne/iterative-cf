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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.state.SetState;

public class GetAndClearUpdatedUsers extends BaseStateUpdater<SetState<Long>> {

	private static final long serialVersionUID = 1550810054247286639L;

	private static final Logger LOGGER = LoggerFactory.getLogger(GetAndClearUpdatedUsers.class);

	@Override
	public void updateState(SetState<Long> state, List<TridentTuple> tuples, TridentCollector collector) {
		Set<Long> users = state.get();
		state.clear();
		LOGGER.info("Found " + users.size() + " updated users");

		// For new value stream
		for (Long user : users) {
			collector.emit(new Values(user));
		}
	}

}

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

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.state.SetMultiMapState;

public class PreferredItemUpdater extends BaseStateUpdater<SetMultiMapState<Long, Long>> {

	private static final long serialVersionUID = 8199649920663679656L;

	@Override
	public void updateState(SetMultiMapState<Long, Long> state, List<TridentTuple> tuples, TridentCollector collector) {

		long user, item;
		for (TridentTuple tuple : tuples) {
			user = tuple.getLong(0);
			item = tuple.getLong(1);
			
			boolean alreadyExist = !state.put(item, user);

			// For new values stream
			if (!alreadyExist) {
				collector.emit(new Values(user, item));
			}
		}

	}

}

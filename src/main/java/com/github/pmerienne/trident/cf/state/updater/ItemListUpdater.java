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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.state.SetState;

public class ItemListUpdater extends BaseStateUpdater<SetState<Long>> {

	private static final long serialVersionUID = -1796957052133633660L;

	@Override
	public void updateState(SetState<Long> state, List<TridentTuple> tuples, TridentCollector collector) {
		Set<Long> items = new HashSet<Long>();

		long user, item;
		for (TridentTuple tuple : tuples) {
			user = tuple.getLong(0);
			item = tuple.getLong(1);
			items.add(item);

			// For new values stream
			collector.emit(new Values(user, item));
		}

		state.addAll(items);
	}

}

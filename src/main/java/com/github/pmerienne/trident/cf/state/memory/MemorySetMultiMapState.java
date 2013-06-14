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
package com.github.pmerienne.trident.cf.state.memory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.cf.state.SetMultiMapState;
import com.github.pmerienne.trident.cf.util.MapStateUtil;

public class MemorySetMultiMapState<K, V> extends TransactionalMemoryMapState<Set<V>> implements SetMultiMapState<K, V> {

	public MemorySetMultiMapState(String id) {
		super(id);
	}

	@Override
	public long sizeOf(K key) {
		Set<V> set = this.get(key);
		return set == null ? 0 : set.size();
	}

	@Override
	public Set<V> get(K key) {
		Set<V> value = 	MapStateUtil.getSingle(this, key);
		if(value == null) {
			value = new HashSet<V>();
		}
		return value;
	}

	@Override
	public boolean put(K key, V value) {
		Set<V> set = this.get(key);
		if (set == null) {
			set = new HashSet<V>();
		}

		boolean result = set.add(value);
		MapStateUtil.putSingle(this, key, set);

		return result;
	}

	@SuppressWarnings({ "rawtypes" })
	public static class Factory implements StateFactory {

		private static final long serialVersionUID = -6865870100536320916L;

		private final String id;

		public Factory() {
			this.id = UUID.randomUUID().toString();
		}

		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new MemorySetMultiMapState(this.id);
		}
	}
}

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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import storm.trident.state.State;
import storm.trident.state.StateFactory;

import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.cf.state.MapMultimapState;
import com.github.pmerienne.trident.cf.util.MapStateUtil;

public class MemoryMapMultimapState<K1, K2, V> extends TransactionalMemoryMapState<Map<K2, V>> implements MapMultimapState<K1, K2, V> {

	public MemoryMapMultimapState(String id) {
		super(id);
	}

	@Override
	public boolean put(K1 key, K2 subkey, V value) {
		Map<K2, V> all = this.getAll(key);
		V previous = all.put(subkey, value);
		MapStateUtil.putSingle(this, key, all);
		return previous == null;
	}

	@Override
	public V get(K1 key, K2 subkey) {
		Map<K2, V> all = this.getAll(key);
		return all.get(subkey);
	}

	@Override
	public Map<K2, V> getAll(K1 key) {
		Map<K2, V> map = MapStateUtil.getSingle(this, key);
		return map == null ? new HashMap<K2, V>() : map;
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
			return new MemoryMapMultimapState(this.id);
		}
	}

}

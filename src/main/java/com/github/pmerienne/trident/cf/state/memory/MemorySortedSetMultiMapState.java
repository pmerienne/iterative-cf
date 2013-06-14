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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.cf.state.SortedSetMultiMapState;
import com.github.pmerienne.trident.cf.state.SortedSetMultiMapState.ScoredValue;
import com.github.pmerienne.trident.cf.util.MapStateUtil;

public class MemorySortedSetMultiMapState<K, T> extends TransactionalMemoryMapState<TreeSet<ScoredValue<T>>> implements SortedSetMultiMapState<K, T> {

	public MemorySortedSetMultiMapState(String id) {
		super(id);
	}

	@Override
	public long sizeOf(K key) {
		TreeSet<ScoredValue<T>> set = this.get(key);
		return set == null ? 0 : set.size();
	}

	@Override
	public boolean put(K key, ScoredValue<T> scoredValue) {
		TreeSet<ScoredValue<T>> set = this.get(key);

		boolean result = set.add(scoredValue);
		MapStateUtil.putSingle(this, key, set);

		return result;
	}

	@Override
	public List<ScoredValue<T>> getSorted(K key, int count) {
		TreeSet<ScoredValue<T>> set = this.get(key);
		List<ScoredValue<T>> results = new ArrayList<ScoredValue<T>>(count);

		int i = 0;
		Iterator<ScoredValue<T>> it = set.descendingIterator();
		while (it.hasNext() && i < count) {
			results.add(it.next());
		}

		return results;
	}

	@Override
	public double getScore(K key, T value) {
		TreeSet<ScoredValue<T>> set = this.get(key);
		if (set == null) {
			return 0.0;
		}

		Double score = null;

		ScoredValue<T> current;
		Iterator<ScoredValue<T>> it = set.iterator();
		while (it.hasNext() && score == null) {
			current = it.next();
			if (current.getValue().equals(value)) {
				score = current.getScore();
			}
		}

		return score == null ? 0.0 : score;
	}

	protected TreeSet<ScoredValue<T>> get(K key) {
		TreeSet<ScoredValue<T>> value = MapStateUtil.getSingle(this, key);
		if (value == null) {
			value = new TreeSet<ScoredValue<T>>();
		}
		return value;
	}

	@SuppressWarnings({ "rawtypes" })
	public static class Factory implements StateFactory {

		private static final long serialVersionUID = -6865870100536320916L;

		private final String _id;

		public Factory() {
			this._id = UUID.randomUUID().toString();
		}

		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new MemorySortedSetMultiMapState(this._id);
		}
	}

}

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
package com.github.pmerienne.trident.cf.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import storm.trident.state.ITupleCollection;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.TransactionalValue;
import storm.trident.state.ValueUpdater;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;
import storm.trident.state.snapshot.Snapshottable;
import storm.trident.testing.MemoryMapState;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

public class MemoryCFState extends DelegateCFState {

	@Override
	protected <T> MapState<T> createMapState(String id) {
		return new TransactionalMemoryMapState<T>(id);
	}

	public static class Factory implements StateFactory {

		private static final long serialVersionUID = 4718043951532492603L;

		@SuppressWarnings("rawtypes")
		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new MemoryCFState();
		}
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static class TransactionalMemoryMapState<T> implements Snapshottable<T>, ITupleCollection, MapState<T> {

		MemoryMapStateBacking<TransactionalValue> _backing;
		SnapshottableMap<T> _delegate;

		public TransactionalMemoryMapState(String id) {
			_backing = new MemoryMapStateBacking(id);
			_delegate = new SnapshottableMap(TransactionalMap.build(_backing), new Values("$MEMORY-MAP-STATE-GLOBAL$"));
		}

		public T update(ValueUpdater updater) {
			return _delegate.update(updater);
		}

		public void set(T o) {
			_delegate.set(o);
		}

		public T get() {
			return _delegate.get();
		}

		public void beginCommit(Long txid) {
			_delegate.beginCommit(txid);
		}

		public void commit(Long txid) {
			_delegate.commit(txid);
		}

		public Iterator<List<Object>> getTuples() {
			return _backing.getTuples();
		}

		public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
			return _delegate.multiUpdate(keys, updaters);
		}

		public void multiPut(List<List<Object>> keys, List<T> vals) {
			_delegate.multiPut(keys, vals);
		}

		public List<T> multiGet(List<List<Object>> keys) {
			return _delegate.multiGet(keys);
		}

		@SuppressWarnings("serial")
		public static class Factory implements StateFactory {

			String _id;

			public Factory() {
				_id = UUID.randomUUID().toString();
			}

			@Override
			public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
				return new MemoryMapState(_id);
			}
		}

		static ConcurrentHashMap<String, Map<List<Object>, Object>> _dbs = new ConcurrentHashMap<String, Map<List<Object>, Object>>();

		static class MemoryMapStateBacking<T> implements IBackingMap<T>, ITupleCollection {

			public static void clearAll() {
				_dbs.clear();
			}

			Map<List<Object>, T> db;
			Long currTx;

			public MemoryMapStateBacking(String id) {
				if (!_dbs.containsKey(id)) {
					_dbs.put(id, new HashMap());
				}
				this.db = (Map<List<Object>, T>) _dbs.get(id);
			}

			@Override
			public List<T> multiGet(List<List<Object>> keys) {
				List<T> ret = new ArrayList();
				for (List<Object> key : keys) {
					ret.add(db.get(key));
				}
				return ret;
			}

			@Override
			public void multiPut(List<List<Object>> keys, List<T> vals) {
				for (int i = 0; i < keys.size(); i++) {
					List<Object> key = keys.get(i);
					T val = vals.get(i);
					db.put(key, val);
				}
			}

			@Override
			public Iterator<List<Object>> getTuples() {
				return new Iterator<List<Object>>() {

					private Iterator<Map.Entry<List<Object>, T>> it = db.entrySet().iterator();

					public boolean hasNext() {
						return it.hasNext();
					}

					public List<Object> next() {
						Map.Entry<List<Object>, T> e = it.next();
						List<Object> ret = new ArrayList<Object>();
						ret.addAll(e.getKey());
						ret.add(((TransactionalValue) e.getValue()).getVal());
						return ret;
					}

					public void remove() {
						throw new UnsupportedOperationException("Not supported yet.");
					}
				};
			}
		}
	}
}

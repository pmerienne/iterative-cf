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

import java.util.List;

import storm.trident.state.State;

public interface SortedSetMultiMapState<K, T> extends State {

	long sizeOf(K key);

	boolean put(K key, ScoredValue<T> value);

	List<ScoredValue<T>> getSorted(K key, int count);

	double getScore(K key, T value);

	@SuppressWarnings("rawtypes")
	public static class ScoredValue<T> implements Comparable<ScoredValue<T>> {

		private final double score;
		private final T value;

		public ScoredValue(double score, T value) {
			this.score = score;
			this.value = value;
		}

		public double getScore() {
			return score;
		}

		public T getValue() {
			return value;
		}

		@Override
		public int compareTo(ScoredValue<T> o) {
			if (o == null) {
				return 1;
			} else {
				if (this.score != o.score) {
					return Double.compare(this.score, o.score);
				} else {
					return compare(o.getValue(), o.getValue());
				}
			}
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((value == null) ? 0 : value.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ScoredValue other = (ScoredValue) obj;
			if (value == null) {
				if (other.value != null)
					return false;
			} else if (!value.equals(other.value))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "ScoredValue [score=" + score + ", value=" + value + "]";
		}

		@SuppressWarnings("unchecked")
		public static <T> int compare(T k1, T k2) {
			if (k1 instanceof Comparable<?>) {
				return ((Comparable<? super T>) k1).compareTo((T) k2);
			} else {
				return 0;
			}
		}
	}

}

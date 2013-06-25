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

/**
 * A {@link State} which wrap access to a Multimap that cannot hold duplicate
 * key-value pairs. Adding a key-value pair that's already in the multimap has
 * no effect. This class is very similar to the {@link SetMultiMapState}. The
 * difference is that every member is associated with score, that is used in
 * order to take the sorted set ordered, from the smallest to the greatest
 * score. While members are unique, scores may be repeated.
 * 
 * @author pmerienne
 * 
 * @param <K>
 *            the type of keys
 * @param <V>
 *            the type of values
 */
public interface SortedSetMultiMapState<K, V> extends State {

	/**
	 * Returns the numbers of values which are mapped to a given key
	 * 
	 * @param key
	 * @return
	 */
	long sizeOf(K key);

	/**
	 * Adds the specified value with the specified score to the sorted set
	 * stored at key. Stores a key-value pair in the multimap. If the specified
	 * value already exists in the sorted set, the score is updated and the
	 * element reinserted at the right position to ensure the correct ordering.
	 * If key does not exist, a new sorted set with the specified value as sole
	 * members is created, like if the sorted set was empty.
	 * 
	 * @param key
	 *            key to store
	 * @param value
	 *            scored value to store
	 * @return <code>true</code> if this set changed as a result of the call
	 */
	boolean put(K key, ScoredValue<V> value);

	/**
	 * Returns the specified range of elements in the sorted set stored at key.
	 * The elements are considered to be ordered from the highest to the lowest
	 * score.
	 * 
	 * @param key
	 *            the key whose associated set is used
	 * @param count
	 *            the maximum number of scored values returned
	 * @return
	 */
	List<ScoredValue<V>> getSorted(K key, int count);

	/**
	 * Returns the score of value in the sorted set at key. If value does not
	 * exist in the sorted set, or key does not exist, 0.0 is returned.
	 * 
	 * @param key
	 *            the key whose associated set is used
	 * @param value
	 *            the value whose score is searched
	 * @return
	 */
	double getScore(K key, V value);

	@SuppressWarnings("rawtypes")
	public static class ScoredValue<V> implements Comparable<ScoredValue<V>> {

		private final double score;
		private final V value;

		public ScoredValue(double score, V value) {
			this.score = score;
			this.value = value;
		}

		public double getScore() {
			return score;
		}

		public V getValue() {
			return value;
		}

		@Override
		public int compareTo(ScoredValue<V> o) {
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
		public static <V> int compare(V k1, V k2) {
			if (k1 instanceof Comparable<?>) {
				return ((Comparable<? super V>) k1).compareTo((V) k2);
			} else {
				return 0;
			}
		}
	}

}

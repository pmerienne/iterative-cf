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

import java.util.Set;

import storm.trident.state.State;

/**
 * A {@link State} which wrap access to a Multimap that cannot hold duplicate
 * key-value pairs. Adding a key-value pair that's already in the multimap has
 * no effect.
 * 
 * @author pmerienne
 * 
 * @param <K>
 *            the type of keys
 * @param <V>
 *            the type of values
 */
public interface SetMultiMapState<K, V> extends State {

	/**
	 * Returns the numbers of values which are mapped to a given key
	 * 
	 * @param key
	 * @return
	 */
	long sizeOf(K key);

	/**
	 * Stores a key-value pair in the multimap. Storing a key-value pair that's
	 * already in the multimap has no effect.
	 * 
	 * @param key
	 *            key to store
	 * @param value
	 *            value to store
	 * @return <code>true</code> if this changed the state
	 */
	boolean put(K key, V value);

	/**
	 * Returns a {@link Set} view of all values associated with a key. If
	 * there's no mappings, an empty {@link Set} is returned.
	 * 
	 * @param key
	 * @return the set of values that the key maps to
	 */
	Set<V> get(K key);
}

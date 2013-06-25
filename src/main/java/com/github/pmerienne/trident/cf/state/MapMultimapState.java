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

import java.util.Map;

import storm.trident.state.State;

/**
 * A {@link State} which wrap access to a Map of nested map.
 * 
 * @author pmerienne
 * 
 * @param <K1>
 *            the type of keys in the upper map
 * @param <K2>
 *            the type of keys in the nested maps
 * @param <V>
 *            the type of values
 */
public interface MapMultimapState<K1, K2, V> extends State {

	/**
	 * Add a given value at key, subkey.
	 * 
	 * @param key
	 * @param subkey
	 * @param value
	 * @return
	 */
	boolean put(K1 key, K2 subkey, V value);

	/**
	 * Get the value stored at key, subkey.
	 * 
	 * @param key
	 * @param subkey
	 * @return the stored value or <code>null</code> if there's no mappings
	 */
	V get(K1 key, K2 subkey);

	/**
	 * Returns the nested map at key. If there's no mappings, an empty
	 * {@link Map} is returned.
	 * 
	 * @param key
	 * @return
	 */
	Map<K2, V> getAll(K1 key);
}

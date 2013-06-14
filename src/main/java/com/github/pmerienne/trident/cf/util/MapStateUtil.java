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
package com.github.pmerienne.trident.cf.util;

import java.util.Arrays;
import java.util.List;

import storm.trident.state.map.MapState;

public class MapStateUtil {

	@SuppressWarnings("unchecked")
	public static <T> void putSingle(MapState<T> state, Object singleKey, T value) {
		List<List<Object>> keys = toKeys(singleKey);
		List<T> values = Arrays.asList(value);
		state.multiPut(keys, values);
	}

	@SuppressWarnings("unchecked")
	public static <T> void putSingle(MapState<T> state, Object key1, Object key2, T value) {
		List<List<Object>> keys = Arrays.asList(Arrays.asList(key1, key2));
		List<T> values = Arrays.asList(value);
		state.multiPut(keys, values);
	}

	public static <T> T getSingle(MapState<T> state, Object singleKey) {
		List<List<Object>> keys = toKeys(singleKey);
		List<T> values = state.multiGet(keys);
		T value = singleValue(values);
		return value;
	}

	@SuppressWarnings("unchecked")
	public static <T> T getSingle(MapState<T> state, Object key1, Object key2) {
		List<List<Object>> keys = Arrays.asList(Arrays.asList(key1, key2));
		List<T> values = state.multiGet(keys);
		T value = singleValue(values);
		return value;
	}

	public static <T> T singleValue(List<T> values) {
		return values != null && !values.isEmpty() ? values.get(0) : null;
	}

	@SuppressWarnings("unchecked")
	public static List<List<Object>> toKeys(Object singleKey) {
		List<List<Object>> keys = Arrays.asList(Arrays.asList(singleKey));
		return keys;
	}
}

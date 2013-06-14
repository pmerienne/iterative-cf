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
package com.github.pmerienne.trident.cf.aggregator;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * {@link CombinerAggregator} which re-send the first tuple received
 * 
 * @author pmerienne
 * 
 */
@SuppressWarnings("unchecked")
public class KeepFirst<T> implements CombinerAggregator<T> {

	private static final long serialVersionUID = 1675152830890440572L;

	@Override
	public T init(TridentTuple tuple) {
		T value = (T) tuple.get(0);
		return value;
	}

	@Override
	public T combine(T val1, T val2) {
		return val1;
	}

	@Override
	public T zero() {
		return null;
	}

}
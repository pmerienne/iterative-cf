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
package com.github.pmerienne.trident.cf;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * {@link CombinerAggregator} which re-send the first tuple received
 * 
 * @author pmerienne
 * 
 */
public class KeepFirst implements CombinerAggregator<Values> {

	private static final long serialVersionUID = 1675152830890440572L;

	@Override
	public Values init(TridentTuple tuple) {
		Values values = new Values();
		for (Object obj : tuple) {
			values.add(obj);
		}
		return values;
	}

	@Override
	public Values combine(Values val1, Values val2) {
		return val1;
	}

	@Override
	public Values zero() {
		return null;
	}

}
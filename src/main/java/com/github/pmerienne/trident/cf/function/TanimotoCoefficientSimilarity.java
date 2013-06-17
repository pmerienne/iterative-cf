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
package com.github.pmerienne.trident.cf.function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class TanimotoCoefficientSimilarity extends BaseFunction {

	private static final long serialVersionUID = 1014073072262444995L;
	private static final Logger LOGGER = LoggerFactory.getLogger(TanimotoCoefficientSimilarity.class);

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		long user1PreferenceCount = tuple.getLong(0);
		long user2PreferenceCount = tuple.getLong(1);
		long coPreferenceCount = tuple.getLong(02);

		double userSimilarity = this.userSimilarity(user1PreferenceCount, user2PreferenceCount, coPreferenceCount);
		collector.emit(new Values(userSimilarity));

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Tanimoto similarity : " + userSimilarity + " (user1 : " + user1PreferenceCount + ", user2 : " + user2PreferenceCount + ", co : " + coPreferenceCount + ")");
		}
	}

	protected double userSimilarity(long user1PreferenceCount, long user2PreferenceCount, long coPreferenceCount) {
		long unionSize = user1PreferenceCount + user2PreferenceCount - coPreferenceCount;
		double similarity = (double) coPreferenceCount / (double) unionSize;
		return similarity;
	}

}

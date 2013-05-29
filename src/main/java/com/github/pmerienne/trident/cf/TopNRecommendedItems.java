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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.model.RecommendedItem;
import com.github.pmerienne.trident.cf.model.WeightedRatings;

public class TopNRecommendedItems extends BaseFunction {

	private static final long serialVersionUID = 2330321317497348772L;

	private int nbItems;

	public TopNRecommendedItems(int nbItems) {
		this.nbItems = nbItems;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		WeightedRatings weightedRatings = (WeightedRatings) tuple.get(0);
		List<RecommendedItem> itemRecommendations = this.getItemRecommendations(weightedRatings);
		collector.emit(new Values(itemRecommendations));
	}

	protected List<RecommendedItem> getItemRecommendations(WeightedRatings weightedRatings) {
		Queue<RecommendedItem> topItems = new PriorityQueue<RecommendedItem>(this.nbItems + 1, Collections.reverseOrder());
		boolean full = false;
		double lowestTopValue = Double.NEGATIVE_INFINITY;
		double recommendation;
		for (Long item : weightedRatings.getItems()) {
			recommendation = weightedRatings.getRecommendation(item);
			if (!Double.isNaN(recommendation) && (!full || recommendation > lowestTopValue)) {
				topItems.add(new RecommendedItem(item, recommendation));
				if (full) {
					topItems.poll();
				} else if (topItems.size() > this.nbItems) {
					full = true;
					topItems.poll();
				}
				lowestTopValue = topItems.peek().getRecommendation();
			}
		}

		int size = topItems.size();
		if (size == 0) {
			return new ArrayList<RecommendedItem>();
		}

		// Sort by recommendation
		List<RecommendedItem> sorted = new ArrayList<RecommendedItem>(size);
		sorted.addAll(topItems);
		Collections.sort(sorted);

		return sorted;
	}
}

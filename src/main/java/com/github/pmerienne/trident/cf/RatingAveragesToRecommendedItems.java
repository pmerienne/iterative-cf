package com.github.pmerienne.trident.cf;

import java.util.HashMap;
import java.util.Map;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.model.RatingAverages;

public class RatingAveragesToRecommendedItems extends BaseFunction {

	private static final long serialVersionUID = 2330321317497348772L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		RatingAverages ratingAverages = (RatingAverages) tuple.get(0);
		Map<Long, Double> itemRecommendations = this.getItemRecommendations(ratingAverages);
		collector.emit(new Values(itemRecommendations));
	}

	protected Map<Long, Double> getItemRecommendations(RatingAverages ratingAverages) {
		Map<Long, Double> itemRecommendations = new HashMap<Long, Double>();

		for (Long item : ratingAverages.getItems()) {
			itemRecommendations.put(item, ratingAverages.getMean(item));
		}

		return itemRecommendations;
	}
}

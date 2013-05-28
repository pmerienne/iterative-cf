package com.github.pmerienne.trident.cf;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

import com.github.pmerienne.trident.cf.model.Average;
import com.github.pmerienne.trident.cf.model.RatingAverages;

public class UnratedItemsCombiner implements CombinerAggregator<RatingAverages> {

	private static final long serialVersionUID = 7721467172409954246L;

	@SuppressWarnings("unchecked")
	@Override
	public RatingAverages init(TridentTuple tuple) {
		Map<Long, Double> user1Ratings = (Map<Long, Double>) tuple.get(0);
		Map<Long, Double> user2Ratings = (Map<Long, Double>) tuple.get(1);

		RatingAverages ratingAverage = new RatingAverages();
		for (Long item : user2Ratings.keySet()) {
			if (!user1Ratings.containsKey(item)) {
				ratingAverage.addRating(item, user2Ratings.get(item));
			}
		}

		return ratingAverage;
	}

	@Override
	public RatingAverages combine(RatingAverages val1, RatingAverages val2) {
		RatingAverages newRatingAverages = new RatingAverages();

		Set<Long> items = new HashSet<Long>();
		items.addAll(val1.getItems());
		items.addAll(val2.getItems());

		Average average1, average2, newAverage;
		for (long item : items) {
			average1 = val1.getAverage(item);
			average2 = val2.getAverage(item);

			if (average1 == null) {
				newAverage = average2;
			} else if (average2 == null) {
				newAverage = average1;
			} else {
				newAverage = new Average(average1.sum + average2.sum, average1.count + average2.count);
			}

			newRatingAverages.averages.put(item, newAverage);
		}

		return newRatingAverages;
	}

	@Override
	public RatingAverages zero() {
		return new RatingAverages();
	}

}

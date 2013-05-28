package com.github.pmerienne.trident.cf.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RatingAverages implements Serializable {

	private static final long serialVersionUID = -8200579542852223837L;

	public final Map<Long, Average> averages = new HashMap<Long, Average>();

	public Set<Long> getItems() {
		return this.averages.keySet();
	}

	public Double getMean(long item) {
		Average average = this.averages.get(item);
		return average == null ? null : average.getMean();
	}

	public Average getAverage(long item) {
		return this.averages.get(item);
	}

	public void addRating(long item, double rating) {
		Average average = this.averages.get(item);
		if (average == null) {
			this.averages.put(item, new Average(rating, 1L));
		} else {
			this.averages.put(item, average.add(rating));
		}
	}

	@Override
	public String toString() {
		return "RatingAverages [averages=" + averages + "]";
	}

}

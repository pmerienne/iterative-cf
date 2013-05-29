package com.github.pmerienne.trident.cf.model;

import java.io.Serializable;

public class RecommendedItem implements Comparable<RecommendedItem>, Serializable {

	private static final long serialVersionUID = -682518328605185319L;

	protected final long item;
	protected final double recommendation;

	public RecommendedItem(long item, double recommendation) {
		this.item = item;
		this.recommendation = recommendation;
	}

	public long getItem() {
		return item;
	}

	public double getRecommendation() {
		return recommendation;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (item ^ (item >>> 32));
		long temp;
		temp = Double.doubleToLongBits(recommendation);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RecommendedItem other = (RecommendedItem) obj;
		if (item != other.item)
			return false;
		if (Double.doubleToLongBits(recommendation) != Double.doubleToLongBits(other.recommendation))
			return false;
		return true;
	}

	@Override
	public int compareTo(RecommendedItem other) {
		double otherRecommendation = other.recommendation;
		if (recommendation > otherRecommendation) {
			return -1;
		}
		if (recommendation < otherRecommendation) {
			return 1;
		}
		long otherItem = other.item;
		if (item < otherItem) {
			return -1;
		}
		if (item > otherItem) {
			return 1;
		}
		return 0;
	}

	@Override
	public String toString() {
		return "RecommendedItem [item=" + item + ", recommendation=" + recommendation + "]";
	}

}
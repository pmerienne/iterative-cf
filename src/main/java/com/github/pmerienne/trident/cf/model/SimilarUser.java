package com.github.pmerienne.trident.cf.model;

import java.io.Serializable;

public class SimilarUser implements Comparable<SimilarUser>, Serializable {

	private static final long serialVersionUID = 1808024231838017915L;

	private final long user;
	private final double similarity;

	public SimilarUser(long user, double similarity) {
		this.user = user;
		this.similarity = similarity;
	}

	public long getUser() {
		return user;
	}

	public double getSimilarity() {
		return similarity;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(similarity);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + (int) (user ^ (user >>> 32));
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
		SimilarUser other = (SimilarUser) obj;
		if (Double.doubleToLongBits(similarity) != Double.doubleToLongBits(other.similarity))
			return false;
		if (user != other.user)
			return false;
		return true;
	}

	/** Defines an ordering from most similar to least similar. */
	@Override
	public int compareTo(SimilarUser other) {
		double otherSimilarity = other.getSimilarity();
		if (similarity > otherSimilarity) {
			return -1;
		}
		if (similarity < otherSimilarity) {
			return 1;
		}
		long otherUser = other.getUser();
		if (user < otherUser) {
			return -1;
		}
		if (user > otherUser) {
			return 1;
		}
		return 0;
	}

	@Override
	public String toString() {
		return "SimilarUser [user=" + user + ", similarity=" + similarity + "]";
	}
}
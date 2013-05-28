package com.github.pmerienne.trident.cf.testing;

import java.util.Map;

import com.github.pmerienne.trident.cf.state.CFState;
import com.github.pmerienne.trident.cf.state.InMemoryCFState;

public class ICF2 {

	private CFState cfState = new InMemoryCFState();

	public void update(long user, long item, double newRating) {
		this.cfState.addUser(user);

		// Update cached m (The number of the items that a user has rated)
		this.updateNumberOfRatedItem(user, item);
		double averageRatingDifference = this.updateAverageRating(user, item, newRating);

		for (Long otherUser : this.cfState.getUsers()) {
			if (otherUser != user) {
				this.updateSimilarity(user, otherUser, item, newRating, averageRatingDifference);
				this.updateCoRated(user, otherUser, item, newRating);
			}
		}

		this.cfState.addRating(user, item, newRating);
	}

	private void updateCoRated(long user1, long user2, long item, double newRating) {
		Double user1OldRating = this.cfState.getRating(user1, item);
		Double user2Rating = this.cfState.getRating(user2, item);

		// Update co-rating
		if (user2Rating != null) {
			if (user1OldRating == null) {
				// Update count
				long oldCount = this.cfState.getCoRatedCount(user1, user2);
				this.cfState.setCoRatedCount(user1, user2, oldCount + 1);

				// Update co rated sums
				Map<Long, Double> sums = this.cfState.getCoRatedSums(user1, user2);
				sums.put(user1, sums.get(user1) + newRating);
				sums.put(user2, sums.get(user2) + user2Rating);
				this.cfState.setCoRatedSums(user1, user2, sums);
			} else {
				// Update co rated sums
				Map<Long, Double> sums = this.cfState.getCoRatedSums(user1, user2);
				sums.put(user1, sums.get(user1) + (newRating - user1OldRating));
				this.cfState.setCoRatedSums(user1, user2, sums);
			}
		}
	}

	private void updateNumberOfRatedItem(long user, long item) {
		Double rating = this.cfState.getRating(user, item);
		if (rating == null) {
			long count = this.cfState.getRatedItemCount(user);
			this.cfState.setRatedItemCount(user, count + 1);
		}
	}

	private double updateAverageRating(long user, long item, double newRating) {
		long m = this.cfState.getRatedItemCount(user);
		double oldAverageRating = this.cfState.getAverageRating(user);
		double newAverageRating;

		Double oldRating = this.cfState.getRating(user, item);
		if (oldRating == null) {
			// Submission of a new rating
			newAverageRating = newRating / (m) + oldAverageRating * (m - 1) / (m);
		} else {
			// Update of existing rating
			newAverageRating = oldAverageRating + (newRating - oldRating) / m;
		}

		this.cfState.setAverageRating(user, newAverageRating);
		return newAverageRating - oldAverageRating;
	}

	private void updateSimilarity(long user1, long user2, long item, double newRating, double averageRatingDifference) {
		Double user1OldRating = this.cfState.getRating(user1, item);
		Double user2Rating = this.cfState.getRating(user2, item);

		// Get average ratings of users
		Double user1AverageRating = this.cfState.getAverageRating(user1);
		Double user1OldAverageRating = user1AverageRating - averageRatingDifference;
		Double user2AverageRating = this.cfState.getAverageRating(user2);

		double e = 0, f = 0, g = 0;

		// TODO
		Long coRatedCount = this.cfState.getCoRatedCount(user1, user2);
		Map<Long, Double> coRatedSums = this.cfState.getCoRatedSums(user1, user2);

		double magic1 = averageRatingDifference * (coRatedSums.get(user1) - coRatedCount * user1OldAverageRating);
		double magic2 = averageRatingDifference * (coRatedSums.get(user2) - coRatedCount * user2AverageRating);
		if (user1OldRating == null) {
			if (user2Rating != null) {
				// Submission of a new rating and uy had rated ia
				e = (newRating - user1AverageRating) * (user2Rating - user2AverageRating) - magic2;
				f = Math.pow(newRating - user1AverageRating, 2) + coRatedCount * Math.pow(averageRatingDifference, 2) - 2 * magic1;
				g = Math.pow(user2Rating - user2AverageRating, 2);
			} else {
				// Submission of a new rating and uy had not rated ia
				e = -magic2;
				f = coRatedCount * Math.pow(averageRatingDifference, 2) - 2 * magic1;
				g = 0.0;
			}
		} else {
			double druaia = newRating - user1OldRating;
			if (user2Rating != null) {
				// Update of an existing rating and uy had rated ia
				e = druaia * (user2Rating - user2AverageRating) - magic2;
				f = Math.pow(druaia, 2) + 2 * druaia * (user1OldRating - user1AverageRating) + coRatedCount * Math.pow(averageRatingDifference, 2) - 2 * magic1;
				g = 0.0;
			} else {
				// Update of an existing rating and uy had not rated ia
				e = -magic2;
				f = coRatedCount * Math.pow(averageRatingDifference, 2) - 2 * magic1;
				g = 0.0;
			}
		}

		// Get cached b, c, d
		Double b = this.cfState.getB(user1, user2);
		Double c = this.cfState.getC(user1, user2);
		Double d = this.cfState.getD(user1, user2);
		b = b + e;
		c = c + f;
		d = d + g;
		this.cfState.setA(user1, user2, b / (Math.sqrt(c) * Math.sqrt(d)));
		this.cfState.setB(user1, user2, b);
		this.cfState.setC(user1, user2, c);
		this.cfState.setD(user1, user2, d);
	}

	public static void main(String[] args) {
		ICF2 icf = new ICF2();
		icf.update(0L, 0L, 0.0);
		icf.update(0L, 1L, 0.5);
		icf.update(0L, 2L, 0.9);
		icf.update(0L, 3L, 0.6);

		icf.update(1L, 0L, 0.1);
		icf.update(1L, 1L, 0.4);
		icf.update(1L, 3L, 0.7);

		icf.update(2L, 0L, 0.8);
		icf.update(2L, 2L, 0.2);
		icf.update(2L, 3L, 0.1);

		System.out.println(icf.cfState.getA(0, 1));
		System.out.println(icf.cfState.getA(1, 2));
		System.out.println(icf.cfState.getA(0, 2));

		System.out.println("#RELA");

		double averageRating0 = (0.0 + 0.5 + 0.9 + 0.6) / 4;
		double averageRating1 = (0.1 + 0.4 + 0.7) / 3;
		double averageRating2 = (0.8 + 0.2 + 0.1) / 3;

		// sim 0, 1
		double sumXY = (0.0 - averageRating0) * (0.1 - averageRating1) + (0.5 - averageRating0) * (0.4 - averageRating1) + (0.6 - averageRating0) * (0.7 - averageRating1);
		double sumX2 = Math.pow(0.0 - averageRating0, 2) + Math.pow(0.5 - averageRating0, 2) + Math.pow(0.6 - averageRating0, 2);
		double sumY2 = Math.pow(0.1 - averageRating1, 2) + Math.pow(0.4 - averageRating1, 2) + Math.pow(0.7 - averageRating1, 2);
		double sim01 = sumXY / (Math.sqrt(sumX2) * Math.sqrt(sumY2));
		System.out.println(sim01);
	}
}

package com.github.pmerienne.trident.cf;

import java.util.List;

import com.github.pmerienne.trident.cf.state.CFState;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class UpdateUserCache extends BaseStateUpdater<CFState> {

	private static final long serialVersionUID = -7860608839049663486L;

	@Override
	public void updateState(CFState state, List<TridentTuple> tuples, TridentCollector collector) {
		for (TridentTuple tuple : tuples) {
			this.updateUserCache(state, tuple, collector);
		}
	}

	protected void updateUserCache(CFState state, TridentTuple tuple, TridentCollector collector) {
		long user = this.getUser(tuple);
		long item = this.getItem(tuple);
		double newRating = this.getRating(tuple);
		
		// Update user list
		state.addUser(user);

		// Update rated item count
		Double oldRating = state.getRating(user, item);
		long ratedItemCount = state.getRatedItemCount(user);
		if (oldRating == null) {
			ratedItemCount = ratedItemCount + 1;
			state.setRatedItemCount(user, ratedItemCount);
		}

		// Update average rating
		double oldAverageRating = state.getAverageRating(user);
		double newAverageRating;
		if (oldRating == null) {
			newAverageRating = (newRating / ratedItemCount) + oldAverageRating * (ratedItemCount - 1 )/ (ratedItemCount);
		} else {
			newAverageRating = oldAverageRating + (newRating - oldRating) / ratedItemCount;
		}
		state.setAverageRating(user, newAverageRating);

		// Emit values for new value stream
		collector.emit(new Values(user, item, newRating, oldRating, newAverageRating, oldAverageRating));
	}

	protected long getUser(TridentTuple tuple) {
		return tuple.getLong(0);
	}

	protected long getItem(TridentTuple tuple) {
		return tuple.getLong(1);
	}

	protected double getRating(TridentTuple tuple) {
		return tuple.getDouble(2);
	}
}

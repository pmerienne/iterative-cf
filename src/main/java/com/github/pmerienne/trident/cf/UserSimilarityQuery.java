package com.github.pmerienne.trident.cf;

import java.util.ArrayList;
import java.util.List;

import com.github.pmerienne.trident.cf.state.CFState;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class UserSimilarityQuery extends BaseQueryFunction<CFState, Double> {

	private static final long serialVersionUID = -3959281861317353583L;

	@Override
	public List<Double> batchRetrieve(CFState state, List<TridentTuple> args) {
		List<Double> similarities = new ArrayList<Double>(args.size());

		for (TridentTuple tuple : args) {
			long user1 = tuple.getLong(0);
			long user2 = tuple.getLong(1);
			similarities.add(state.getA(user1, user2));
		}

		return similarities;
	}

	@Override
	public void execute(TridentTuple tuple, Double result, TridentCollector collector) {
		collector.emit(new Values(result));
	}

}

package com.github.pmerienne.trident.cf;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.state.CFState;

public class FetchSimilarUsers extends BaseQueryFunction<CFState, Set<Long>> {

	private static final long serialVersionUID = -8188458622514047837L;

	public final static int DEFAULT_COUNT = 10;
	private int count = DEFAULT_COUNT;

	public FetchSimilarUsers() {
	}

	public FetchSimilarUsers(int count) {
		this.count = count;
	}

	@Override
	public List<Set<Long>> batchRetrieve(CFState state, List<TridentTuple> args) {
		List<Set<Long>> results = new ArrayList<Set<Long>>(args.size());

		long user1;
		for (TridentTuple tuple : args) {
			user1 = tuple.getLong(0);
			results.add(state.getMostSimilarUsers(user1, this.count));
		}

		return results;
	}

	@Override
	public void execute(TridentTuple tuple, Set<Long> result, TridentCollector collector) {
		long user1 = tuple.getLong(0);
		for (Long user : result) {
			if (user != user1) {
				collector.emit(new Values(user));
			}
		}
	}

}

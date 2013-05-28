package com.github.pmerienne.trident.cf;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.github.pmerienne.trident.cf.state.CFState;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class FetchOtherUsers extends BaseQueryFunction<CFState, Set<Long>> {

	private static final long serialVersionUID = -8188458622514047837L;

	@Override
	public List<Set<Long>> batchRetrieve(CFState state, List<TridentTuple> args) {
		Set<Long> users = state.getUsers();

		List<Set<Long>> results = new ArrayList<Set<Long>>(args.size());
		for (int i = 0; i < args.size(); i++) {
			results.add(users);
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

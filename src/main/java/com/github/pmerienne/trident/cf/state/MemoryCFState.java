package com.github.pmerienne.trident.cf.state;

import java.util.Map;
import java.util.Set;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.snapshot.Snapshottable;
import storm.trident.testing.MemoryMapState;
import backtype.storm.task.IMetricsContext;

public class MemoryCFState extends DelegateCFState {

	protected MemoryMapState<Double> as = new MemoryMapState<Double>("as");
	protected MemoryMapState<Double> bs = new MemoryMapState<Double>("bs");
	protected MemoryMapState<Double> cs = new MemoryMapState<Double>("cs");
	protected MemoryMapState<Double> ds = new MemoryMapState<Double>("ds");
	protected MemoryMapState<Map<Long, Double>> coRatedSums = new MemoryMapState<Map<Long, Double>>("coRatedSums");
	protected MemoryMapState<Long> coRatedCounts = new MemoryMapState<Long>("coRatedCounts");

	protected MemoryMapState<Long> ms = new MemoryMapState<Long>("ms");
	protected MemoryMapState<Double> averageRatings = new MemoryMapState<Double>("averageRatings");

	protected MemoryMapState<Double> ratings = new MemoryMapState<Double>("ratings");
	protected MemoryMapState<Map<Long, Double>> perUserRatings = new MemoryMapState<Map<Long, Double>>("perUserRatings");

	private MemoryMapState<Set<Long>> users = new MemoryMapState<Set<Long>>("users");

	@Override
	protected MemoryMapState<Double> getAsMapState() {
		return this.as;
	}

	@Override
	protected MemoryMapState<Double> getBsMapState() {
		return this.bs;
	}

	@Override
	protected MemoryMapState<Double> getCsMapState() {
		return this.cs;
	}

	@Override
	protected MemoryMapState<Double> getDsMapState() {
		return this.ds;
	}

	@Override
	protected MemoryMapState<Map<Long, Double>> getCoRatedSumsMapState() {
		return this.coRatedSums;
	}

	@Override
	protected MemoryMapState<Long> getCoRatedCountsMapState() {
		return this.coRatedCounts;
	}

	@Override
	protected MemoryMapState<Long> getMsMapState() {
		return this.ms;
	}

	@Override
	protected MemoryMapState<Double> getAverageRatingsMapState() {
		return this.averageRatings;
	}

	@Override
	protected MemoryMapState<Double> getRatingsMapState() {
		return this.ratings;
	}

	@Override
	protected MemoryMapState<Map<Long, Double>> getPerUserRatingsMapState() {
		return this.perUserRatings;
	}

	@Override
	protected Snapshottable<Set<Long>> getUsersMapState() {
		return this.users;
	}

	public static class Factory implements StateFactory {

		private static final long serialVersionUID = 4718043951532492603L;

		@SuppressWarnings("rawtypes")
		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new MemoryCFState();
		}

	}
}

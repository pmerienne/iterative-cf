package com.github.pmerienne.trident.cf;

import storm.trident.Stream;
import storm.trident.TridentTopology;

import backtype.storm.Config;

import com.github.pmerienne.trident.cf.TridentCollaborativeFiltering.Options;
import com.google.common.base.Preconditions;

public class TridentCollaborativeFilteringBuilder {

	private TridentTopology topology;
	private Options options = new Options();
	private Config config;
	private Stream preferenceStream;
	private Stream updateSimilaritiesStream;

	public TridentCollaborativeFilteringBuilder use(TridentTopology topology) {
		this.topology = topology;
		return this;
	}

	public TridentCollaborativeFilteringBuilder with(Options options) {
		this.options = options;
		return this;
	}

	public TridentCollaborativeFilteringBuilder configure(Config config) {
		this.config = config;
		return this;
	}

	public TridentCollaborativeFilteringBuilder process(Stream preferenceStream) {
		this.preferenceStream = preferenceStream;
		return this;
	}

	public TridentCollaborativeFilteringBuilder updateSimilaritiesOn(Stream updateSimilaritiesStream) {
		this.updateSimilaritiesStream = updateSimilaritiesStream;
		return this;
	}

	public TridentCollaborativeFiltering build() {
		Preconditions.checkArgument(this.topology != null, "You must provide a TridentTopology");
		Preconditions.checkArgument(this.preferenceStream != null, "You must provide a stream containing preferences (required fields : 'user','item'))");

		TridentCollaborativeFiltering tridentCollaborativeFiltering = new TridentCollaborativeFiltering(this.topology, this.options);
		tridentCollaborativeFiltering.appendUpdateUserPreferencesTopology(this.preferenceStream);

		if (this.updateSimilaritiesStream != null) {
			tridentCollaborativeFiltering.appendUpdateUserSimilaritiesTopology(this.updateSimilaritiesStream);
		}

		if (this.config != null) {
			tridentCollaborativeFiltering.registerKryoSerializers(config);
		}

		return tridentCollaborativeFiltering;
	}
}

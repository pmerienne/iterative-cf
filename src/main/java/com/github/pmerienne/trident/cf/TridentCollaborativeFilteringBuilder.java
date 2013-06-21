/**
 * Copyright 2013-2015 Pierre Merienne
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
		Preconditions.checkArgument(this.updateSimilaritiesStream != null, "You must provide a stream which trigger the similarity updates (no field is required)");

		TridentCollaborativeFiltering tridentCollaborativeFiltering = new TridentCollaborativeFiltering(this.topology, this.options);
		tridentCollaborativeFiltering.appendCollaborativeFilteringTopology(this.preferenceStream, this.updateSimilaritiesStream);

		if (this.config != null) {
			tridentCollaborativeFiltering.registerKryoSerializers(config);
		}

		return tridentCollaborativeFiltering;
	}
}

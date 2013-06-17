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
package com.github.pmerienne.trident.cf.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.cf.TridentCollaborativeFiltering;

@SuppressWarnings("rawtypes")
public class MovieLensPreferencesSpout implements IBatchSpout {

	private static final long serialVersionUID = -180727442291490144L;
	private static final Logger LOGGER = LoggerFactory.getLogger(MovieLensPreferencesSpout.class);

	private final static File MOVIE_LENS_FILE = new File("src/test/resources/movielens.csv");

	private int batchSize = 100;
	private static Iterator<Preference> it;
	static {
		try {
			loadPreferences();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	public MovieLensPreferencesSpout() {
		this(100);
	}

	public MovieLensPreferencesSpout(int batchSize) {
		this.batchSize = batchSize;
	}

	private static void loadPreferences() throws IOException {
		LOGGER.info("Loading movielens preferences from " + MOVIE_LENS_FILE);

		List<Preference> preferences = new ArrayList<Preference>();

		FileInputStream is = new FileInputStream(MOVIE_LENS_FILE);
		BufferedReader br = new BufferedReader(new InputStreamReader(is));

		try {
			String line;
			while ((line = br.readLine()) != null) {
				try {
					String[] values = line.split("\t");

					long user = Long.parseLong(values[0]);
					long item = Long.parseLong(values[1]);
					preferences.add(new Preference(user, item));
				} catch (Exception ex) {
					System.err.println("Skipped movie lens sample : " + line);
				}
			}
		} finally {
			is.close();
			br.close();
		}

		it = preferences.iterator();
	}

	@Override
	public void open(Map conf, TopologyContext context) {
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		Preference preference;

		int k = 0;
		while (k < this.batchSize && it.hasNext()) {
			preference = it.next();
			collector.emit(new Values(preference.user, preference.item));
			k++;
		}
	}

	@Override
	public void ack(long batchId) {
	}

	@Override
	public void close() {
	}

	@Override
	public Map getComponentConfiguration() {
		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		return conf;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields(TridentCollaborativeFiltering.USER_FIELD, TridentCollaborativeFiltering.ITEM_FIELD);
	}

	public boolean finished() {
		return !it.hasNext();
	}

	private static class Preference {
		public final long user;
		public final long item;

		public Preference(long user, long item) {
			this.user = user;
			this.item = item;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (item ^ (item >>> 32));
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
			Preference other = (Preference) obj;
			if (item != other.item)
				return false;
			if (user != other.user)
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "Preference [user=" + user + ", item=" + item + "]";
		}

	}
}

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
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;
import backtype.storm.Config;
import backtype.storm.tuple.Fields;

import com.github.pmerienne.trident.cf.model.RecommendedItem;
import com.github.pmerienne.trident.cf.model.SimilarUser;
import com.github.pmerienne.trident.cf.model.UserPair;
import com.github.pmerienne.trident.cf.model.WeightedPreferences;
import com.github.pmerienne.trident.cf.state.MemoryCFState;

/**
 * @author pmerienne
 * 
 */
public class TridentCollaborativeFiltering {

	public static final String USER_FIELD = "user";
	public static final String ITEM_FIELD = "item";
	public static final String USER2_FIELD = "user2";
	public static final String SIMILARITY_FIELD = "similarity";
	public static final String RECOMMENDED_ITEMS_FIELD = "recommendedItems";

	private static final String USER_PAIR_FIELD = "userPair";
	private static final String UNIQUE_USER_PAIR_FIELD = "uniqueUserPair";
	private static final String CO_PREFERENCE_COUNT = "coPreferenceCount";
	private static final String PREFERENCE_COUNT1_FIELD = "preferenceCount1";
	private static final String PREFERENCE_COUNT2_FIELD = "preferenceCount2";

	private static final String USER1_PREFERENCES = "user1Preferences";
	private static final String USER2_PREFERENCES = "user2Preferences";
	private static final String WEIGHTED_PREFERENCES_FIELD = "preferences";

	private StateFactory cfStateFactory;
	private TridentState cfState;

	public TridentCollaborativeFiltering(Options options) {
		super();
		this.cfStateFactory = options.cfStateFactory;
	}

	public TridentCollaborativeFiltering() {
		this(new Options());
	}

	public void registerKryoSerializers(Config config) {
		config.registerSerialization(RecommendedItem.class);
		config.registerSerialization(SimilarUser.class);
		config.registerSerialization(UserPair.class);
		config.registerSerialization(WeightedPreferences.class);
	}

	public void initSimilarityTopology(TridentTopology topology, Stream preferenceStream) {
		this.cfState = topology.newStaticState(this.cfStateFactory);

		preferenceStream
				// // Update user preference
				.partitionPersist(this.cfStateFactory, new Fields(USER_FIELD, ITEM_FIELD), new UserPreferenceUpdater(), new Fields(USER_FIELD, ITEM_FIELD))
				.newValuesStream()
				// Update user list
				.partitionPersist(this.cfStateFactory, new Fields(USER_FIELD, ITEM_FIELD), new UserListUpdater(), new Fields(USER_FIELD, ITEM_FIELD))
				.newValuesStream()
				// Update item list
				.partitionPersist(this.cfStateFactory, new Fields(USER_FIELD, ITEM_FIELD), new ItemListUpdater(), new Fields(USER_FIELD, ITEM_FIELD))
				.newValuesStream()
				// Get user 1 preference count
				.stateQuery(cfState, new Fields(USER_FIELD), new PreferenceCountQuery(), new Fields(PREFERENCE_COUNT1_FIELD))
				// Get other users
				.stateQuery(cfState, new Fields(USER_FIELD), new OtherUsersQuery(), new Fields(USER2_FIELD))
				// Get user 2 preference count
				.stateQuery(cfState, new Fields(USER2_FIELD), new PreferenceCountQuery(), new Fields(PREFERENCE_COUNT2_FIELD))
				.parallelismHint(5)
				// Debug("Preference count2"))

				// Remove duplicate user1, user2, item
				.each(new Fields(USER_FIELD, USER2_FIELD), new UserPairCreator(), new Fields(USER_PAIR_FIELD))
				.groupBy(new Fields(USER_PAIR_FIELD, ITEM_FIELD))
				.aggregate(new Fields(USER_PAIR_FIELD, ITEM_FIELD, PREFERENCE_COUNT1_FIELD, PREFERENCE_COUNT2_FIELD), new KeepFirst(), new Fields(UNIQUE_USER_PAIR_FIELD))
				.parallelismHint(10)
				.project(new Fields(UNIQUE_USER_PAIR_FIELD))
				.each(new Fields(UNIQUE_USER_PAIR_FIELD), new Expand(), new Fields(USER_PAIR_FIELD, ITEM_FIELD, PREFERENCE_COUNT1_FIELD, PREFERENCE_COUNT2_FIELD))

				// Update co preference count
				.partitionPersist(this.cfStateFactory, new Fields(USER_PAIR_FIELD, ITEM_FIELD, PREFERENCE_COUNT1_FIELD, PREFERENCE_COUNT2_FIELD), new CoPreferenceCountUpdater(),
						new Fields(USER_FIELD, USER2_FIELD, CO_PREFERENCE_COUNT, ITEM_FIELD, PREFERENCE_COUNT1_FIELD, PREFERENCE_COUNT2_FIELD)).parallelismHint(5).newValuesStream()

				// Update similariry
				.partitionPersist(this.cfStateFactory, new Fields(USER_FIELD, USER2_FIELD, PREFERENCE_COUNT1_FIELD, PREFERENCE_COUNT2_FIELD, CO_PREFERENCE_COUNT), new UserSimilarityUpdater())
				.parallelismHint(5);
	}

	public Stream createUserSimilarityStream(Stream inputStream) {
		return inputStream.stateQuery(this.cfState, new Fields(USER_FIELD, USER2_FIELD), new UserSimilarityQuery(), new Fields(SIMILARITY_FIELD)).project(new Fields(SIMILARITY_FIELD));
	}

	public Stream createItemRecommendationStream(Stream inputStream, int nbItems, int neighborhoodSize) {
		return inputStream
				// Get user1 ratings
				.stateQuery(this.cfState, new Fields(USER_FIELD), new UserPreferencesQuery(), new Fields(USER1_PREFERENCES))
				// Get top n similar users
				.stateQuery(this.cfState, new Fields(USER_FIELD), new SimilarUsersQuery(neighborhoodSize), new Fields(USER2_FIELD, SIMILARITY_FIELD))
				// Get similar users preferences
				.stateQuery(this.cfState, new Fields(USER2_FIELD), new UserPreferencesQuery(), new Fields(USER2_PREFERENCES)).parallelismHint(neighborhoodSize)
				// Aggregate similar users preference
				.aggregate(new Fields(USER1_PREFERENCES, USER2_PREFERENCES, SIMILARITY_FIELD), new PreferencesAggregator(), new Fields(WEIGHTED_PREFERENCES_FIELD))
				.parallelismHint(neighborhoodSize / 2)
				// Convert to recommended item
				.each(new Fields(WEIGHTED_PREFERENCES_FIELD), new TopNRecommendedItems(nbItems), new Fields(RECOMMENDED_ITEMS_FIELD))
				// Keep only recommendations
				.project(new Fields(RECOMMENDED_ITEMS_FIELD));
	}

	public static class Options {
		public StateFactory cfStateFactory = new MemoryCFState.Factory();
	}
}
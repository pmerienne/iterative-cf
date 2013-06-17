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

import com.github.pmerienne.trident.cf.aggregator.KeepFirst;
import com.github.pmerienne.trident.cf.aggregator.PreferencesAggregator;
import com.github.pmerienne.trident.cf.function.TanimotoCoefficientSimilarity;
import com.github.pmerienne.trident.cf.function.TopNRecommendedItems;
import com.github.pmerienne.trident.cf.function.UserPairCreator;
import com.github.pmerienne.trident.cf.model.RecommendedItem;
import com.github.pmerienne.trident.cf.model.SimilarUser;
import com.github.pmerienne.trident.cf.model.UserPair;
import com.github.pmerienne.trident.cf.model.WeightedPreferences;
import com.github.pmerienne.trident.cf.state.memory.MemoryMapMultimapState;
import com.github.pmerienne.trident.cf.state.memory.MemorySetMultiMapState;
import com.github.pmerienne.trident.cf.state.memory.MemorySetState;
import com.github.pmerienne.trident.cf.state.memory.MemorySortedSetMultiMapState;
import com.github.pmerienne.trident.cf.state.query.PreferenceCountQuery;
import com.github.pmerienne.trident.cf.state.query.SimilarUsersQuery;
import com.github.pmerienne.trident.cf.state.query.UserPreferencesQuery;
import com.github.pmerienne.trident.cf.state.query.UserSimilarityQuery;
import com.github.pmerienne.trident.cf.state.query.UsersWithCoPreferenceCountQuery;
import com.github.pmerienne.trident.cf.state.query.UsersWithPreferenceQuery;
import com.github.pmerienne.trident.cf.state.redis.RedisMapMultimapState;
import com.github.pmerienne.trident.cf.state.redis.RedisSetMultiMapState;
import com.github.pmerienne.trident.cf.state.redis.RedisSetState;
import com.github.pmerienne.trident.cf.state.redis.RedisSortedSetMultiMapState;
import com.github.pmerienne.trident.cf.state.updater.AddToUserList;
import com.github.pmerienne.trident.cf.state.updater.CoPreferenceCountUpdater;
import com.github.pmerienne.trident.cf.state.updater.GetAndClearUpdatedUsers;
import com.github.pmerienne.trident.cf.state.updater.PreferredItemUpdater;
import com.github.pmerienne.trident.cf.state.updater.UserPreferenceUpdater;
import com.github.pmerienne.trident.cf.state.updater.UserSimilarityUpdater;

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

	private StateFactory updatedUsersStateFactory;
	private StateFactory userPreferencesStateFactory;
	private StateFactory preferredItemsStateFactory;
	private StateFactory coPreferenceCountStateFactory;
	private StateFactory userSimilarityStateFactory;

	private TridentState updatedUsersState;
	private TridentState userPreferencesState;
	private TridentState preferredItemsState;
	private TridentState coPreferenceCountState;
	private TridentState userSimilarityState;

	private int singleUserOperationsForPreferenceUpdateParallelism;
	private int userPairOperationsForPreferenceUpdateParallelism;

	private int singleUserOperationsForSimilarityUpdateParallelism;
	private int userPairOperationsForSimilarityUpdateParallelism;

	public TridentCollaborativeFiltering(TridentTopology topology, Options options) {
		this.updatedUsersStateFactory = options.updatedUsersStateFactory;
		this.userPreferencesStateFactory = options.userPreferencesStateFactory;
		this.preferredItemsStateFactory = options.preferredItemsStateFactory;
		this.coPreferenceCountStateFactory = options.coPreferenceCountStateFactory;
		this.userSimilarityStateFactory = options.userSimilarityStateFactory;

		this.singleUserOperationsForPreferenceUpdateParallelism = options.singleUserOperationsForPreferenceUpdateParallelism;
		this.userPairOperationsForPreferenceUpdateParallelism = options.userPairOperationsForPreferenceUpdateParallelism;

		this.singleUserOperationsForSimilarityUpdateParallelism = options.singleUserOperationsForSimilarityUpdateParallelism;
		this.userPairOperationsForSimilarityUpdateParallelism = options.userPairOperationsForSimilarityUpdateParallelism;

		this.initStaticStates(topology);
	}

	public TridentCollaborativeFiltering(TridentTopology topology) {
		this(topology, new Options());
	}

	public void registerKryoSerializers(Config config) {
		config.registerSerialization(RecommendedItem.class);
		config.registerSerialization(SimilarUser.class);
		config.registerSerialization(UserPair.class);
		config.registerSerialization(WeightedPreferences.class);
	}

	public void appendCollaborativeFilteringTopology(Stream preferenceStream, Stream similaritiesUpdateStream) {
		this.initUpdatePreferencesTopology(preferenceStream);
		this.initUpdateSimilaritiesTopology(similaritiesUpdateStream);
	}

	protected void initStaticStates(TridentTopology topology) {
		this.userPreferencesState = topology.newStaticState(this.userPreferencesStateFactory);
		this.preferredItemsState = topology.newStaticState(preferredItemsStateFactory);
		this.updatedUsersState = topology.newStaticState(this.updatedUsersStateFactory);
		this.coPreferenceCountState = topology.newStaticState(this.coPreferenceCountStateFactory);
	}

	protected void initUpdatePreferencesTopology(Stream preferenceStream) {
		preferenceStream
				// Update user->items preferences
				.partitionPersist(this.userPreferencesStateFactory, new Fields(USER_FIELD, ITEM_FIELD), new UserPreferenceUpdater(), new Fields(USER_FIELD, ITEM_FIELD))
				.parallelismHint(this.singleUserOperationsForPreferenceUpdateParallelism)
				.newValuesStream()

				// Update item->users preferences
				.partitionPersist(this.preferredItemsStateFactory, new Fields(USER_FIELD, ITEM_FIELD), new PreferredItemUpdater(), new Fields(USER_FIELD, ITEM_FIELD))
				.parallelismHint(this.singleUserOperationsForPreferenceUpdateParallelism)
				.newValuesStream()

				// Add user to updated user list
				.partitionPersist(this.updatedUsersStateFactory, new Fields(USER_FIELD, ITEM_FIELD), new AddToUserList(), new Fields(USER_FIELD, ITEM_FIELD))
				.parallelismHint(this.singleUserOperationsForPreferenceUpdateParallelism)
				.newValuesStream()

				// Get other user which rated item
				.stateQuery(this.preferredItemsState, new Fields(ITEM_FIELD), new UsersWithPreferenceQuery(), new Fields(USER2_FIELD))
				.parallelismHint(this.singleUserOperationsForPreferenceUpdateParallelism)

				// Remove duplicate user1, user2, item
				.each(new Fields(USER_FIELD, USER2_FIELD), new UserPairCreator(), new Fields(USER_PAIR_FIELD)).parallelismHint(this.userPairOperationsForPreferenceUpdateParallelism)
				.groupBy(new Fields(USER_PAIR_FIELD, ITEM_FIELD)).aggregate(new Fields(USER_PAIR_FIELD), new KeepFirst<UserPair>(), new Fields(UNIQUE_USER_PAIR_FIELD))
				.parallelismHint(this.userPairOperationsForPreferenceUpdateParallelism)

				// Increment co preference count
				.partitionPersist(this.coPreferenceCountStateFactory, new Fields(UNIQUE_USER_PAIR_FIELD), new CoPreferenceCountUpdater(), new Fields(UNIQUE_USER_PAIR_FIELD, CO_PREFERENCE_COUNT))
				.parallelismHint(this.userPairOperationsForPreferenceUpdateParallelism);
	}

	protected void initUpdateSimilaritiesTopology(Stream updateSimilaritiesStream) {
		this.userSimilarityState = updateSimilaritiesStream

				// Get all updated user and clear list
				.partitionPersist(this.updatedUsersStateFactory, new GetAndClearUpdatedUsers(), new Fields(USER_FIELD))
				.parallelismHint(this.singleUserOperationsForSimilarityUpdateParallelism)
				.newValuesStream()

				// Get user1 preference count
				.stateQuery(this.userPreferencesState, new Fields(USER_FIELD), new PreferenceCountQuery(), new Fields(PREFERENCE_COUNT1_FIELD))
				.parallelismHint(this.singleUserOperationsForSimilarityUpdateParallelism)

				// Get users with co preference
				.stateQuery(this.coPreferenceCountState, new Fields(USER_FIELD), new UsersWithCoPreferenceCountQuery(), new Fields(USER2_FIELD, CO_PREFERENCE_COUNT))
				.parallelismHint(this.singleUserOperationsForSimilarityUpdateParallelism)

				// Get user2 preference count
				.stateQuery(this.userPreferencesState, new Fields(USER2_FIELD), new PreferenceCountQuery(), new Fields(PREFERENCE_COUNT2_FIELD))
				.parallelismHint(this.userPairOperationsForSimilarityUpdateParallelism)

				// Measure similarity
				.each(new Fields(PREFERENCE_COUNT1_FIELD, PREFERENCE_COUNT2_FIELD, CO_PREFERENCE_COUNT), new TanimotoCoefficientSimilarity(), new Fields(SIMILARITY_FIELD))
				.parallelismHint(this.userPairOperationsForSimilarityUpdateParallelism)

				// Update similarity
				.partitionPersist(this.userSimilarityStateFactory, new Fields(USER_FIELD, USER2_FIELD, SIMILARITY_FIELD), new UserSimilarityUpdater())

				.parallelismHint(this.userPairOperationsForSimilarityUpdateParallelism);
		;
	}

	public Stream createUserSimilarityStream(Stream inputStream) {
		return inputStream.stateQuery(this.userSimilarityState, new Fields(USER_FIELD, USER2_FIELD), new UserSimilarityQuery(), new Fields(SIMILARITY_FIELD)).project(new Fields(SIMILARITY_FIELD));
	}

	public Stream createItemRecommendationStream(Stream inputStream, int nbItems, int neighborhoodSize) {
		return inputStream
				// Get user1 ratings
				.stateQuery(this.userPreferencesState, new Fields(USER_FIELD), new UserPreferencesQuery(), new Fields(USER1_PREFERENCES))
				// Get top n similar users
				.stateQuery(this.userSimilarityState, new Fields(USER_FIELD), new SimilarUsersQuery(neighborhoodSize), new Fields(USER2_FIELD, SIMILARITY_FIELD))
				// Get similar users preferences
				.stateQuery(this.userPreferencesState, new Fields(USER2_FIELD), new UserPreferencesQuery(), new Fields(USER2_PREFERENCES)).parallelismHint(neighborhoodSize)
				// Aggregate similar users preference
				.aggregate(new Fields(USER1_PREFERENCES, USER2_PREFERENCES, SIMILARITY_FIELD), new PreferencesAggregator(), new Fields(WEIGHTED_PREFERENCES_FIELD))
				.parallelismHint(neighborhoodSize / 2)
				// Convert to recommended item
				.each(new Fields(WEIGHTED_PREFERENCES_FIELD), new TopNRecommendedItems(nbItems), new Fields(RECOMMENDED_ITEMS_FIELD))
				// Keep only recommendations
				.project(new Fields(RECOMMENDED_ITEMS_FIELD));
	}

	public static class Options {
		public StateFactory updatedUsersStateFactory = new MemorySetState.Factory();
		public StateFactory userPreferencesStateFactory = new MemorySetMultiMapState.Factory();
		public StateFactory preferredItemsStateFactory = new MemorySetMultiMapState.Factory();
		public StateFactory coPreferenceCountStateFactory = new MemoryMapMultimapState.Factory();
		public StateFactory userSimilarityStateFactory = new MemorySortedSetMultiMapState.Factory();

		public int singleUserOperationsForPreferenceUpdateParallelism = 2;
		public int userPairOperationsForPreferenceUpdateParallelism = 10;

		public int singleUserOperationsForSimilarityUpdateParallelism = 2;
		public int userPairOperationsForSimilarityUpdateParallelism = 20;

		public static Options inMemory() {
			return new Options();
		}

		public static Options redis() {
			Options options = new Options();
			options.updatedUsersStateFactory = new RedisSetState.Factory("users");
			options.userPreferencesStateFactory = new RedisSetMultiMapState.Factory("userPreferences");
			options.preferredItemsStateFactory = new RedisSetMultiMapState.Factory("preferredItems");
			options.coPreferenceCountStateFactory = new RedisMapMultimapState.Factory("coPreferenceCount");
			options.userSimilarityStateFactory = new RedisSortedSetMultiMapState.Factory("userSimilarity");
			return options;
		}

	}
}
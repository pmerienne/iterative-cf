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
import storm.trident.state.StateFactory;
import backtype.storm.Config;
import backtype.storm.tuple.Fields;

import com.github.pmerienne.trident.cf.model.RecommendedItem;
import com.github.pmerienne.trident.cf.model.WeightedRating;
import com.github.pmerienne.trident.cf.model.WeightedRatings;

public class CFTopology {

	public final static String DEFAULT_USER1_FIELD = "user1";
	public final static String DEFAULT_USER2_FIELD = "user2";
	public final static String DEFAULT_ITEM_FIELD = "item";
	public final static String DEFAULT_RATING_FIELD = "rating";
	public final static String DEFAULT_SIMILARITY_FIELD = "similarity";
	public static String DEFAULT_RECOMMENDED_ITEMS_FIELD = "recommended_items_ratings";
	public final static int DEFAULT_NEIGHBOTHOOD_SIZE = 10;

	private final static String USER1_FIELD = "user1";
	private final static String USER2_FIELD = "user2";
	private final static String ITEM_FIELD = "item";
	private final static String USER1_NEW_RATING_FIELD = "user1_new_rating";
	private final static String USER1_OLD_RATING_FIELD = "user1_old_rating";
	private final static String USER1_NEW_AVERAGE_RATING_FIELD = "user1_new_average_rating";
	private final static String USER1_OLD_AVERAGE_RATING_FIELD = "user1_old_average_rating";
	private final static String USER2_RATING_FIELD = "user2_rating";
	private final static String CO_RATED_COUNT_FIELD = "co_rated_count";
	private final static String CO_RATED_SUM1_FIELD = "co_rated_sum1";
	private final static String CO_RATED_SUM2_FIELD = "co_rated_sum2";
	private final static String USER1_RATINGS_FIELD = "user1_ratings";
	private final static String USER2_RATINGS_FIELD = "user2_ratings";
	private final static String RATING_AVERAGES_FIELD = "rating_average";

	private final Stream ratingStream;
	private final StateFactory cfStateFactory;

	private String user1Field = DEFAULT_USER1_FIELD;
	private String itemField = DEFAULT_ITEM_FIELD;
	private String ratingField = DEFAULT_RATING_FIELD;

	private TridentState cfState;

	public CFTopology(Stream ratingStream, StateFactory cfStateFactory) {
		this.ratingStream = ratingStream;
		this.cfStateFactory = cfStateFactory;
		this.initRatingTopology();
	}

	public CFTopology(Stream ratingStream, StateFactory cfStateFactory, Config config) {
		this.ratingStream = ratingStream;
		this.cfStateFactory = cfStateFactory;
		this.configure(config);
		this.initRatingTopology();
	}

	public CFTopology(Stream ratingStream, StateFactory cfStateFactory, String userField, String itemField, String ratingField) {
		this.ratingStream = ratingStream;
		this.cfStateFactory = cfStateFactory;
		this.user1Field = userField;
		this.itemField = itemField;
		this.ratingField = ratingField;
		this.initRatingTopology();
	}

	public CFTopology(Stream ratingStream, StateFactory cfStateFactory, Config config, String userField, String itemField, String ratingField) {
		this.ratingStream = ratingStream;
		this.cfStateFactory = cfStateFactory;
		this.user1Field = userField;
		this.itemField = itemField;
		this.ratingField = ratingField;
		this.configure(config);
		this.initRatingTopology();
	}

	public void configure(Config config) {
		config.registerSerialization(WeightedRating.class);
		config.registerSerialization(WeightedRatings.class);
		config.registerSerialization(RecommendedItem.class);
	}

	private void initRatingTopology() {
		Fields updateUserCacheOutputfields = new Fields(USER1_FIELD, ITEM_FIELD, USER1_NEW_RATING_FIELD, USER1_OLD_RATING_FIELD, USER1_NEW_AVERAGE_RATING_FIELD, USER1_OLD_AVERAGE_RATING_FIELD);
		Fields updateUserPairCacheInputFields = new Fields(USER1_FIELD, USER2_FIELD, ITEM_FIELD, USER1_NEW_RATING_FIELD, USER1_OLD_RATING_FIELD, USER1_NEW_AVERAGE_RATING_FIELD,
				USER1_OLD_AVERAGE_RATING_FIELD);
		Fields updateUserPairCacheOuputFields = new Fields(USER1_FIELD, USER2_FIELD, ITEM_FIELD, USER1_NEW_RATING_FIELD, USER1_OLD_RATING_FIELD, USER2_RATING_FIELD, USER1_NEW_AVERAGE_RATING_FIELD,
				USER1_OLD_AVERAGE_RATING_FIELD, CO_RATED_COUNT_FIELD, CO_RATED_SUM1_FIELD, CO_RATED_SUM2_FIELD);

		this.cfState = this.ratingStream
		// Update user cache
				.partitionPersist(this.cfStateFactory, new Fields(this.user1Field, this.itemField, this.ratingField), new UpdateUserCache(), updateUserCacheOutputfields);
		this.cfState.newValuesStream()
		// Get all other users
				.stateQuery(this.cfState, new Fields(USER1_FIELD), new FetchOtherUsers(), new Fields(USER2_FIELD))
				// Update user pair cache
				.partitionPersist(this.cfStateFactory, updateUserPairCacheInputFields, new UpdateUserPairCache(), updateUserPairCacheOuputFields).newValuesStream()
				// Update similarity
				.partitionPersist(this.cfStateFactory, updateUserPairCacheOuputFields, new UpdateUserSimilarity());
	}

	public Stream createUserSimilarityStream(Stream inputStream) {
		return this.createSimilarityStream(inputStream, DEFAULT_SIMILARITY_FIELD, DEFAULT_USER1_FIELD, DEFAULT_USER2_FIELD);
	}

	public Stream createSimilarityStream(Stream inputStream, String similarityField, String user1Field, String user2Field) {
		return inputStream.stateQuery(this.cfState, new Fields(user1Field, user2Field), new UserSimilarityQuery(), new Fields(similarityField)).project(new Fields(similarityField));
	}

	public Stream createRecommendationStream(Stream inputStream, int nbItems) {
		return this.createRecommendationStream(inputStream, nbItems, DEFAULT_NEIGHBOTHOOD_SIZE, DEFAULT_USER1_FIELD, DEFAULT_RECOMMENDED_ITEMS_FIELD);
	}

	public Stream createRecommendationStream(Stream inputStream, int nbItems, int neighborhoodSize) {
		return this.createRecommendationStream(inputStream, nbItems, neighborhoodSize, DEFAULT_USER1_FIELD, DEFAULT_RECOMMENDED_ITEMS_FIELD);
	}

	public Stream createRecommendationStream(Stream inputStream, int nbItems, int neighborhoodSize, String userField, String recommendedItemField) {
		return inputStream
		// Get user1 ratings
				.stateQuery(this.cfState, new Fields(userField), new UserRatingsQuery(), new Fields(USER1_RATINGS_FIELD))
				// Get all similar users
				.stateQuery(this.cfState, new Fields(userField), new FetchSimilarUsers(neighborhoodSize), new Fields(USER2_FIELD, DEFAULT_SIMILARITY_FIELD))
				// Get similar users ratings
				.stateQuery(this.cfState, new Fields(USER2_FIELD), new UserRatingsQuery(), new Fields(USER2_RATINGS_FIELD))
				// Average unrated ratings
				.aggregate(new Fields(USER1_RATINGS_FIELD, USER2_RATINGS_FIELD, DEFAULT_SIMILARITY_FIELD), new UnratedItemsCombiner(), new Fields(RATING_AVERAGES_FIELD))
				// Convert to recommended item
				.each(new Fields(RATING_AVERAGES_FIELD), new TopNRecommendedItems(nbItems), new Fields(recommendedItemField)).project(new Fields(recommendedItemField));
	}
}

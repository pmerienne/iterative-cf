package com.github.pmerienne.trident.cf.state;

import java.util.Map;
import java.util.Set;

import storm.trident.state.State;

public interface CFState extends State {

	Set<Long> getUsers();
	
	Set<Long> getMostSimilarUsers(long user, int count);

	void addUser(long user);

	long getRatedItemCount(long user);

	void setRatedItemCount(long user, long count);

	void setAverageRating(long user, double newAverageRating);

	double getAverageRating(long user);

	Double getRating(long user, long item);

	void addRating(long user, long item, double rating);

	long getCoRatedCount(long user1, long user2);

	void setCoRatedCount(long user1, long user2, long count);

	Map<Long, Double> getCoRatedSums(long user1, long user2);

	void setCoRatedSums(long user1, long user2, Map<Long, Double> coRatedSums);

	double getA(long user1, long user2);

	void setA(long user1, long user2, double a);

	double getB(long user1, long user2);

	void setB(long user1, long user2, double b);

	double getC(long user1, long user2);

	void setC(long user1, long user2, double c);

	double getD(long user1, long user2);

	void setD(long user1, long user2, double d);
	
	Map<Long, Double> getRatings(long user);
}

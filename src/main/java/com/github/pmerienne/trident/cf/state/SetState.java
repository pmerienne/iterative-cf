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
package com.github.pmerienne.trident.cf.state;

import java.util.Collection;
import java.util.Set;

import storm.trident.state.State;

/**
 * A {@link State} which wrap access to a Set of elements.
 * 
 * @author pmerienne
 * 
 * @param <E>
 *            the type of elements maintained by this set
 */
public interface SetState<E> extends State {

	/**
	 * Adds the specified element to this set if it is not already present.
	 * 
	 * @param e
	 *            element to be added to this set
	 * @return <code>true</code> if this set did not already contain the
	 *         specified element
	 */
	boolean add(E e);

	/**
	 * Adds all of the elements in the specified collection to this set if
	 * they're not already present (optional operation).
	 * 
	 * @param c
	 *            collection containing elements to be added to this set
	 * @return <code>true</code> if this set changed as a result of the call
	 */
	boolean addAll(Collection<? extends E> c);

	/**
	 * @return the whole {@link Set} contained in this {@link State}
	 */
	Set<E> get();

	/**
	 * Clear the whole {@link Set} contained in this {@link State}
	 */
	void clear();
}

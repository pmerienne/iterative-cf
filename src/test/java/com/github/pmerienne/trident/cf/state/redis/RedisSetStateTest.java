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
package com.github.pmerienne.trident.cf.state.redis;

import org.junit.After;
import org.junit.Before;

import com.github.pmerienne.trident.cf.state.SetStateTest;
import com.github.pmerienne.trident.cf.testing.TestValue;

public class RedisSetStateTest extends SetStateTest {

	@Before
	public void setup() {
		this.state = new RedisSetState<TestValue>("test");
	}

	@After
	public void cleanup() {
		((AbstractRedisState) this.state).flushAll();
	}
}
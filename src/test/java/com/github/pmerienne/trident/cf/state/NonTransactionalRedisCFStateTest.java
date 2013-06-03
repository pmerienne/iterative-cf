package com.github.pmerienne.trident.cf.state;

import org.junit.Before;

public class NonTransactionalRedisCFStateTest extends AbstractCFStateTest {

	@Before
	public void setup() {
		// Init state
		this.state = new NonTransactionalRedisCFState();
		this.state.drop();
	}

}
package com.github.pmerienne.trident.cf.state;

import org.junit.Before;

public class InMemoryCFStateTest extends AbstractCFStateTest {

	@Before
	public void setup() {
		this.state = new InMemoryCFState();
	}

}

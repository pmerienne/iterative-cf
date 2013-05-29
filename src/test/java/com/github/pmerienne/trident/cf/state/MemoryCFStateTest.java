package com.github.pmerienne.trident.cf.state;

import org.junit.Before;


public class MemoryCFStateTest  extends AbstractCFStateTest {

	@Before
	public void setup() {
		this.state = new MemoryCFState();
	}

}
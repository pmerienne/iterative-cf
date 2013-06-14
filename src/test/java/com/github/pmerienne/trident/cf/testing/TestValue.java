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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class TestValue {

	private Double testDouble;
	private String testString;
	private List<Integer> testIntegerList = new ArrayList<Integer>();

	public TestValue() {
	}

	public TestValue(Double testDouble, String testString) {
		this.testDouble = testDouble;
		this.testString = testString;
	}

	public TestValue(Double testDouble, String testString, List<Integer> testIntegerList) {
		this.testDouble = testDouble;
		this.testString = testString;
		this.testIntegerList = testIntegerList;
	}

	public Double getTestDouble() {
		return testDouble;
	}

	public void setTestDouble(Double testDouble) {
		this.testDouble = testDouble;
	}

	public String getTestString() {
		return testString;
	}

	public void setTestString(String testString) {
		this.testString = testString;
	}

	public List<Integer> getTestIntegerList() {
		return testIntegerList;
	}

	public void setTestIntegerList(List<Integer> testIntegerList) {
		this.testIntegerList = testIntegerList;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((testDouble == null) ? 0 : testDouble.hashCode());
		result = prime * result + ((testIntegerList == null) ? 0 : testIntegerList.hashCode());
		result = prime * result + ((testString == null) ? 0 : testString.hashCode());
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
		TestValue other = (TestValue) obj;
		if (testDouble == null) {
			if (other.testDouble != null)
				return false;
		} else if (!testDouble.equals(other.testDouble))
			return false;
		if (testIntegerList == null) {
			if (other.testIntegerList != null)
				return false;
		} else if (!testIntegerList.equals(other.testIntegerList))
			return false;
		if (testString == null) {
			if (other.testString != null)
				return false;
		} else if (!testString.equals(other.testString))
			return false;
		return true;
	}

	public static TestValue random() {
		TestValue testValue = new TestValue(new Random().nextDouble(), UUID.randomUUID().toString());
		for (int i = 0; i < 7; i++) {
			testValue.getTestIntegerList().add(new Random().nextInt());
		}
		return testValue;
	}

}

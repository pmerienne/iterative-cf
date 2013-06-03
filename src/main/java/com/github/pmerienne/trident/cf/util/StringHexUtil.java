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
package com.github.pmerienne.trident.cf.util;

public class StringHexUtil {
	public static String toHexString(byte[] input) {
		return new String(toHexCharArray(input));
	}

	public static char[] toHexCharArray(byte[] input) {
		int m = input.length;
		int n = 2 * m;
		int l = 0;
		char[] output = new char[n];
		for (int k = 0; k < m; k++) {
			byte v = input[k];
			int i = (v >> 4) & 0xf;
			output[l++] = (char) (i >= 10 ? ('a' + i - 10) : ('0' + i));
			i = v & 0xf;
			output[l++] = (char) (i >= 10 ? ('a' + i - 10) : ('0' + i));
		}

		return output;
	}

	public static byte[] fromHexString(String input) {
		int n = input.length() / 2;
		byte[] output = new byte[n];
		int l = 0;
		for (int k = 0; k < n; k++) {
			char c = input.charAt(l++);
			byte b = (byte) ((c >= 'a' ? (c - 'a' + 10) : (c - '0')) << 4);
			c = input.charAt(l++);
			b |= (byte) (c >= 'a' ? (c - 'a' + 10) : (c - '0'));
			output[k] = b;
		}
		return output;
	}
}
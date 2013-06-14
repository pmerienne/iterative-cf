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

import java.io.ByteArrayOutputStream;

import storm.trident.state.Serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.pmerienne.trident.cf.util.StringHexUtil;

public class KryoValueSerializer implements Serializer<Object> {

	private static final long serialVersionUID = 1689053104987281146L;

	private Kryo kryo;

	public KryoValueSerializer() {
		this.kryo = new Kryo();
	}

	@Override
	public byte[] serialize(Object obj) {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		Output output = new Output(stream);

		this.kryo.writeClassAndObject(output, obj);
		output.close();

		byte[] buffer = stream.toByteArray();

		// Trick to avoid serialization problem!
		String data = StringHexUtil.toHexString(buffer);
		return data.getBytes();
	}

	@Override
	public Object deserialize(byte[] bytes) {
		// Trick to avoid serialization problem!
		String data = new String(bytes);
		bytes = StringHexUtil.fromHexString(data);

		Input input = new Input(bytes);
		Object obj = this.kryo.readClassAndObject(input);
		return obj;
	}
}
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

import java.io.ByteArrayOutputStream;

import storm.trident.state.Serializer;
import storm.trident.state.TransactionalValue;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.github.pmerienne.trident.cf.util.StringHexUtil;

@SuppressWarnings({ "rawtypes" })
public class KryoTransactionalValueSerializer<T> implements Serializer<TransactionalValue> {

	private static final long serialVersionUID = 1689053104987281146L;

	private Kryo kryo;

	@SuppressWarnings("unchecked")
	public KryoTransactionalValueSerializer() {
		this.kryo = new Kryo();
		this.kryo.register(TransactionalValue.class, new FieldSerializer(this.kryo, TransactionalValue.class) {
			public Object create(Kryo kryo, Input input, Class type) {
				return new TransactionalValue(null, null);
			}
		});
	}

	@Override
	public byte[] serialize(TransactionalValue obj) {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		Output output = new Output(stream);

		this.kryo.writeObjectOrNull(output, obj, TransactionalValue.class);
		output.close();

		byte[] buffer = stream.toByteArray();

		// Trick to avoid serialization problem!
		String data = StringHexUtil.toHexString(buffer);
		return data.getBytes();
	}

	@Override
	public TransactionalValue deserialize(byte[] bytes) {
		// Trick to avoid serialization problem!
		String data = new String(bytes);
		bytes = StringHexUtil.fromHexString(data);

		Input input = new Input(bytes);
		TransactionalValue obj = this.kryo.readObjectOrNull(input, TransactionalValue.class);
		return obj;
	}
}
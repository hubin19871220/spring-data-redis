/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.lettuce;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyValue;
import org.springframework.data.redis.test.util.LettuceRedisClientProvider;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.api.sync.RedisCommands;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveCommandsTestsBase {

	public static @ClassRule LettuceRedisClientProvider clientProvider = LettuceRedisClientProvider.local();

	static final String KEY_1 = "key-1";
	static final String KEY_2 = "key-2";
	static final String KEY_3 = "key-3";
	static final String VALUE_1 = "value-1";
	static final String VALUE_2 = "value-2";
	static final String VALUE_3 = "value-3";

	static final byte[] KEY_1_BYTES = KEY_1.getBytes(Charset.forName("UTF-8"));
	static final byte[] KEY_2_BYTES = KEY_2.getBytes(Charset.forName("UTF-8"));
	static final byte[] KEY_3_BYTES = KEY_3.getBytes(Charset.forName("UTF-8"));
	static final byte[] VALUE_1_BYTES = VALUE_1.getBytes(Charset.forName("UTF-8"));
	static final byte[] VALUE_2_BYTES = VALUE_2.getBytes(Charset.forName("UTF-8"));
	static final byte[] VALUE_3_BYTES = VALUE_3.getBytes(Charset.forName("UTF-8"));

	static final ByteBuffer KEY_1_BBUFFER = ByteBuffer.wrap(KEY_1_BYTES);
	static final ByteBuffer VALUE_1_BBUFFER = ByteBuffer.wrap(VALUE_1_BYTES);

	static final ByteBuffer KEY_2_BBUFFER = ByteBuffer.wrap(KEY_2_BYTES);
	static final ByteBuffer VALUE_2_BBUFFER = ByteBuffer.wrap(VALUE_2_BYTES);

	static final ByteBuffer KEY_3_BBUFFER = ByteBuffer.wrap(KEY_3_BYTES);
	static final ByteBuffer VALUE_3_BBUFFER = ByteBuffer.wrap(VALUE_3_BYTES);

	static final KeyValue KV_1 = new KeyValue(KEY_1_BBUFFER, VALUE_1_BBUFFER);
	static final KeyValue KV_2 = new KeyValue(KEY_2_BBUFFER, VALUE_2_BBUFFER);
	static final KeyValue KV_3 = new KeyValue(KEY_3_BBUFFER, VALUE_3_BBUFFER);

	LettuceReactiveRedisConnection connection;
	RedisCommands<String, String> nativeCommands;

	@Before
	public void setUp() {

		RedisClient client = clientProvider.getClient();
		nativeCommands = client.connect().sync();
		connection = new LettuceReactiveRedisConnection(client);
	}

	@After
	public void tearDown() {

		flushAll();
		nativeCommands.close();
		connection.close();
	}

	private void flushAll() {
		nativeCommands.flushall();
	}

}

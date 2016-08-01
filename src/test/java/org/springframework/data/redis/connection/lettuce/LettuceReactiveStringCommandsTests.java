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

import static org.hamcrest.collection.IsIterableContainingInOrder.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyValue;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands.GetResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands.GetSetResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands.MGetResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands.SetResponse;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.TestSubscriber;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveStringCommandsTests extends LettuceReactiveCommandsTestsBase {

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getSetShouldReturnPreviousValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		Mono<ByteBuffer> result = connection.stringCommands().getSet(KEY_1_BBUFFER, VALUE_2_BBUFFER);

		assertThat(result.block(), is(equalTo(VALUE_1_BBUFFER)));
		assertThat(nativeCommands.get(KEY_1), is(equalTo(VALUE_2)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getSetShouldReturnPreviousValueCorrectlyWhenNoExists() {

		Mono<ByteBuffer> result = connection.stringCommands().getSet(KEY_1_BBUFFER, VALUE_2_BBUFFER);

		ByteBuffer value = result.block();
		assertThat(value, is(notNullValue()));
		assertThat(value, is(equalTo(ByteBuffer.allocate(0))));
		assertThat(nativeCommands.get(KEY_1), is(equalTo(VALUE_2)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getSetShouldReturnPreviousValuesCorrectly() {

		KeyValue kv12 = new KeyValue(KEY_1_BBUFFER, VALUE_2_BBUFFER);
		Flux<GetSetResponse> result = connection.stringCommands().getSet(Flux.fromIterable(Arrays.asList(KV_1, kv12)));

		TestSubscriber<GetSetResponse> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(2);

		subscriber.assertValues(new GetSetResponse(KV_1, ByteBuffer.allocate(0)),
				new GetSetResponse(kv12, VALUE_1_BBUFFER));
		assertThat(nativeCommands.get(KEY_1), is(equalTo(VALUE_2)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void setShouldAddValueCorrectly() {

		Mono<Boolean> result = connection.stringCommands().set(KEY_1_BBUFFER, VALUE_1_BBUFFER);

		assertThat(result.block(), is(true));
		assertThat(nativeCommands.get(KEY_1), is(equalTo(VALUE_1)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void setShouldAddValuesCorrectly() {

		Flux<SetResponse> result = connection.stringCommands().set(Flux.fromIterable(Arrays.asList(KV_1, KV_2)));

		TestSubscriber<SetResponse> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(2);
		assertThat(nativeCommands.get(KEY_1), is(equalTo(VALUE_1)));
		assertThat(nativeCommands.get(KEY_2), is(equalTo(VALUE_2)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getShouldRetriveValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		Mono<ByteBuffer> result = connection.stringCommands().get(KEY_1_BBUFFER);
		assertThat(result.block(), is(equalTo(VALUE_1_BBUFFER)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getShouldRetriveNullValueCorrectly() {

		Mono<ByteBuffer> result = connection.stringCommands().get(KEY_1_BBUFFER);
		assertThat(result.block(), is(equalTo(ByteBuffer.allocate(0))));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getShouldRetriveValuesCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<GetResponse> result = connection.stringCommands()
				.get(Flux.fromStream(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER).stream()));

		TestSubscriber<GetResponse> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(2);
		subscriber.assertContainValues(new HashSet<>(Arrays.asList(new GetResponse(KEY_1_BBUFFER, VALUE_1_BBUFFER),
				new GetResponse(KEY_2_BBUFFER, VALUE_2_BBUFFER))));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getShouldRetriveValuesWithNullCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_3, VALUE_3);

		Flux<GetResponse> result = connection.stringCommands()
				.get(Flux.fromStream(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER, KEY_3_BBUFFER).stream()));

		TestSubscriber<GetResponse> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(3);
		subscriber.assertContainValues(new HashSet<>(Arrays.asList(new GetResponse(KEY_1_BBUFFER, VALUE_1_BBUFFER),
				new GetResponse(KEY_2_BBUFFER, ByteBuffer.allocate(0)), new GetResponse(KEY_3_BBUFFER, VALUE_3_BBUFFER))));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void mGetShouldRetriveValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Mono<List<ByteBuffer>> result = connection.stringCommands().mGet(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER));
		assertThat(result.block(), contains(VALUE_1_BBUFFER, VALUE_2_BBUFFER));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void mGetShouldRetriveNullValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_3, VALUE_3);

		Mono<List<ByteBuffer>> result = connection.stringCommands()
				.mGet(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER, KEY_3_BBUFFER));

		assertThat(result.block(), contains(VALUE_1_BBUFFER, ByteBuffer.allocate(0), VALUE_3_BBUFFER));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void mGetShouldRetriveValuesCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<List<ByteBuffer>> result = connection.stringCommands()
				.mGet(
						Flux.fromIterable(Arrays.asList(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), Arrays.asList(KEY_2_BBUFFER))))
				.map(MGetResponse::getOutput);

		TestSubscriber<List<ByteBuffer>> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(2);
		subscriber.assertContainValues(
				new HashSet<>(Arrays.asList(Arrays.asList(VALUE_1_BBUFFER, VALUE_2_BBUFFER), Arrays.asList(VALUE_2_BBUFFER))));
	}

}

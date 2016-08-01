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

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveKeyCommands.DelResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveKeyCommands.MDelResponse;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.TestSubscriber;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveKeyCommandsTests extends LettuceReactiveCommandsTestsBase {

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void shouldDeleteKeyCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		Mono<Long> result = connection.keyCommands().del(KEY_1_BBUFFER);
		assertThat(result.block(), is(1L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void shouldDeleteKeysCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<DelResponse> result = connection.keyCommands()
				.del(Flux.fromIterable(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)));

		TestSubscriber<DelResponse> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(2);
		subscriber.assertValues(new DelResponse(KEY_1_BBUFFER, 1L), new DelResponse(KEY_2_BBUFFER, 1L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void shouldDeleteKeysInBatchCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Mono<Long> result = connection.keyCommands().mDel(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER));

		assertThat(result.block(), is(2L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void shouldDeleteKeysInMultipleBatchesCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<Long> result = connection.keyCommands()
				.mDel(
						Flux.fromIterable(Arrays.asList(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), Arrays.asList(KEY_1_BBUFFER))))
				.map(MDelResponse::getOutput);

		TestSubscriber<Long> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(2);
		subscriber.assertValues(2L, 0L);
	}

}

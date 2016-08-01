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
package org.springframework.data.redis.connection;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;

import lombok.Data;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveRedisConnection extends Closeable {

	/**
	 * Get {@link ReactiveKeyCommands}.
	 * 
	 * @return never {@literal null}
	 */
	ReactiveKeyCommands keyCommands();

	/**
	 * Get {@link ReactiveStringCommands}.
	 * 
	 * @return never {@literal null}
	 */
	ReactiveStringCommands stringCommands();

	@Data
	static class CommandResponse<I, O> {

		private final I input;
		private final O output;
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@Data
	static class KeyValue {

		final ByteBuffer key;
		final ByteBuffer value;

		public byte[] keyAsBytes() {
			return key.array();
		}

		public byte[] valueAsBytes() {
			return value.array();
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static interface ReactiveStringCommands {

		/**
		 * Set {@literal value} for {@literal key}.
		 * 
		 * @param key must not be {@literal null}.
		 * @param value must not be {@literal null}.
		 * @return
		 */
		default Mono<Boolean> set(ByteBuffer key, ByteBuffer value) {

			Assert.notNull(key, "Key must not be null!");
			Assert.notNull(value, "Value must not be null!");

			return set(Mono.just(new KeyValue(key, value))).next().map(SetResponse::getOutput);
		}

		/**
		 * Set each and every {@link KeyValue} item separately.
		 * 
		 * @param values must not be {@literal null}.
		 * @return {@link Flux} of {@link SetResponse} holding the {@link KeyValue} pair to set along with the command
		 *         result.
		 */
		Flux<SetResponse> set(Publisher<KeyValue> values);

		/**
		 * Get single element stored at {@literal key}.
		 * 
		 * @param key must not be {@literal null}.
		 * @return empty {@link ByteBuffer} in case {@literal key} does not exist.
		 */
		default Mono<ByteBuffer> get(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");
			return get(Mono.just(key)).next().map((result) -> result.getOutput());
		}

		/**
		 * Get elements one by one.
		 * 
		 * @param keys must not be {@literal null}.
		 * @return {@link Flux} of {@link GetResponse} holding the {@literal key} to get along with the value retrieved.
		 */
		Flux<GetResponse> get(Publisher<ByteBuffer> keys);

		/**
		 * Set {@literal value} for {@literal key} and return the existing value.
		 * 
		 * @param key must not be {@literal null}.
		 * @param value must not be {@literal null}.
		 * @return
		 */
		default Mono<ByteBuffer> getSet(ByteBuffer key, ByteBuffer value) {

			Assert.notNull(key, "Key must not be null!");
			Assert.notNull(value, "Value must not be null!");

			return getSet(Mono.just(new KeyValue(key, value))).next().map(GetSetResponse::getOutput);
		}

		/**
		 * Set {@literal value} for {@literal key} and return the existing value one by one.
		 * 
		 * @param key must not be {@literal null}.
		 * @param value must not be {@literal null}.
		 * @return {@link Flux} of {@link GetSetResponse} holding the {@link KeyValue} pair to set along with the previously
		 *         existing value.
		 */
		Flux<GetSetResponse> getSet(Publisher<KeyValue> values);

		/**
		 * Set {@literal value} for {@literal key} with {@literal expiration} and {@literal options}.
		 * 
		 * @param key must not be {@literal null}.
		 * @param value must not be {@literal null}.
		 * @param expiration must not be {@literal null}.
		 * @param option must not be {@literal null}.
		 * @return
		 */
		default Mono<Boolean> set(ByteBuffer key, ByteBuffer value, Expiration expiration, SetOption option) {

			Assert.notNull(key, "Key must not be null!");
			Assert.notNull(value, "Value must not be null!");
			Assert.notNull(expiration, "Expiration must not be null!");
			Assert.notNull(option, "Option must not be null!");

			return set(Mono.just(new KeyValue(key, value)), () -> expiration, () -> option).next()
					.map(SetResponse::getOutput);
		}

		/**
		 * Set {@literal value} for {@literal key} with {@literal expiration} and {@literal options} one by one.
		 * 
		 * @param values must not be {@literal null}.
		 * @param expiration must not be {@literal null}.
		 * @param option must not be {@literal null}.
		 * @return {@link Flux} of {@link SetResponse} holding the {@link KeyValue} pair to set along with the command
		 *         result.
		 */
		Flux<SetResponse> set(Publisher<KeyValue> values, Supplier<Expiration> expiration, Supplier<SetOption> option);

		/**
		 * Get multiple values in one batch.
		 * 
		 * @param keys must not be {@literal null}.
		 * @return
		 */
		default Mono<List<ByteBuffer>> mGet(List<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null!");
			return mGet(Mono.just(keys)).next().map(MGetResponse::getOutput);
		}

		/**
		 * <br />
		 * Get multiple values at in batches.
		 * 
		 * @param keys must not be {@literal null}.
		 * @return
		 */
		Flux<MGetResponse> mGet(Publisher<List<ByteBuffer>> keysets);

		static class SetResponse extends CommandResponse<KeyValue, Boolean> {

			public SetResponse(KeyValue input, Boolean output) {
				super(input, output);
			}

		}

		/**
		 * @author Christoph Strobl
		 * @since 2.0
		 */
		static class GetResponse extends CommandResponse<ByteBuffer, ByteBuffer> {

			public GetResponse(ByteBuffer input, ByteBuffer output) {
				super(input, output);
			}
		}

		/**
		 * @author Christoph Strobl
		 * @since 2.0
		 */
		static class GetSetResponse extends CommandResponse<KeyValue, ByteBuffer> {

			public GetSetResponse(KeyValue input, ByteBuffer output) {
				super(input, output);
			}
		}

		/**
		 * @author Christoph Strobl
		 * @since 2.0
		 */
		static class MGetResponse extends CommandResponse<List<ByteBuffer>, List<ByteBuffer>> {

			public MGetResponse(List<ByteBuffer> input, List<ByteBuffer> output) {
				super(input, output);
			}
		}

	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static interface ReactiveKeyCommands {

		/**
		 * Delete {@literal key}.
		 * 
		 * @param key must not be {@literal null}.
		 * @return
		 */
		default Mono<Long> del(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");
			return del(Mono.just(key)).next().map(DelResponse::getOutput);
		}

		/**
		 * Delete {@literal keys} one by one.
		 * 
		 * @param keys must not be {@literal null}.
		 * @return {@link Flux} of {@link DelResponse} holding the {@literal key} removed along with the deletion result.
		 */
		Flux<DelResponse> del(Publisher<ByteBuffer> keys);

		/**
		 * Delete multiple {@literal keys} one in one batch.
		 * 
		 * @param keys must not be {@literal null}.
		 * @return
		 */
		default Mono<Long> mDel(List<ByteBuffer> keys) {
			return mDel(Mono.just(keys)).next().map(MDelResponse::getOutput);
		}

		/**
		 * Delete multiple {@literal keys} in batches.
		 * 
		 * @param keys must not be {@literal null}.
		 * @return {@link Flux} of {@link MDelResponse} holding the {@literal keys} removed along with the deletion result.
		 */
		Flux<MDelResponse> mDel(Publisher<List<ByteBuffer>> keys);

		/**
		 * @author Christoph Strobl
		 * @since 2.0
		 */
		static class DelResponse extends CommandResponse<ByteBuffer, Long> {

			public DelResponse(ByteBuffer input, Long output) {
				super(input, output);
			}
		}

		/**
		 * @author Christoph Strobl
		 * @since 2.0
		 */
		static class MDelResponse extends CommandResponse<List<ByteBuffer>, Long> {

			public MDelResponse(List<ByteBuffer> input, Long output) {
				super(input, output);
			}
		}
	}
}

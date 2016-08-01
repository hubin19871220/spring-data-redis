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
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyValue;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;

import com.lambdaworks.redis.SetArgs;

import reactor.core.publisher.Flux;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class LettuceReactiveStringCommands implements ReactiveStringCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveStringCommands}.
	 * 
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveStringCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#mGet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MGetResponse> mGet(Publisher<List<ByteBuffer>> keyCollections) {

		return connection.execute(cmd -> {

			return Flux.from(keyCollections).flatMap((keys) -> {

				return LettuceReactiveRedisConnection.<MGetResponse> monoConverter().convert(
						cmd.mget(keys.stream().map(ByteBuffer::array).collect(Collectors.toList()).toArray(new byte[keys.size()][]))
								.map((value) -> value != null ? ByteBuffer.wrap(value) : ByteBuffer.allocate(0)).toList()
								.map((values) -> new MGetResponse(keys, values)));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#set(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<SetResponse> set(Publisher<KeyValue> values) {

		return connection.execute(cmd -> {
			return Flux.from(values).flatMap((kv) -> {
				return LettuceReactiveRedisConnection.<Boolean> monoConverter()
						.convert(cmd.set(kv.getKey().array(), kv.getValue().array()).map(LettuceConverters::stringToBoolean))
						.map((value) -> new SetResponse(kv, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#getSet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<GetSetResponse> getSet(Publisher<KeyValue> values) {

		return connection.execute(cmd -> {
			return Flux.from(values).flatMap((kv) -> {

				return LettuceReactiveRedisConnection.<GetSetResponse> monoConverter()
						.convert(cmd.getset(kv.getKey().array(), kv.getValue().array())
								.map((value) -> new GetSetResponse(kv, ByteBuffer.wrap(value)))
								.defaultIfEmpty(new GetSetResponse(kv, ByteBuffer.allocate(0))));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#set(org.reactivestreams.Publisher, java.util.function.Supplier, java.util.function.Supplier)
	 */
	@Override
	public Flux<SetResponse> set(Publisher<KeyValue> values, Supplier<Expiration> expiration,
			Supplier<SetOption> option) {

		return connection.execute(cmd -> {

			return Flux.from(values).flatMap((kv) -> {

				SetArgs args = LettuceConverters.toSetArgs(expiration.get(), option.get());

				return LettuceReactiveRedisConnection.<Boolean> monoConverter()
						.convert(cmd.set(kv.getKey().array(), kv.getValue().array(), args).map(LettuceConverters::stringToBoolean))
						.map((value) -> new SetResponse(kv, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#get(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<GetResponse> get(Publisher<ByteBuffer> keys) {

		return connection.execute(cmd -> {
			return Flux.from(keys).flatMap((key) -> {

				return LettuceReactiveRedisConnection.<ByteBuffer> monoConverter()
						.convert(cmd.get(key.array()).map(ByteBuffer::wrap)).map((value) -> new GetResponse(key, value))
						.defaultIfEmpty(new GetResponse(key, ByteBuffer.allocate(0)));
			});
		});
	}

}

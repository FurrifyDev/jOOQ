/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jooq.impl.vertx;

import java.util.ArrayList;
import java.util.List;

import org.reactivestreams.Publisher;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Result;
import io.vertx.sqlclient.Pool;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Implements the R2DBC {@link Batch} interface for jOOQ's
 * {@code BatchMultiple} — i.e. a sequence of distinct SQL strings executed
 * in order without shared parameters.
 *
 * <p>Each SQL string is executed as a simple (non-prepared) query against the
 * {@link Pool}.  Results are concatenated as a flat {@link Flux} of
 * {@link VertxResult}s.
 *
 * <p>Parameterised batches (same SQL, different bindings) are handled
 * directly by {@link VertxStatement#add()} / {@link VertxStatement#execute()}
 * via {@code executeBatch(List<Tuple>)}.
 */
final class VertxBatch implements Batch {

    private final Pool         pool;
    private final List<String> statements = new ArrayList<>();

    VertxBatch(Pool pool) {
        this.pool = pool;
    }

    @Override
    public Batch add(String sql) {
        if (sql == null)
            throw new IllegalArgumentException("sql must not be null");
        statements.add(sql);
        return this;
    }

    @Override
    public Publisher<? extends Result> execute() {
        if (statements.isEmpty())
            return Flux.empty();

        // Execute each statement sequentially and flatten results.
        return Flux.fromIterable(statements)
            .concatMap(sql ->
                Mono.fromCompletionStage(
                    pool.query(sql).execute().toCompletionStage()
                ).flatMapMany(rowSet -> Flux.just(new VertxResult(rowSet)))
            );
    }
}

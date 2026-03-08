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

import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Implements the R2DBC {@link Statement} interface backed by a Vert.x
 * {@link SqlClient} / {@link Pool}, with automatic read/write routing.
 *
 * <h2>Routing rules</h2>
 * <ol>
 *   <li>If a transaction connection is active ({@code txConn} is set), all
 *       statements execute on that connection — regardless of SQL kind.</li>
 *   <li>Otherwise {@link SqlRouter#isReadOnly(String)} inspects the leading
 *       keyword:
 *       <ul>
 *         <li>READ (SELECT / WITH / …) → pipelined {@code SqlClient}</li>
 *         <li>WRITE (INSERT / UPDATE / DELETE / …) → {@code Pool}</li>
 *       </ul>
 *   </li>
 * </ol>
 *
 * <h2>Batch support</h2>
 * Calling {@link #add()} after binding parameters appends the current binding
 * to the batch list and resets the builder. {@link #execute()} then calls
 * {@code executeBatch(List<Tuple>)} if there is more than one set of bindings.
 */
final class VertxStatement implements Statement {
    private final String sql;
    private final SqlClient sqlClient;
    private final Pool pool;
    private final AtomicReference<SqlConnection> txConn;

    /**
     * Current (accumulating) set of bind values.
     */
    private VertxTupleBuilder current = new VertxTupleBuilder();
    /**
     * Additional bind sets added via {@link #add()} for batch execution.
     */
    private final java.util.List<VertxTupleBuilder> batch = new java.util.ArrayList<>();

    /**
     * fetchSize hint — not used by Vert.x but stored for compliance.
     */
    private int fetchSize = 0;

    VertxStatement(
            String sql,
            SqlClient sqlClient,
            Pool pool,
            AtomicReference<SqlConnection> txConn
    ) {
        this.sql = sql;
        this.sqlClient = sqlClient;
        this.pool = pool;
        this.txConn = txConn;
    }

    // ------------------------------------------------------------------
    // Binding — jOOQ calls bind(index - 1, value) via DefaultBindContext
    // ------------------------------------------------------------------

    @Override
    public @NotNull Statement bind(int index, @NotNull Object value) {
        current.bind(index, value);
        return this;
    }

    @Override
    public @NotNull Statement bind(@NotNull String name, @NotNull Object value) {
        throw new UnsupportedOperationException("Named parameter binding is not supported; use positional ($1, $2, …)");
    }

    @Override
    public Statement bindNull(int index, Class<?> type) {
        current.bindNull(index, type);
        return this;
    }

    @Override
    public Statement bindNull(String name, Class<?> type) {
        throw new UnsupportedOperationException("Named parameter binding is not supported; use positional ($1, $2, …)");
    }

    // ------------------------------------------------------------------
    // Batch: add() seals the current bind set and starts a new one
    // ------------------------------------------------------------------

    @Override
    public @NotNull Statement add() {
        batch.add(current);
        current = new VertxTupleBuilder();
        return this;
    }

    // ------------------------------------------------------------------
    // Fetch size (ignored for Vert.x streaming; stored for compliance)
    // ------------------------------------------------------------------

    @Override
    public @NotNull Statement fetchSize(int rows) {
        this.fetchSize = rows;
        return this;
    }

    @Override
    public @NotNull Statement returnGeneratedValues(String @NotNull ... columns) {
        // Vert.x supports RETURNING clauses natively in the SQL string;
        // jOOQ will have already appended RETURNING to the SQL text.
        return this;
    }

    // ------------------------------------------------------------------
    // Execute
    // ------------------------------------------------------------------

    @Override
    public @NotNull Publisher<? extends Result> execute() {
        SqlClient target = resolveTarget();

        if (!batch.isEmpty()) {
            // Seal the last accumulating builder
            batch.add(current);
            java.util.List<io.vertx.sqlclient.Tuple> tuples = batch.stream()
                    .map(VertxTupleBuilder::build)
                    .toList();
            return Mono
                    .fromCompletionStage(
                            target.preparedQuery(sql).executeBatch(tuples).toCompletionStage()
                    )
                    .flatMapMany(firstRowSet -> {
                        // executeBatch returns a linked chain: rowSet.next() per batch entry.
                        // Eagerly collect all segments to avoid returning null from a
                        // state-generator (Flux.generate does not allow null states).
                        java.util.List<VertxResult> results = new java.util.ArrayList<>();
                        for (io.vertx.sqlclient.RowSet<io.vertx.sqlclient.Row> rs = firstRowSet;
                             rs != null;
                             rs = rs.next())
                            results.add(new VertxResult(rs));
                        return Flux.fromIterable(results);
                    });
        }

        io.vertx.sqlclient.Tuple params = current.build();
        return Mono
                .fromCompletionStage(
                        target.preparedQuery(sql).execute(params).toCompletionStage()
                )
                .flatMapMany(rowSet -> Flux.just(new VertxResult(rowSet)));
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /**
     * Selects the appropriate Vert.x client:
     * <ol>
     *   <li>Active transaction connection → always use it.</li>
     *   <li>Read-only SQL → pipelined {@code SqlClient}.</li>
     *   <li>Write SQL → {@code Pool}.</li>
     * </ol>
     */
    private SqlClient resolveTarget() {
        SqlConnection tx = txConn.get();
        if (tx != null)
            return tx;
        return SqlRouter.isReadOnly(sql) ? sqlClient : pool;
    }
}

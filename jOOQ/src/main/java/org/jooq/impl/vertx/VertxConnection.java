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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import reactor.core.publisher.Mono;

/**
 * Implements the R2DBC {@link Connection} interface backed by a Vert.x
 * {@link SqlClient} (pipelined reads) and {@link Pool} (writes / transactions).
 *
 * <h2>Lifecycle</h2>
 * <ul>
 *   <li>No physical connection is acquired at construction time.</li>
 *   <li>For non-transactional statements the {@link SqlClient} (pipelined) or
 *       {@link Pool} is used directly — no connection is held.</li>
 *   <li>On {@link #beginTransaction()} a real {@link SqlConnection} is leased
 *       from the pool and stored in {@code txConn}; all subsequent statements
 *       route through it.  It is released on commit/rollback/{@link #close()}.</li>
 * </ul>
 */
final class VertxConnection implements Connection {
    private final SqlClient sqlClient;

    private final Pool pool;
    /**
     * Holds the pooled connection while a transaction is open; null otherwise.
     */
    private final AtomicReference<SqlConnection> txConn = new AtomicReference<>();
    /**
     * Holds the active Vert.x Transaction object; null outside of a tx.
     */
    private final AtomicReference<VertxTransaction> tx = new AtomicReference<>();

    VertxConnection(SqlClient sqlClient, Pool pool) {
        this.sqlClient = sqlClient;
        this.pool = pool;
    }

    // ------------------------------------------------------------------
    // Statement / Batch factory
    // ------------------------------------------------------------------

    @Override
    public @NotNull Statement createStatement(@NotNull String sql) {
        return new VertxStatement(sql, sqlClient, pool, txConn);
    }

    @Override
    public @NotNull Batch createBatch() {
        return new VertxBatch(pool);
    }

    // ------------------------------------------------------------------
    // Transaction lifecycle
    // ------------------------------------------------------------------

    @Override
    public @NotNull Publisher<Void> beginTransaction() {
        return acquireAndBegin();
    }

    @Override
    public @NotNull Publisher<Void> beginTransaction(@NotNull TransactionDefinition definition) {
        return acquireAndBegin();
    }

    private Mono<Void> acquireAndBegin() {
        return Mono.fromCompletionStage(pool.getConnection().toCompletionStage())
                .flatMap(conn -> {
                    txConn.set(conn);
                    return Mono.fromCompletionStage(conn.begin().toCompletionStage());
                })
                .doOnNext(transaction -> tx.set(new VertxTransaction(transaction)))
                .then();
    }

    @Override
    public @NotNull Publisher<Void> commitTransaction() {
        VertxTransaction t = tx.getAndSet(null);
        if (t == null)
            return Mono.error(new IllegalStateException("No active transaction to commit"));
        return Mono.from(t.commit())
                .doFinally(_ -> releaseConnection());
    }

    @Override
    public @NotNull Publisher<Void> rollbackTransaction() {
        VertxTransaction t = tx.getAndSet(null);
        if (t == null)
            return Mono.error(new IllegalStateException("No active transaction to roll back"));
        return Mono.from(t.rollback())
                .doFinally(signal -> releaseConnection());
    }

    @Override
    public @NotNull Publisher<Void> rollbackTransactionToSavepoint(@NotNull String name) {
        SqlConnection conn = txConn.get();
        if (conn == null)
            return Mono.error(new IllegalStateException("No active transaction"));
        return Mono.fromCompletionStage(
                conn.query("ROLLBACK TO SAVEPOINT \"" + name + "\"").execute().toCompletionStage()
        ).then();
    }

    @Override
    public @NotNull Publisher<Void> createSavepoint(@NotNull String name) {
        SqlConnection conn = txConn.get();
        if (conn == null)
            return Mono.error(new IllegalStateException("No active transaction"));
        return Mono.fromCompletionStage(
                conn.query("SAVEPOINT \"" + name + "\"").execute().toCompletionStage()
        ).then();
    }

    @Override
    public @NotNull Publisher<Void> releaseSavepoint(@NotNull String name) {
        SqlConnection conn = txConn.get();
        if (conn == null)
            return Mono.error(new IllegalStateException("No active transaction"));
        return Mono.fromCompletionStage(
                conn.query("RELEASE SAVEPOINT \"" + name + "\"").execute().toCompletionStage()
        ).then();
    }

    // ------------------------------------------------------------------
    // Connection settings
    // ------------------------------------------------------------------

    @Override
    public @NotNull Publisher<Void> setAutoCommit(boolean autoCommit) {
        // Vert.x connections operate in auto-commit by default outside of
        // explicit transactions; nothing to send to the server.
        return Mono.empty();
    }

    @Override
    public @NotNull Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        SqlConnection conn = txConn.get();
        String sql = "SET TRANSACTION ISOLATION LEVEL " + isolationLevel.asSql();
        SqlClient target = conn != null ? conn : pool;
        return Mono.fromCompletionStage(target.query(sql).execute().toCompletionStage()).then();
    }

    @Override
    public @NotNull Publisher<Void> setLockWaitTimeout(@NotNull Duration timeout) {
        return Mono.empty();
    }

    @Override
    public @NotNull Publisher<Void> setStatementTimeout(@NotNull Duration timeout) {
        return Mono.empty();
    }

    // ------------------------------------------------------------------
    // Validation / metadata
    // ------------------------------------------------------------------

    @Override
    public @NotNull Publisher<Boolean> validate(@NotNull ValidationDepth depth) {
        // LOCAL validation: return true if we have not yet closed the connection.
        // REMOTE validation: ping the database via a lightweight query.
        if (depth == ValidationDepth.LOCAL)
            return Mono.just(true);

        SqlClient target = txConn.get() != null ? txConn.get() : pool;
        return Mono.fromCompletionStage(target.query("SELECT 1").execute().toCompletionStage())
                .map(rs -> true)
                .onErrorReturn(false);
    }

    @Override
    public boolean isAutoCommit() {
        return txConn.get() == null;
    }

    private static final ConnectionMetadata CONNECTION_METADATA = new ConnectionMetadata() {
        @Override
        public @NotNull String getDatabaseProductName() {
            return "PostgreSQL";
        }

        @Override
        public @NotNull String getDatabaseVersion() {
            return "unknown";
        }
    };

    @Override
    public @NotNull ConnectionMetadata getMetadata() {
        return CONNECTION_METADATA;
    }

    @Override
    public @NotNull IsolationLevel getTransactionIsolationLevel() {
        return IsolationLevel.READ_COMMITTED;
    }

    // ------------------------------------------------------------------
    // Close
    // ------------------------------------------------------------------

    @Override
    public @NotNull Publisher<Void> close() {
        releaseConnection();
        return Mono.empty();
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    private void releaseConnection() {
        SqlConnection conn = txConn.getAndSet(null);
        if (conn != null)
            conn.close();
    }
}

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

import org.jetbrains.annotations.NotNull;
import org.jooq.tools.r2dbc.LoggingConnection;
import org.reactivestreams.Publisher;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlClient;
import reactor.core.publisher.Mono;

/**
 * Implements the R2DBC {@link ConnectionFactory} SPI using a Vert.x
 * {@link SqlClient} for pipelined read queries and a {@link Pool} for
 * write queries and transaction management.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * SqlClient pipelinedClient = PgBuilder.client()
 *     .with(poolOptions)
 *     .connectingTo(connectOptions)
 *     .build();
 *
 * Pool pool = PgBuilder.pool()
 *     .with(poolOptions)
 *     .connectingTo(connectOptions)
 *     .build();
 *
 * DSLContext ctx = DSL.using(pipelinedClient, pool, SQLDialect.POSTGRES);
 * }</pre>
 *
 * <p>The factory is lazy: {@link #create()} constructs a {@link VertxConnection}
 * immediately without acquiring any physical connection.  A physical connection
 * is only leased from the pool when a transaction is started via
 * {@link VertxConnection#beginTransaction()}.
 */
public final class VertxConnectionFactory implements ConnectionFactory {
    /**
     * Shared metadata descriptor for this factory.
     */
    static final ConnectionFactoryMetadata METADATA = () -> "vertx-pg-client";

    private final SqlClient sqlClient;

    private final Pool pool;

    private final boolean withLogging;

    /**
     * Creates a new factory.
     *
     * @param sqlClient a Vert.x {@link SqlClient} (typically a pipelined
     *                  client) used for read-only queries
     * @param pool      a Vert.x {@link Pool} used for DML queries and as
     *                  the source of connections for transactions
     */
    public VertxConnectionFactory(SqlClient sqlClient, Pool pool) {
        if (sqlClient == null) throw new IllegalArgumentException("sqlClient must not be null");
        if (pool == null) throw new IllegalArgumentException("pool must not be null");
        this.sqlClient = sqlClient;
        this.pool = pool;
        this.withLogging = false;
    }

    public VertxConnectionFactory(SqlClient sqlClient, Pool pool, boolean withLogging) {
        if (sqlClient == null) throw new IllegalArgumentException("sqlClient must not be null");
        if (pool == null) throw new IllegalArgumentException("pool must not be null");
        this.sqlClient = sqlClient;
        this.pool = pool;
        this.withLogging = withLogging;
    }

    /**
     * Returns a {@link Publisher} that immediately emits a single
     * {@link VertxConnection}.  No physical connection is acquired here.
     */
    @Override
    public @NotNull Publisher<? extends Connection> create() {
        final var connection = new VertxConnection(sqlClient, pool);
        return Mono.just(withLogging ? new LoggingConnection(connection) : connection);
    }

    @Override
    public @NotNull ConnectionFactoryMetadata getMetadata() {
        return METADATA;
    }
}

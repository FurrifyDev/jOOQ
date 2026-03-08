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

import org.reactivestreams.Publisher;

import io.vertx.sqlclient.Transaction;
import reactor.core.publisher.Mono;

/**
 * Bridges a Vert.x {@link Transaction} to the Reactive Streams
 * {@link Publisher} model used by jOOQ's R2DBC transaction lifecycle.
 *
 * <p>This class is not part of the public API; it is used internally by
 * {@link VertxConnection} to commit and roll back transactions.
 */
final class VertxTransaction {
    private final Transaction transaction;

    VertxTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    /**
     * Commits the transaction and returns a {@link Publisher} that completes
     * when the commit has been acknowledged by the server.
     */
    Publisher<Void> commit() {
        return Mono.fromCompletionStage(transaction.commit().toCompletionStage());
    }

    /**
     * Rolls back the transaction and returns a {@link Publisher} that
     * completes when the rollback has been acknowledged by the server.
     */
    Publisher<Void> rollback() {
        return Mono.fromCompletionStage(transaction.rollback().toCompletionStage());
    }
}

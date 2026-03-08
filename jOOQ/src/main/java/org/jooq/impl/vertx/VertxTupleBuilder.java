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
import java.util.TreeMap;

import io.vertx.sqlclient.Tuple;

/**
 * Bridges jOOQ's incremental {@code bind(index, value)} API — as called by
 * {@link org.jooq.impl.DefaultBindContext} — to Vert.x's all-at-once
 * {@link Tuple} that is passed to {@code PreparedQuery.execute(Tuple)}.
 *
 * <p>Bindings are stored in a {@link TreeMap} keyed by 0-based column index
 * to guarantee correct ordering even if jOOQ emits them out of sequence.
 * {@link #build()} assembles the final {@link Tuple} from the sorted values.
 */
final class VertxTupleBuilder {
    /**
     * 0-based index → value (may be {@code null} for SQL NULLs).
     */
    private final TreeMap<Integer, Object> bindings = new TreeMap<>();

    /**
     * Bind a non-null value at the given 0-based parameter index.
     */
    void bind(int index, Object value) {
        bindings.put(index, value);
    }

    /**
     * Bind a SQL NULL at the given 0-based parameter index.
     * The type hint is retained as a {@link NullBinding} wrapper so that
     * callers that need the type can still inspect it.
     */
    void bindNull(int index, Class<?> type) {
        bindings.put(index, new NullBinding(type));
    }

    /**
     * Build the Vert.x {@link Tuple} from the accumulated bindings.
     *
     * <p>{@link NullBinding} wrappers are replaced with {@code null} in the
     * returned tuple.
     */
    Tuple build() {
        ArrayList<Object> values = new ArrayList<>(bindings.size());
        for (Object v : bindings.values())
            values.add(v instanceof NullBinding ? null : v);
        return Tuple.tuple(values);
    }

    /**
     * Returns {@code true} when no bindings have been registered.
     */
    boolean isEmpty() {
        return bindings.isEmpty();
    }

    /**
     * Sentinel that carries the {@link Class} hint for a SQL NULL binding.
     * Vert.x {@link Tuple} encodes nulls simply as {@code null}, so this
     * wrapper is unwrapped in {@link #build()}.
     */
    record NullBinding(Class<?> type) {
    }
}

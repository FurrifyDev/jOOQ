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

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;

import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Adapts a Vert.x {@link RowSet} to the R2DBC {@link Result} interface
 * consumed by jOOQ's internal {@code R2DBC.ResultSubscriber}.
 *
 * <p>The two paths jOOQ uses are:
 * <ul>
 *   <li>{@link #map(BiFunction)} — for SELECT queries: emits one mapped value
 *       per row via a {@link Flux}.</li>
 *   <li>{@link #getRowsUpdated()} — for DML queries: emits the affected-row
 *       count as a single {@link Long}.</li>
 * </ul>
 */
final class VertxResult implements Result {
    private final RowSet<io.vertx.sqlclient.Row> rowSet;

    VertxResult(RowSet<io.vertx.sqlclient.Row> rowSet) {
        this.rowSet = rowSet;
    }

    // ------------------------------------------------------------------
    // Core: SELECT path
    // ------------------------------------------------------------------

    @Override
    public <T> @NotNull Publisher<T> map(@NotNull BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
        List<ColumnDescriptor> descriptors = rowSet.columnDescriptors();
        VertxRowMetadata meta = new VertxRowMetadata(descriptors);

        return Flux.fromIterable(rowSet)
                .map(row -> mappingFunction.apply(new VertxRow(row, meta), meta));
    }

    // ------------------------------------------------------------------
    // Core: DML / update-count path
    // ------------------------------------------------------------------

    @Override
    public @NotNull Publisher<Long> getRowsUpdated() {
        return Mono.just((long) rowSet.rowCount());
    }

    // ------------------------------------------------------------------
    // Segment-based API (R2DBC 0.9 / 1.0): map(Function<Readable, T>)
    // ------------------------------------------------------------------

    @Override
    public <T> @NotNull Publisher<T> map(@NotNull Function<? super Readable, ? extends T> mappingFunction) {
        List<ColumnDescriptor> descriptors = rowSet.columnDescriptors();
        VertxRowMetadata meta = new VertxRowMetadata(descriptors);

        return Flux.fromIterable(rowSet)
                .map(row -> mappingFunction.apply(new VertxRow(row, meta)));
    }

    // ------------------------------------------------------------------
    // Segment-based API: filter / flatMap
    // ------------------------------------------------------------------

    @Override
    public @NotNull Result filter(@NotNull Predicate<Segment> filter) {
        // RowSet results are already materialised; filtering segments is a
        // no-op here — return a view that only emits segments the predicate accepts.
        return new FilteredVertxResult(this, filter);
    }

    @Override
    public <T> @NotNull Publisher<T> flatMap(@NotNull Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {

        List<ColumnDescriptor> descriptors = rowSet.columnDescriptors();
        VertxRowMetadata meta = new VertxRowMetadata(descriptors);

        return Flux.fromIterable(rowSet)
                .<Segment>map(row -> (RowSegment) () -> new VertxRow(row, meta))
                .flatMap(mappingFunction);
    }

    // ------------------------------------------------------------------
    // FilteredVertxResult
    // ------------------------------------------------------------------

    /**
     * A thin filtered view of a {@link VertxResult} that honours the
     * {@link Result#filter(Predicate)} contract.
     */
    private record FilteredVertxResult(VertxResult delegate, Predicate<Segment> filter) implements Result {
        @Override
        public <T> @NotNull Publisher<T> map(@NotNull BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
            List<ColumnDescriptor> descriptors = delegate.rowSet.columnDescriptors();
            VertxRowMetadata meta = new VertxRowMetadata(descriptors);

            return Flux.fromIterable(delegate.rowSet)
                    .<Segment>map(row -> (RowSegment) () -> new VertxRow(row, meta))
                    .filter(filter)
                    .map(seg -> mappingFunction.apply(((RowSegment) seg).row(), meta));
        }

        @Override
        public @NotNull Publisher<Long> getRowsUpdated() {
            Segment updateSeg = (UpdateCount) delegate.rowSet::rowCount;
            return filter.test(updateSeg)
                    ? Mono.just((long) delegate.rowSet.rowCount())
                    : Mono.empty();
        }

        @Override
        public <T> @NotNull Publisher<T> map(@NotNull Function<? super Readable, ? extends T> mappingFunction) {
            return delegate.map(mappingFunction);
        }

        @Override
        public @NotNull Result filter(@NotNull Predicate<Segment> filter) {
            return new FilteredVertxResult(delegate, this.filter.and(filter));
        }

        @Override
        public <T> @NotNull Publisher<T> flatMap(@NotNull Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
            return delegate.flatMap(mappingFunction);
        }
    }
}

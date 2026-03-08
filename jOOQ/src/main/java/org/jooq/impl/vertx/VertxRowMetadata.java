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
import java.util.NoSuchElementException;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import org.jetbrains.annotations.NotNull;

/**
 * Adapts a list of Vert.x {@link ColumnDescriptor}s — obtained from
 * {@code RowSet.columnDescriptors()} — to the R2DBC {@link RowMetadata}
 * interface consumed by jOOQ's internal record-hydration machinery.
 */
final class VertxRowMetadata implements RowMetadata {
    private final List<ColumnDescriptor> descriptors;
    private final List<VertxColumnMetadata> columnMetadatas;

    VertxRowMetadata(List<ColumnDescriptor> descriptors) {
        this.descriptors = descriptors;
        this.columnMetadatas = descriptors.stream()
                .map(VertxColumnMetadata::new)
                .toList();
    }

    @Override
    public @NotNull ColumnMetadata getColumnMetadata(int index) {
        if (index < 0 || index >= columnMetadatas.size())
            throw new IndexOutOfBoundsException("Column index " + index + " out of bounds for " + columnMetadatas.size() + " columns");
        return columnMetadatas.get(index);
    }

    @Override
    public @NotNull ColumnMetadata getColumnMetadata(@NotNull String columnName) {
        for (int i = 0; i < descriptors.size(); i++)
            if (descriptors.get(i).name().equalsIgnoreCase(columnName))
                return columnMetadatas.get(i);
        // R2DBC 1.0 specifies NoSuchElementException for unknown column names
        throw new NoSuchElementException("No column named '" + columnName + "' in result set");
    }

    @Override
    public @NotNull List<? extends ColumnMetadata> getColumnMetadatas() {
        return columnMetadatas;
    }

    /**
     * Returns the column names in result-set order. Not part of the R2DBC
     * {@link RowMetadata} contract but useful internally (e.g. by
     * {@link VertxRow#indexOfColumn}).
     */
    List<String> columnNames() {
        return descriptors.stream().map(ColumnDescriptor::name).toList();
    }

    @Override
    public boolean contains(@NotNull String columnName) {
        return descriptors.stream().anyMatch(d -> d.name().equalsIgnoreCase(columnName));
    }
}

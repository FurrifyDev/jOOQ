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

import java.sql.JDBCType;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Type;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import org.jetbrains.annotations.NotNull;

/**
 * Adapts a Vert.x {@link ColumnDescriptor} to the R2DBC {@link ColumnMetadata}
 * interface consumed by jOOQ's internal record-hydration machinery.
 *
 * <p>Only {@link #getName()} and {@link #getType()} are abstract in
 * {@code ReadableMetadata}; all other methods have default implementations
 * that return {@code null} / {@code UNKNOWN} and are overridden here for
 * completeness.
 */
final class VertxColumnMetadata implements ColumnMetadata {

    private final ColumnDescriptor descriptor;

    VertxColumnMetadata(ColumnDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    // ------------------------------------------------------------------
    // Required abstract methods
    // ------------------------------------------------------------------

    @Override
    public @NotNull String getName() {
        return descriptor.name();
    }

    /**
     * Returns an R2DBC {@link Type} built from the Vert.x
     * {@link ColumnDescriptor}.  The type's {@code getJavaType()} mirrors
     * {@link #getJavaType()} and its {@code getName()} returns the
     * vendor-specific type name (e.g. {@code "int4"}, {@code "text"}).
     */
    @Override
    public @NotNull Type getType() {
        Class<?> javaType = resolveJavaType(descriptor.jdbcType());
        String   typeName = descriptor.typeName() != null ? descriptor.typeName()
                          : descriptor.jdbcType()  != null ? descriptor.jdbcType().getName()
                          : "UNKNOWN";
        return new Type() {
            @Override public Class<?> getJavaType() { return javaType; }
            @Override public String   getName()     { return typeName; }
        };
    }

    // ------------------------------------------------------------------
    // Optional ReadableMetadata overrides (all have defaults; provided
    // for accuracy)
    // ------------------------------------------------------------------

    @Override
    public Class<?> getJavaType() {
        return resolveJavaType(descriptor.jdbcType());
    }

    /**
     * Returns the vendor-specific type name as the native type descriptor.
     */
    @Override
    public Object getNativeTypeMetadata() {
        return descriptor.typeName();
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /**
     * Maps a {@link JDBCType} to the most appropriate Java representation.
     */
    private static Class<?> resolveJavaType(JDBCType jdbcType) {
        if (jdbcType == null)
            return Object.class;

        return switch (jdbcType) {
            case BOOLEAN, BIT                      -> Boolean.class;
            case TINYINT                           -> Byte.class;
            case SMALLINT                          -> Short.class;
            case INTEGER                           -> Integer.class;
            case BIGINT                            -> Long.class;
            case REAL                              -> Float.class;
            case FLOAT, DOUBLE                     -> Double.class;
            case NUMERIC, DECIMAL                  -> java.math.BigDecimal.class;
            case CHAR, VARCHAR, LONGVARCHAR,
                 NCHAR, NVARCHAR, LONGNVARCHAR      -> String.class;
            case DATE                               -> java.time.LocalDate.class;
            case TIME, TIME_WITH_TIMEZONE           -> java.time.LocalTime.class;
            case TIMESTAMP                          -> java.time.LocalDateTime.class;
            case TIMESTAMP_WITH_TIMEZONE            -> java.time.OffsetDateTime.class;
            case BINARY, VARBINARY, LONGVARBINARY   -> byte[].class;
            default                                 -> Object.class;
        };
    }
}

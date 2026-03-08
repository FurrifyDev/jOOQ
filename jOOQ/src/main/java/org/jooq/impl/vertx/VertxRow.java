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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.UUID;

import io.r2dbc.spi.RowMetadata;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.data.Numeric;
import org.jetbrains.annotations.NotNull;

/**
 * Adapts a Vert.x {@link io.vertx.sqlclient.Row} to the R2DBC
 * {@link io.r2dbc.spi.Row} interface consumed by jOOQ's record-hydration
 * machinery ({@code R2DBCResultSet}).
 *
 * <p>Type coercion strategy:
 * <ol>
 *   <li>Delegate directly to {@code row.get(type, index)} when Vert.x supports
 *       the requested Java type natively.</li>
 *   <li>Fall back to reading the raw value and converting it where possible
 *       (e.g. {@link Buffer} → {@code byte[]}, {@link Numeric} →
 *       {@link BigDecimal}).</li>
 *   <li>Return the raw value unchanged for {@link Object} requests.</li>
 * </ol>
 */
final class VertxRow implements io.r2dbc.spi.Row {
    private final io.vertx.sqlclient.Row delegate;
    private final VertxRowMetadata metadata;

    VertxRow(io.vertx.sqlclient.Row delegate, VertxRowMetadata metadata) {
        this.delegate = delegate;
        this.metadata = metadata;
    }

    @Override
    public @NotNull RowMetadata getMetadata() {
        return metadata;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(int index, @NotNull Class<T> type) {
        Object raw = delegate.getValue(index);
        if (raw == null)
            return null;

        return (T) coerce(raw, type, index);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(@NotNull String name, @NotNull Class<T> type) {
        Object raw = delegate.getValue(name);
        if (raw == null)
            return null;

        int index = indexOfColumn(name);
        return (T) coerce(raw, type, index);
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    private int indexOfColumn(String name) {
        var names = metadata.columnNames();
        int i = 0;
        for (String n : names) {
            if (n.equalsIgnoreCase(name)) return i;
            i++;
        }
        return -1;
    }

    /**
     * Coerces the Vert.x raw value to the type requested by jOOQ.
     * The raw value is whatever Vert.x decoded from the wire protocol.
     */
    private Object coerce(Object raw, Class<?> type, int index) {
        // Fast path: already the exact type
        if (type.isInstance(raw))
            return raw;

        // Object → return as-is, let jOOQ's converters handle it
        if (type == Object.class)
            return raw;

        // Numeric types
        if (type == Boolean.class || type == boolean.class)
            return toBoolean(raw);
        if (type == Byte.class || type == byte.class)
            return toByte(raw);
        if (type == Short.class || type == short.class)
            return toShort(raw);
        if (type == Integer.class || type == int.class)
            return toInt(raw);
        if (type == Long.class || type == long.class)
            return toLong(raw);
        if (type == Float.class || type == float.class)
            return toFloat(raw);
        if (type == Double.class || type == double.class)
            return toDouble(raw);
        if (type == BigDecimal.class)
            return toBigDecimal(raw);

        // Strings
        if (type == String.class)
            return raw.toString();

        // UUID
        if (type == UUID.class) {
            if (raw instanceof UUID u) return u;
            return UUID.fromString(raw.toString());
        }

        // Temporal types
        if (type == LocalDate.class)
            return delegate.getLocalDate(index);
        if (type == LocalTime.class)
            return delegate.getLocalTime(index);
        if (type == LocalDateTime.class)
            return delegate.getLocalDateTime(index);
        if (type == OffsetDateTime.class)
            return delegate.getOffsetDateTime(index);
        if (type == OffsetTime.class)
            return delegate.getOffsetTime(index);

        // Binary: Vert.x returns Buffer for BYTEA
        if (type == byte[].class) {
            if (raw instanceof Buffer b) return b.getBytes();
            if (raw instanceof byte[] ba) return ba;
        }

        // JSON
        if (type == JsonObject.class && raw instanceof JsonObject jo)
            return jo;
        if (type == JsonArray.class && raw instanceof JsonArray ja)
            return ja;

        // Numeric (PostgreSQL NUMERIC)
        if (type == Numeric.class && raw instanceof Numeric n)
            return n;

        // Arrays: delegate to Vert.x driver which returns Object[]
        if (type.isArray())
            return delegate.get(type, index);

        // Last resort: ask Vert.x driver to perform the conversion
        return delegate.get(type, index);
    }

    // ------------------------------------------------------------------
    // Numeric conversion helpers
    // ------------------------------------------------------------------

    private static Boolean toBoolean(Object v) {
        if (v instanceof Boolean b) return b;
        if (v instanceof Number n) return n.intValue() != 0;
        if (v instanceof String s) return Boolean.parseBoolean(s);
        throw conversionError(v, Boolean.class);
    }

    private static Byte toByte(Object v) {
        if (v instanceof Number n) return n.byteValue();
        throw conversionError(v, Byte.class);
    }

    private static Short toShort(Object v) {
        if (v instanceof Number n) return n.shortValue();
        throw conversionError(v, Short.class);
    }

    private static Integer toInt(Object v) {
        if (v instanceof Number n) return n.intValue();
        throw conversionError(v, Integer.class);
    }

    private static Long toLong(Object v) {
        if (v instanceof Number n) return n.longValue();
        throw conversionError(v, Long.class);
    }

    private static Float toFloat(Object v) {
        if (v instanceof Number n) return n.floatValue();
        throw conversionError(v, Float.class);
    }

    private static Double toDouble(Object v) {
        if (v instanceof Number n) return n.doubleValue();
        throw conversionError(v, Double.class);
    }

    private static BigDecimal toBigDecimal(Object v) {
        if (v instanceof BigDecimal bd) return bd;
        if (v instanceof Numeric n) return n.bigDecimalValue();
        if (v instanceof Number n) return new BigDecimal(n.toString());
        if (v instanceof String s) return new BigDecimal(s);
        throw conversionError(v, BigDecimal.class);
    }

    private static IllegalArgumentException conversionError(Object value, Class<?> target) {
        return new IllegalArgumentException(
                "Cannot convert " + value.getClass().getName() + " to " + target.getName());
    }
}

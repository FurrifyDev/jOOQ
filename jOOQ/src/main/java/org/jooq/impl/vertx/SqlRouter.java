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

/**
 * Stateless utility that inspects the leading SQL keyword to determine whether
 * a query should be routed to the pipelined {@code SqlClient} (reads) or the
 * connection {@code Pool} (writes / DML).
 */
final class SqlRouter {

    private SqlRouter() {}

    /**
     * Returns {@code true} when the SQL string represents a read-only
     * operation that is safe to execute over a pipelined {@code SqlClient}.
     *
     * <p>Recognised read prefixes: {@code SELECT}, {@code WITH} (CTEs that
     * project rows), {@code TABLE}, {@code VALUES}.
     *
     * <p>Everything else — INSERT, UPDATE, DELETE, MERGE, CALL, … — is
     * considered a write and will be routed to the {@code Pool}.
     */
    static boolean isReadOnly(String sql) {
        if (sql == null || sql.isEmpty())
            return false;

        String t = sql.stripLeading().toUpperCase();
        return t.startsWith("SELECT")
            || t.startsWith("WITH")
            || t.startsWith("TABLE")
            || t.startsWith("VALUES");
    }
}

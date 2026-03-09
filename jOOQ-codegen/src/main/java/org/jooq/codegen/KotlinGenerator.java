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
 *
 * Other licenses:
 * -----------------------------------------------------------------------------
 * Commercial licenses for this work are available. These replace the above
 * Apache-2.0 license and offer limited warranties, support, maintenance, and
 * commercial database integrations.
 *
 * For more information, please visit: https://www.jooq.org/legal/licensing
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */
package org.jooq.codegen;

import static org.jooq.codegen.Language.KOTLIN;

/**
 * A {@link JavaGenerator} variant that targets Kotlin source files with full Kotlin null-safety.
 * <p>
 * In addition to setting the output language to Kotlin, this generator enables proper nullability
 * annotations on all generated constructs by default:
 * <ul>
 *   <li>{@code generateKotlinNotNullPojoAttributes} — POJO data class properties reflect DB NOT NULL</li>
 *   <li>{@code generateKotlinNotNullRecordAttributes} — Record properties reflect DB NOT NULL</li>
 *   <li>{@code generateKotlinNotNullInterfaceAttributes} — Interface properties reflect DB NOT NULL</li>
 *   <li>{@code generateKotlinNotNullTableAttributes} — {@code TableField<R, T>} type param reflects DB NOT NULL,
 *       so columns declared {@code NOT NULL} produce {@code TableField<R, T>} instead of {@code TableField<R, T?>}</li>
 *   <li>{@code generateKotlinNotNullArrayElements} — array element types are non-nullable by default
 *       (e.g. {@code Array<String>} instead of {@code Array<String?>}), since databases do not store
 *       per-element nullability in schema metadata</li>
 * </ul>
 * <p>
 * All flags can still be overridden individually via the jOOQ configuration {@code <generate>} block.
 *
 * @author Lukas Eder
 */
public class KotlinGenerator extends JavaGenerator {

    public KotlinGenerator() {
        super(KOTLIN);

        // Enable DB-accurate Kotlin null-safety on all generated constructs by default.
        // Individual flags can be overridden via the jOOQ <generate> configuration.
        this.generateKotlinNotNullPojoAttributes      = true;
        this.generateKotlinNotNullRecordAttributes    = true;
        this.generateKotlinNotNullInterfaceAttributes = true;
        this.generateKotlinNotNullTableAttributes     = true;
        this.generateKotlinNotNullArrayElements       = true;
    }
}

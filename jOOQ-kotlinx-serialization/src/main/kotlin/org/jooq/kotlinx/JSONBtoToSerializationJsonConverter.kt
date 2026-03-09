package org.jooq.kotlinx

import kotlinx.serialization.KSerializer
import kotlinx.serialization.modules.SerializersModule
import org.jooq.JSONB
import kotlin.reflect.KClass

class JSONBtoToSerializationJsonConverter<U : Any>(
    toType: KClass<U>,
    serializer: KSerializer<U>,
    serializers: SerializersModule = SerializersModule {},
) : AbstractToSerializationJsonConverter<JSONB, U>(JSONB::class, toType, serializer, serializers) {
    override fun data(json: JSONB?): String? = json?.data()

    override fun json(string: String?): JSONB? = JSONB.jsonbOrNull(string)
}

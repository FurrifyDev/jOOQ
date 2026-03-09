package org.jooq.kotlinx

import kotlinx.serialization.KSerializer
import kotlinx.serialization.modules.SerializersModule
import org.jooq.JSON
import kotlin.reflect.KClass

class JSONtoToSerializationJsonConverter<U : Any>(
    toType: KClass<U>,
    serializer: KSerializer<U>,
    serializers: SerializersModule = SerializersModule {},
) : AbstractToSerializationJsonConverter<JSON, U>(JSON::class, toType, serializer, serializers) {
    override fun data(json: JSON?): String? = json?.data()

    override fun json(string: String?): JSON? = JSON.jsonOrNull(string)
}

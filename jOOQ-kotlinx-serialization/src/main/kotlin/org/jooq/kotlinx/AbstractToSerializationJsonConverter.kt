package org.jooq.kotlinx

import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import org.jooq.exception.DataTypeException
import org.jooq.impl.AbstractConverter
import kotlin.reflect.KClass

abstract class AbstractToSerializationJsonConverter<J : Any, U : Any>(
    fromType: KClass<J>,
    toType: KClass<U>,
    private val serializer: KSerializer<U>,
    serializers: SerializersModule,
) : AbstractConverter<J, U>(fromType.java, toType.java) {

    private val json: Json = Json {
        encodeDefaults = true
        isLenient = true
        allowSpecialFloatingPointValues = true
        allowStructuredMapKeys = true
        prettyPrint = false
        useArrayPolymorphism = false
        ignoreUnknownKeys = true
        serializersModule = serializers
    }

    abstract fun data(json: J?): String?

    abstract fun json(string: String?): J?

    override fun from(databaseObject: J?): U? {
        if (databaseObject == null) return null

        return try {
            val str = data(databaseObject) ?: return null
            json.decodeFromString(serializer, str)
        } catch (e: SerializationException) {
            throw DataTypeException("Error when converting object of type " + toType() + " to JSON", e)
        }
    }

    override fun to(userObject: U?): J? {
        if (userObject == null) return null

        return try {
            json(json.encodeToString(serializer, userObject))
        } catch (e: SerializationException) {
            throw DataTypeException("Error when converting object of type " + toType() + " to JSON", e)
        }
    }
}

package org.jooq.kotlin.coroutines

import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.mono
import org.jooq.Configuration
import org.jooq.DSLContext
import org.jooq.Isolation
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

// ----------------------------------------------------------------------------
// Extensions to bridge between the reactive-streams and the coroutine world
// ----------------------------------------------------------------------------

suspend fun <T> DSLContext.transactionCoroutine(transactional: suspend (Configuration) -> T): T =
    transactionCoroutine(Isolation.READ_COMMITTED, EmptyCoroutineContext, transactional)


suspend fun <T> DSLContext.transactionCoroutine(
    isolationLevel: Isolation,
    context: CoroutineContext,
    transactional: suspend (Configuration) -> T
): T {
    // [#14997] Wrap values in an auxiliary class in order to allow nulls, which awaitSingle()
    //          doesn't allow, otherwise
    data class Wrap<T>(val t: T)

    return transactionPublisher(
        { c -> mono(context) { Wrap(transactional.invoke(c)) } },
        isolationLevel
    ).awaitSingle().t
}

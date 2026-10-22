package ru.quipy.payments.logic

import java.time.Clock
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.ceil
import kotlin.math.floor

class PaymentLoadManager(
    properties: PaymentAccountProperties,
    processingP99: Duration,
    workerCount: Int,
    private val clock: Clock = Clock.systemUTC()
) {
    private companion object {
        const val MILLIS_PER_SECOND = 1000.0
    }

    private val lock = Any()
    private var pending = 0
    private var processing = 0
    private val processingBudgetMillis = processingP99.toMillis().toDouble()
    private val throughput = minOf(
        properties.rateLimitPerSec.toDouble(),
        minOf(properties.parallelRequests, workerCount) * MILLIS_PER_SECOND / processingBudgetMillis
    )
    val retryAfter: Duration = Duration.ofMillis(ceil(MILLIS_PER_SECOND / throughput).toLong())

    fun tryReserve(deadline: Long): PaymentReservation? = synchronized(lock) {
        val waiting = pending - processing
        val remainingMillis = deadline.toDouble() - clock.millis()
        val maxWaitingPayments = floor(
            throughput * (remainingMillis - processingBudgetMillis) / MILLIS_PER_SECOND
        ).toInt()
        if (waiting >= maxWaitingPayments) {
            return@synchronized null
        }
        pending++
        val estimatedMillis = waiting * MILLIS_PER_SECOND / throughput + processingBudgetMillis
        PaymentReservation(estimatedMillis, retryAfter) {
            synchronized(lock) {
                pending--
            }
        }
    }

    fun <T> processing(action: () -> T): T {
        synchronized(lock) {
            processing++
        }
        try {
            return action()
        } finally {
            synchronized(lock) {
                processing--
            }
        }
    }
}

class PaymentReservation(
    val estimatedMillis: Double,
    val retryAfter: Duration,
    private val release: () -> Unit
) : AutoCloseable {
    private val released = AtomicBoolean()

    override fun close() {
        if (released.compareAndSet(false, true)) {
            release()
        }
    }
}
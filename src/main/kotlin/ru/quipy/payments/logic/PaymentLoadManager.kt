package ru.quipy.payments.logic

import java.time.Clock
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.ceil

class PaymentLoadManager(
    properties: PaymentAccountProperties,
    private val deadlineReserveTime: Duration,
    workerCount: Int,
    private val clock: Clock = Clock.systemUTC()
) {
    private companion object {
        const val MILLIS_PER_SECOND = 1000.0
    }

    private val lock = Any()
    private var pending = 0
    private var processing = 0
    private val processingMillis = properties.averageProcessingTime.toMillis()
    private val throughput = minOf(
        properties.rateLimitPerSec.toDouble(),
        minOf(properties.parallelRequests, workerCount) * MILLIS_PER_SECOND / processingMillis
    )
    val retryAfter: Duration = Duration.ofMillis(ceil(MILLIS_PER_SECOND / throughput).toLong())

    fun tryReserve(deadline: Long): PaymentReservation? = synchronized(lock) {
        val waiting = pending - processing
        val estimatedMillis = waiting * MILLIS_PER_SECOND / throughput + processingMillis
        if (estimatedMillis + deadlineReserveTime.toMillis() > deadline.toDouble() - clock.millis()) {
            return@synchronized null
        }
        pending++
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
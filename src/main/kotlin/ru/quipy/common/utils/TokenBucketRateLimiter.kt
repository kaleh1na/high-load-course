package ru.quipy.common.utils

import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.math.ceil

class TokenBucketRateLimiter(
    rate: Int,
    private val bucketMaxCapacity: Int,
    window: Long,
    timeUnit: TimeUnit = TimeUnit.MINUTES,
    private val nanoTime: () -> Long = System::nanoTime,
) : RateLimiter {
    init {
        require(rate > 0 && bucketMaxCapacity > 0 && window > 0)
    }

    private val nanosPerToken = timeUnit.toNanos(window).toDouble() / rate
    private var tokens = bucketMaxCapacity.toDouble()
    private var lastUpdate = nanoTime()

    @Synchronized
    fun tryAcquire(capacity: Int = bucketMaxCapacity): Duration? {
        require(capacity > 0)
        val now = nanoTime()
        tokens = minOf(capacity.toDouble(), tokens + (now - lastUpdate) / nanosPerToken)
        lastUpdate = now
        if (tokens < 1.0) {
            val waitMillis = ceil((1.0 - tokens) * nanosPerToken / 1_000_000).toLong()
            return Duration.ofMillis(waitMillis)
        }
        tokens -= 1.0
        return null
    }

    override fun tick() = tryAcquire() == null
}

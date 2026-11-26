package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque

class SlidingWindowRateLimiter(
    private val rate: Long,
    window: Duration,
) : RateLimiter {
    private val windowNanos = window.toNanos()
    private val timestamps = ArrayDeque<Long>()

    init {
        require(rate > 0)
        require(windowNanos > 0)
    }

    @Synchronized
    override fun tick(): Boolean {
        val now = System.nanoTime()
        while (timestamps.isNotEmpty() && now - timestamps.first >= windowNanos) {
            timestamps.removeFirst()
        }
        if (timestamps.size.toLong() >= rate) {
            return false
        }
        timestamps.addLast(now)
        return true
    }

    fun tickBlocking(canWait: () -> Boolean = { true }): Boolean {
        while (canWait()) {
            if (tick()) return true
            Thread.sleep(10)
        }
        return false
    }
}
package ru.quipy.payments.logic

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.test.util.ReflectionTestUtils
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class OrderPayerCapacityTest {
    @Test
    fun `busy executor rejects immediately without queueing and accepts after workers finish`() {
        val payer = OrderPayer()
        val executor = ReflectionTestUtils.getField(payer, "paymentExecutor") as ThreadPoolExecutor
        val started = CountDownLatch(executor.maximumPoolSize)
        val release = CountDownLatch(1)
        try {
            repeat(executor.maximumPoolSize) {
                executor.execute {
                    started.countDown()
                    release.await()
                }
            }
            assertTrue(started.await(5, TimeUnit.SECONDS))
            assertTimeoutPreemptively(Duration.ofSeconds(1)) {
                assertThrows(RejectedExecutionException::class.java) {
                    payer.processPayment(UUID.randomUUID(), 100, UUID.randomUUID(), Long.MAX_VALUE)
                }
            }
            assertEquals(0, executor.queue.size)
            release.countDown()

            val accepted = CountDownLatch(1)
            assertTimeoutPreemptively(Duration.ofSeconds(5)) {
                while (true) {
                    try {
                        executor.execute { accepted.countDown() }
                        break
                    } catch (e: RejectedExecutionException) {
                        Thread.sleep(1)
                    }
                }
                assertTrue(accepted.await(1, TimeUnit.SECONDS))
            }
        } finally {
            release.countDown()
            executor.shutdownNow()
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS))
        }
    }
}
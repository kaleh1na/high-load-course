package ru.quipy.payments.logic

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class PaymentLoadManagerTest {
    private val clock = Clock.fixed(Instant.ofEpochMilli(100_000), ZoneOffset.UTC)
    private val account = PaymentAccountProperties("test", "acc-5", 5, 3, 30, Duration.ofMillis(4900), true)

    @Test
    fun `admission uses remaining deadline and configured margin and release is idempotent`() {
        val paymentLoadManager = PaymentLoadManager(account, Duration.ofSeconds(10), 16, clock)
        assertNull(paymentLoadManager.tryReserve(114_899))
        val reservation = paymentLoadManager.tryReserve(114_900)
        assertNotNull(reservation)
        assertNull(paymentLoadManager.tryReserve(114_900))
        reservation!!.close()
        reservation.close()
        val next = paymentLoadManager.tryReserve(114_900)
        assertNotNull(next)
        assertNull(paymentLoadManager.tryReserve(114_900))
        next!!.close()
        val withoutMargin = PaymentLoadManager(account, Duration.ZERO, 16, clock)
        withoutMargin.tryReserve(104_900)!!.close()
    }

    @Test
    fun `simultaneous requests cannot overbook the deadline budget`() {
        val paymentLoadManager = PaymentLoadManager(account, Duration.ofSeconds(10), 16, clock)
        val callers = Executors.newFixedThreadPool(8)
        try {
            val reservations = callers.invokeAll((1..100).map {
                Callable { paymentLoadManager.tryReserve(160_000) }
            }).mapNotNull { it.get() }
            assertEquals(47, reservations.size)
            reservations.forEach { it.close() }
            paymentLoadManager.tryReserve(114_900)!!.close()
        } finally {
            callers.shutdownNow()
        }
    }

    @Test
    fun `provider processing is excluded from waiting count and exception restores it`() {
        val paymentLoadManager = PaymentLoadManager(account, Duration.ofSeconds(10), 16, clock)
        val first = paymentLoadManager.tryReserve(160_000)!!
        val entered = CountDownLatch(1)
        val finish = CountDownLatch(1)
        val worker = Executors.newSingleThreadExecutor()
        try {
            val task = worker.submit {
                assertThrows(IllegalStateException::class.java) {
                    paymentLoadManager.processing {
                        entered.countDown()
                        finish.await()
                        throw IllegalStateException("Provider failed")
                    }
                }
            }
            assertTrue(entered.await(5, TimeUnit.SECONDS))
            val next = paymentLoadManager.tryReserve(114_900)
            assertNotNull(next)
            assertNull(paymentLoadManager.tryReserve(114_900))
            next!!.close()
            finish.countDown()
            task.get(5, TimeUnit.SECONDS)
            assertNull(paymentLoadManager.tryReserve(114_900))
            first.close()
            paymentLoadManager.tryReserve(114_900)!!.close()
        } finally {
            finish.countDown()
            first.close()
            worker.shutdownNow()
        }
    }

    @Test
    fun `estimation respects rate limit and worker count`() {
        val slowRate = PaymentLoadManager(
            account.copy(rateLimitPerSec = 1, averageProcessingTime = Duration.ofMillis(100)),
            Duration.ZERO, 16, clock
        )
        val first = slowRate.tryReserve(100_100)!!
        assertNull(slowRate.tryReserve(101_099))
        slowRate.tryReserve(101_100)!!.close()
        first.close()

        val workerLimited = PaymentLoadManager(
            account.copy(parallelRequests = 30, rateLimitPerSec = 100),
            Duration.ZERO, 4, clock
        )
        val pending = workerLimited.tryReserve(104_900)!!
        assertNull(workerLimited.tryReserve(106_124))
        workerLimited.tryReserve(106_125)!!.close()
        pending.close()
    }

    @Test
    fun `partial and combined account reservations are released on rejection`() {
        val first = mock(PaymentExternalSystemAdapter::class.java)
        val second = mock(PaymentExternalSystemAdapter::class.java)
        val releases = AtomicInteger()
        `when`(first.tryReservePayment(anyLong())).thenAnswer {
            PaymentReservation(40_000.0) { releases.incrementAndGet() }
        }
        val service = PaymentSystemImpl(listOf(first, second), Duration.ofSeconds(10))
        assertThrows(RejectedExecutionException::class.java) {
            service.reservePayment(System.currentTimeMillis() + 60_000)
        }
        assertEquals(1, releases.get())
        `when`(second.tryReservePayment(anyLong())).thenAnswer {
            PaymentReservation(40_000.0) { releases.incrementAndGet() }
        }
        assertThrows(RejectedExecutionException::class.java) {
            service.reservePayment(System.currentTimeMillis() + 60_000)
        }
        assertEquals(3, releases.get())
        val reservation = service.reservePayment(System.currentTimeMillis() + 120_000)
        reservation.close()
        reservation.close()
        assertEquals(5, releases.get())
    }

    @Test
    fun `fixed safety margin is applied once for sequential accounts`() {
        val windows = List(2) {
            PaymentLoadManager(
                account.copy(averageProcessingTime = Duration.ofSeconds(1)),
                Duration.ofSeconds(10), 16
            )
        }
        val adapters = windows.map { paymentLoadManager ->
            mock(PaymentExternalSystemAdapter::class.java).also { adapter ->
                `when`(adapter.tryReservePayment(anyLong())).thenAnswer { invocation ->
                    paymentLoadManager.tryReserve(invocation.getArgument(0))
                }
            }
        }
        val service = PaymentSystemImpl(adapters, Duration.ofSeconds(10))
        service.reservePayment(System.currentTimeMillis() + 13_000).close()
        assertThrows(RejectedExecutionException::class.java) {
            service.reservePayment(System.currentTimeMillis() + 11_500)
        }
    }
}
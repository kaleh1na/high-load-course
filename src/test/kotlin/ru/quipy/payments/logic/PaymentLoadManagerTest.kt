package ru.quipy.payments.logic

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.doAnswer
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
    fun `admission uses remaining deadline and configured divisor and release is idempotent`() {
        val paymentLoadManager = PaymentLoadManager(account, 3, 16, clock)
        assertEquals(Duration.ofMillis(980), paymentLoadManager.retryAfter)
        assertNull(paymentLoadManager.tryReserve(107_349))
        val reservation = paymentLoadManager.tryReserve(107_350)
        assertNotNull(reservation)
        assertNull(paymentLoadManager.tryReserve(107_350))
        reservation!!.close()
        reservation.close()
        val next = paymentLoadManager.tryReserve(107_350)
        assertNotNull(next)
        assertNull(paymentLoadManager.tryReserve(107_350))
        next!!.close()
        val largerReserve = PaymentLoadManager(account, 2, 16, clock)
        assertNull(largerReserve.tryReserve(109_799))
        largerReserve.tryReserve(109_800)!!.close()
        val shortDeadline = PaymentLoadManager(
            account.copy(parallelRequests = 10_000, rateLimitPerSec = 110, averageProcessingTime = Duration.ofSeconds(1)),
            3, 16, clock
        )
        val reservations = (1..17).map { shortDeadline.tryReserve(103_000)!! }
        assertNull(shortDeadline.tryReserve(103_000))
        reservations.forEach { it.close() }
    }

    @Test
    fun `simultaneous requests cannot overbook the deadline budget`() {
        val paymentLoadManager = PaymentLoadManager(account, 3, 16, clock)
        val callers = Executors.newFixedThreadPool(8)
        try {
            val reservations = callers.invokeAll((1..100).map {
                Callable { paymentLoadManager.tryReserve(160_000) }
            }).mapNotNull { it.get() }
            assertEquals(36, reservations.size)
            reservations.forEach { it.close() }
            paymentLoadManager.tryReserve(107_350)!!.close()
        } finally {
            callers.shutdownNow()
        }
    }

    @Test
    fun `provider processing is excluded from waiting count and exception restores it`() {
        val paymentLoadManager = PaymentLoadManager(account, 3, 16, clock)
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
            val next = paymentLoadManager.tryReserve(107_350)
            assertNotNull(next)
            assertNull(paymentLoadManager.tryReserve(107_350))
            next!!.close()
            finish.countDown()
            task.get(5, TimeUnit.SECONDS)
            assertNull(paymentLoadManager.tryReserve(107_350))
            first.close()
            paymentLoadManager.tryReserve(107_350)!!.close()
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
            3, 16, clock
        )
        assertEquals(Duration.ofSeconds(1), slowRate.retryAfter)
        val first = slowRate.tryReserve(100_150)!!
        assertNull(slowRate.tryReserve(101_649))
        slowRate.tryReserve(101_650)!!.close()
        first.close()

        val workerLimited = PaymentLoadManager(
            account.copy(parallelRequests = 30, rateLimitPerSec = 100),
            3, 4, clock
        )
        assertEquals(Duration.ofMillis(1225), workerLimited.retryAfter)
        val pending = workerLimited.tryReserve(107_350)!!
        assertNull(workerLimited.tryReserve(109_187))
        workerLimited.tryReserve(109_188)!!.close()
        pending.close()
        val fractionalRate = PaymentLoadManager(
            account.copy(rateLimitPerSec = 3, averageProcessingTime = Duration.ofMillis(100)),
            3, 16, clock
        )
        assertEquals(Duration.ofMillis(334), fractionalRate.retryAfter)
    }

    @Test
    fun `partial and combined account reservations are released on rejection`() {
        val first = mock(PaymentExternalSystemAdapter::class.java)
        val second = mock(PaymentExternalSystemAdapter::class.java)
        val releases = AtomicInteger()
        `when`(first.tryReservePayment(anyLong())).thenAnswer {
            PaymentReservation(40_000.0, Duration.ofMillis(980)) { releases.incrementAndGet() }
        }
        `when`(second.tryReservePayment(anyLong()))
            .thenThrow(PaymentRejectedException(Duration.ofSeconds(2), "Deadline"))
        val service = PaymentSystemImpl(listOf(first, second), 3)
        val accountRejection = assertThrows(PaymentRejectedException::class.java) {
            service.reservePayment(System.currentTimeMillis() + 60_000)
        }
        assertEquals(Duration.ofSeconds(2), accountRejection.retryAfter)
        assertEquals(1, releases.get())
        doAnswer {
            PaymentReservation(40_000.0, Duration.ofSeconds(2)) { releases.incrementAndGet() }
        }.`when`(second).tryReservePayment(anyLong())
        val combinedRejection = assertThrows(PaymentRejectedException::class.java) {
            service.reservePayment(System.currentTimeMillis() + 60_000)
        }
        assertEquals(Duration.ofSeconds(2), combinedRejection.retryAfter)
        assertEquals(3, releases.get())
        val reservation = service.reservePayment(System.currentTimeMillis() + 150_000)
        assertEquals(Duration.ofSeconds(2), reservation.retryAfter)
        reservation.close()
        reservation.close()
        assertEquals(5, releases.get())
    }

    @Test
    fun `fractional deadline reserve is applied once for sequential accounts`() {
        val windows = List(2) {
            PaymentLoadManager(
                account.copy(averageProcessingTime = Duration.ofSeconds(1)),
                3, 16
            )
        }
        val adapters = windows.map { paymentLoadManager ->
            mock(PaymentExternalSystemAdapter::class.java).also { adapter ->
                `when`(adapter.tryReservePayment(anyLong())).thenAnswer { invocation ->
                    paymentLoadManager.tryReserve(invocation.getArgument(0))
                        ?: throw PaymentRejectedException(paymentLoadManager.retryAfter, "Deadline")
                }
            }
        }
        val service = PaymentSystemImpl(adapters, 3)
        service.reservePayment(System.currentTimeMillis() + 4_000).close()
        assertThrows(RejectedExecutionException::class.java) {
            service.reservePayment(System.currentTimeMillis() + 2_500)
        }
    }
}
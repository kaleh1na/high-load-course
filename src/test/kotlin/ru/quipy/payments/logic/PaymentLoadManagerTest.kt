package ru.quipy.payments.logic

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.mock.env.MockEnvironment
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
    fun `queue limit uses p99 and release is idempotent`() {
        val paymentLoadManager = PaymentLoadManager(account, Duration.ofSeconds(10), 16, clock)
        assertEquals(Duration.ofSeconds(2), paymentLoadManager.retryAfter)
        assertNull(paymentLoadManager.tryReserve(110_000))
        assertNull(paymentLoadManager.tryReserve(111_999))
        val reservation = paymentLoadManager.tryReserve(112_000)
        assertNotNull(reservation)
        assertEquals(10_000.0, reservation!!.estimatedMillis)
        assertNull(paymentLoadManager.tryReserve(112_000))
        reservation.close()
        reservation.close()
        val next = paymentLoadManager.tryReserve(112_000)
        assertNotNull(next)
        assertNull(paymentLoadManager.tryReserve(112_000))
        next!!.close()
        val largerP99 = PaymentLoadManager(account, Duration.ofSeconds(20), 16, clock)
        assertNull(largerP99.tryReserve(123_999))
        largerP99.tryReserve(124_000)!!.close()
        val shortDeadline = PaymentLoadManager(
            account.copy(parallelRequests = 10_000, rateLimitPerSec = 110, averageProcessingTime = Duration.ofSeconds(1)),
            Duration.ofMillis(3500), 16, clock
        )
        assertNull(shortDeadline.tryReserve(103_000))
    }

    @Test
    fun `configured account p99 determines queue capacity and retry interval`() {
        val environment = MockEnvironment()
            .withProperty("payment.accounts-p99.acc-5", "10s")
            .withProperty("payment.accounts-p99.acc-other", "2000ms")
        val accountsP99 = Binder.get(environment)
            .bind("payment.accounts-p99", Bindable.mapOf(String::class.java, Duration::class.java)).get()
        val manager = PaymentLoadManager(account, accountsP99.getValue(account.accountName), 16, clock)
        assertEquals(Duration.ofSeconds(2), manager.retryAfter)
        assertNull(manager.tryReserve(111_999))
        val reservation = manager.tryReserve(112_000)!!
        assertEquals(10_000.0, reservation.estimatedMillis)
        assertNull(manager.tryReserve(112_000))
        reservation.close()

        val reservations = (1..25).map { manager.tryReserve(160_000)!! }
        assertNull(manager.tryReserve(160_000))
        reservations.forEach { it.close() }

        val otherAccount = PaymentLoadManager(
            account.copy(accountName = "acc-other"), accountsP99.getValue("acc-other"), 16, clock
        )
        assertEquals(2_000.0, otherAccount.tryReserve(110_980)!!.estimatedMillis)
    }

    @Test
    fun `simultaneous requests cannot exceed calculated queue limit`() {
        val paymentLoadManager = PaymentLoadManager(account, Duration.ofSeconds(10), 16, clock)
        val callers = Executors.newFixedThreadPool(8)
        try {
            val reservations = callers.invokeAll((1..100).map {
                Callable { paymentLoadManager.tryReserve(160_000) }
            }).mapNotNull { it.get() }
            assertEquals(25, reservations.size)
            reservations.forEach { it.close() }
            paymentLoadManager.tryReserve(112_000)!!.close()
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
            val next = paymentLoadManager.tryReserve(112_000)
            assertNotNull(next)
            assertNull(paymentLoadManager.tryReserve(112_000))
            next!!.close()
            finish.countDown()
            task.get(5, TimeUnit.SECONDS)
            assertNull(paymentLoadManager.tryReserve(112_000))
            first.close()
            paymentLoadManager.tryReserve(112_000)!!.close()
        } finally {
            finish.countDown()
            first.close()
            worker.shutdownNow()
        }
    }

    @Test
    fun `queue limit respects rate limit and worker count`() {
        val slowRate = PaymentLoadManager(
            account.copy(rateLimitPerSec = 1, averageProcessingTime = Duration.ofMillis(100)),
            Duration.ofMillis(350), 16, clock
        )
        assertEquals(Duration.ofSeconds(1), slowRate.retryAfter)
        val first = slowRate.tryReserve(101_350)!!
        assertNull(slowRate.tryReserve(102_349))
        slowRate.tryReserve(102_350)!!.close()
        first.close()

        val workerLimited = PaymentLoadManager(
            account.copy(parallelRequests = 30, rateLimitPerSec = 100),
            Duration.ofSeconds(10), 4, clock
        )
        assertEquals(Duration.ofMillis(2500), workerLimited.retryAfter)
        val pending = workerLimited.tryReserve(112_500)!!
        assertNull(workerLimited.tryReserve(114_999))
        workerLimited.tryReserve(115_000)!!.close()
        pending.close()
        val fractionalRate = PaymentLoadManager(
            account.copy(rateLimitPerSec = 3, averageProcessingTime = Duration.ofMillis(100)),
            Duration.ofMillis(350), 16, clock
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
        val service = PaymentSystemImpl(listOf(first, second))
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
    fun `p99 is included once per account in sequential account total`() {
        val windows = List(2) {
            PaymentLoadManager(
                account.copy(averageProcessingTime = Duration.ofSeconds(1)),
                Duration.ofMillis(3500), 16
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
        val service = PaymentSystemImpl(adapters)
        val reservation = service.reservePayment(System.currentTimeMillis() + 8_000)
        assertEquals(7_000.0, reservation.estimatedMillis)
        reservation.close()
        assertThrows(RejectedExecutionException::class.java) {
            service.reservePayment(System.currentTimeMillis() + 6_000)
        }
    }
}
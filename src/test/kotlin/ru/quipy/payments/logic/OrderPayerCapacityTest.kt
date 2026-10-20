package ru.quipy.payments.logic

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions
import org.mockito.Mockito.`when`
import org.springframework.test.util.ReflectionTestUtils
import ru.quipy.apigateway.APIController.Order
import ru.quipy.apigateway.APIController.OrderStatus
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.repository.OrderRepository
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class OrderPayerCapacityTest {
    @Test
    fun `queued task holds reservation and releases it on executor rejection or task failure`() {
        val payer = OrderPayer(1, 1)
        val executor = ReflectionTestUtils.getField(payer, "paymentExecutor") as ThreadPoolExecutor
        val paymentId = UUID.randomUUID()
        val creationFailure = IllegalStateException("Payment creation failed")
        val events = mock(EventSourcingService::class.java) {
            throw creationFailure
        }
        val service = mock(PaymentService::class.java)
        val repository = OrderRepository()
        val order = repository.save(newOrder())
        val rejectedOrder = repository.save(newOrder())
        val releases = AtomicInteger()
        `when`(service.reservePayment(Long.MAX_VALUE)).thenAnswer {
            PaymentReservation(0.0) { releases.incrementAndGet() }
        }
        ReflectionTestUtils.setField(payer, "paymentService", service)
        ReflectionTestUtils.setField(payer, "paymentESService", events)
        ReflectionTestUtils.setField(payer, "orderRepository", repository)
        val started = CountDownLatch(executor.maximumPoolSize)
        val releaseWorker = CountDownLatch(1)
        try {
            repeat(executor.maximumPoolSize) {
                executor.execute {
                    started.countDown()
                    releaseWorker.await()
                }
            }
            assertTrue(started.await(5, TimeUnit.SECONDS))
            payer.processPayment(order, paymentId, Long.MAX_VALUE)
            assertEquals(1, executor.queue.size)
            assertEquals(OrderStatus.PAYMENT_IN_PROGRESS, repository.findById(order.id)!!.status)
            val task = executor.queue.peek() as Future<*>
            assertEquals(0, releases.get())

            assertThrows(RejectedExecutionException::class.java) {
                payer.processPayment(rejectedOrder, UUID.randomUUID(), Long.MAX_VALUE)
            }
            assertEquals(rejectedOrder, repository.findById(rejectedOrder.id))
            assertEquals(1, releases.get())
            releaseWorker.countDown()
            executor.shutdown()
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS))
            val failure = assertThrows(ExecutionException::class.java) { task.get(5, TimeUnit.SECONDS) }
            assertSame(creationFailure, failure.cause)
            assertEquals(2, releases.get())
            verify(service, times(2)).reservePayment(Long.MAX_VALUE)
            verifyNoMoreInteractions(service)
        } finally {
            releaseWorker.countDown()
            executor.shutdownNow()
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS))
        }
    }

    @Test
    fun `deadline rejection does not enqueue payment`() {
        val payer = OrderPayer(1, 1)
        val executor = ReflectionTestUtils.getField(payer, "paymentExecutor") as ThreadPoolExecutor
        val service = mock(PaymentService::class.java)
        val repository = OrderRepository()
        val order = repository.save(newOrder())
        `when`(service.reservePayment(Long.MAX_VALUE)).thenThrow(RejectedExecutionException("Deadline"))
        ReflectionTestUtils.setField(payer, "paymentService", service)
        ReflectionTestUtils.setField(payer, "orderRepository", repository)
        try {
            assertThrows(RejectedExecutionException::class.java) {
                payer.processPayment(order, UUID.randomUUID(), Long.MAX_VALUE)
            }
            assertEquals(0L, executor.taskCount)
            assertEquals(order, repository.findById(order.id))
        } finally {
            executor.shutdownNow()
        }
    }

    private fun newOrder() = Order(UUID.randomUUID(), UUID.randomUUID(), 0L, OrderStatus.COLLECTING, 100)
}
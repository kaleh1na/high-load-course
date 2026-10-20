package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.apigateway.APIController.Order
import ru.quipy.apigateway.APIController.OrderStatus
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    @Value("\${payment.worker-count}") workerCount: Int,
    @Value("\${payment.queue-capacity}") queueCapacity: Int
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var orderRepository: OrderRepository

    private val paymentExecutor = ThreadPoolExecutor(
        workerCount,
        workerCount,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(queueCapacity),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

    fun processPayment(order: Order, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        val reservation = paymentService.reservePayment(deadline)
        var statusChanged = false
        try {
            orderRepository.save(order.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            statusChanged = true
            paymentExecutor.submit {
                try {
                    val createdEvent = paymentESService.create {
                        it.create(
                            paymentId,
                            order.id,
                            order.price
                        )
                    }
                    logger.trace("Payment ${createdEvent.paymentId} for order ${order.id} created.")

                    paymentService.submitPaymentRequest(paymentId, order.price, createdAt, deadline)
                } finally {
                    reservation.close()
                }
            }
        } catch (e: Throwable) {
            try {
                if (statusChanged) {
                    orderRepository.save(order)
                }
            } finally {
                reservation.close()
            }
            throw e
        }
        return createdAt
    }
}
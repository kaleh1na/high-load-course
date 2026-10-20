package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
    @Value("\${payment.deadline-reserve-time}") private val deadlineReserveTime: Duration
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun reservePayment(deadline: Long): PaymentReservation {
        val reservations = mutableListOf<PaymentReservation>()
        try {
            for (account in paymentAccounts) {
                reservations += account.tryReservePayment(deadline)
                    ?: throw RejectedExecutionException("Payment cannot meet its deadline")
            }
            val estimatedMillis = reservations.sumOf { it.estimatedMillis }
            if (estimatedMillis + deadlineReserveTime.toMillis() > deadline.toDouble() - now()) {
                throw RejectedExecutionException("Payment cannot meet its deadline")
            }
            return PaymentReservation(estimatedMillis) {
                reservations.forEach { it.close() }
            }
        } catch (e: Throwable) {
            reservations.forEach { it.close() }
            throw e
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }
}
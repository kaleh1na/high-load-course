package ru.quipy.payments.config

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.springframework.mock.env.MockEnvironment
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.PaymentAggregateState
import java.util.UUID

class PaymentAccountsConfigTest {
    @Test
    fun `missing p99 for selected account fails before contacting provider`() {
        val config = PaymentAccountsConfig().apply {
            allowedAccounts = listOf("acc-5", "acc-18")
        }
        val environment = MockEnvironment().withProperty("payment.accounts-p99.acc-5", "10s")
        val events = mock<EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>>()
        val registry = SimpleMeterRegistry()
        try {
            val error = assertThrows(IllegalArgumentException::class.java) {
                config.accountAdapters(events, registry, environment, 16)
            }
            assertEquals("Missing payment.accounts-p99.acc-18", error.message)
        } finally {
            registry.close()
        }
    }
}
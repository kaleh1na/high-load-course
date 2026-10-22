package ru.quipy.payments.config

import com.sun.net.httpserver.HttpServer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.springframework.mock.env.MockEnvironment
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.PaymentAggregateState
import java.net.InetSocketAddress
import java.time.Duration
import java.util.UUID

class PaymentAccountsConfigTest {
    @Test
    fun `configured p99 takes precedence and missing p99 falls back to account average`() {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/external/accounts") { exchange ->
            exchange.use {
                val body = """[
                    {"serviceName":"test","accountName":"configured","parallelRequests":5,"rateLimitPerSec":3,"price":30,"averageProcessingTime":"PT4.9S","enabled":true},
                    {"serviceName":"test","accountName":"fallback","parallelRequests":5,"rateLimitPerSec":3,"price":30,"averageProcessingTime":"PT4.9S","enabled":true}
                ]""".toByteArray()
                exchange.sendResponseHeaders(200, body.size.toLong())
                exchange.responseBody.write(body)
            }
        }
        server.start()
        val config = PaymentAccountsConfig().apply {
            allowedAccounts = listOf("configured", "fallback")
            paymentProviderHostPort = "127.0.0.1:${server.address.port}"
            serviceName = "test"
            token = "test"
        }
        val environment = MockEnvironment().withProperty("payment.accounts-p99.configured", "10s")
        val events = mock<EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>>()
        val registry = SimpleMeterRegistry()
        try {
            for (env in listOf(environment, MockEnvironment())) {
                val adapters = config.accountAdapters(events, registry, env, 16)
                assertEquals(2, adapters.size)
                for (adapter in adapters) {
                    val expectedMillis = if (env === environment && adapter.name() == "configured") 10000L else 4900L
                    adapter.tryReservePayment(System.currentTimeMillis() + 60000).use { reservation ->
                        assertEquals(expectedMillis.toDouble(), reservation.estimatedMillis)
                        assertEquals(Duration.ofMillis(expectedMillis / 5), reservation.retryAfter)
                    }
                }
            }
        } finally {
            server.stop(0)
            registry.close()
        }
    }
}
package ru.quipy.payments.logic

import com.sun.net.httpserver.HttpServer
import io.micrometer.core.instrument.MockClock
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.prometheus.metrics.model.registry.PrometheusRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.InetSocketAddress
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

class PaymentProviderMetricsTest {
    @Test
    fun `histogram records HTTP and body reading per account but not database updates`() {
        val clock = MockClock()
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT, PrometheusRegistry(), clock)
        val events = mock(EventSourcingService::class.java) {
            clock.add(Duration.ofSeconds(10))
            null
        } as EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/external/process") { exchange ->
            exchange.use {
                val body = """{"transactionId":"tx","paymentId":"payment","result":true,"message":null}""".toByteArray()
                val brokenBody = exchange.requestURI.query.contains("accountName=broken")
                clock.add(Duration.ofSeconds(1))
                exchange.sendResponseHeaders(200, body.size.toLong() + if (brokenBody) 1 else 0)
                clock.add(Duration.ofSeconds(1))
                exchange.responseBody.write(body)
            }
        }
        server.start()
        try {
            for (account in listOf("success", "broken")) {
                val adapter = PaymentExternalSystemAdapterImpl(
                    PaymentAccountProperties("test", account, 5, 3, 30, Duration.ofSeconds(1), true),
                    events, "127.0.0.1:${server.address.port}", "test", Duration.ofSeconds(2), 16, registry
                )
                adapter.performPaymentAsync(UUID.randomUUID(), 30, now(), Long.MAX_VALUE)
                val timer = registry.get("payment_provider_request_duration")
                    .tags("service", "test", "account", account).timer()
                assertEquals(1L, timer.count())
                assertEquals(2.0, timer.totalTime(TimeUnit.SECONDS))
                assertTrue(registry.scrape().lineSequence().any {
                    it.startsWith("payment_provider_request_duration_seconds_bucket{") &&
                        it.contains("account=\"$account\"") && it.contains("service=\"test\"")
                })
            }
        } finally {
            server.stop(0)
            registry.close()
        }
    }
}
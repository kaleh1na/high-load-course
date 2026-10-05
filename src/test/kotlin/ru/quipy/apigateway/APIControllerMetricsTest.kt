package ru.quipy.apigateway

import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.springframework.test.util.ReflectionTestUtils
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.header
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.util.UUID
import java.util.concurrent.RejectedExecutionException

class APIControllerMetricsTest {
    private lateinit var registry: PrometheusMeterRegistry
    private lateinit var controller: APIController
    private lateinit var orderRepository: OrderRepository
    private lateinit var orderPayer: OrderPayer

    @BeforeEach
    fun setUp() {
        registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        controller = APIController(registry)
        orderRepository = OrderRepository()
        orderPayer = mock(OrderPayer::class.java)
        ReflectionTestUtils.setField(controller, "orderRepository", orderRepository)
        ReflectionTestUtils.setField(controller, "orderPayer", orderPayer)
    }

    @AfterEach
    fun tearDown() {
        registry.close()
    }

    @Test
    fun `accepted payment returns 200 and updates order and counter`() {
        val order = existingOrder()
        val mvc = MockMvcBuilders.standaloneSetup(controller).build()

        mvc.perform(post("/orders/{orderId}/payment", order.id)
            .param("deadline", Long.MAX_VALUE.toString()))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.timestamp").value(0))
            .andExpect(jsonPath("$.transactionId").isString)
            .andExpect(header().doesNotExist("Retry-After"))

        assertEquals(APIController.OrderStatus.PAYMENT_IN_PROGRESS, orderRepository.findById(order.id)!!.status)
        assertCount("accepted", 1.0)
        assertCount("rejected", 0.0)
    }

    @Test
    fun `overload returns 429 with retry timestamp without changing the order`() {
        val order = existingOrder()
        val overloadedPayer = mock(OrderPayer::class.java) { throw RejectedExecutionException("Busy") }
        ReflectionTestUtils.setField(controller, "orderPayer", overloadedPayer)
        val mvc = MockMvcBuilders.standaloneSetup(controller).build()
        val before = System.currentTimeMillis()

        val response = mvc.perform(post("/orders/{orderId}/payment", order.id)
            .param("deadline", Long.MAX_VALUE.toString()))
            .andExpect(status().isTooManyRequests)
            .andReturn().response

        val retryAfter = response.getHeader("Retry-After")!!.toLong()
        assertTrue(retryAfter >= before + 1000)
        assertTrue(retryAfter <= System.currentTimeMillis() + 1000)
        assertEquals("", response.contentAsString)
        assertEquals(APIController.OrderStatus.COLLECTING, orderRepository.findById(order.id)!!.status)
        assertCount("accepted", 0.0)
        assertCount("rejected", 1.0)

        ReflectionTestUtils.setField(controller, "orderPayer", orderPayer)
        assertEquals(200, controller.payOrder(order.id, Long.MAX_VALUE).statusCode.value())
        assertCount("accepted", 1.0)
        assertCount("rejected", 1.0)
    }

    private fun existingOrder(): APIController.Order {
        val order = APIController.Order(UUID.randomUUID(), UUID.randomUUID(), 0L, APIController.OrderStatus.COLLECTING, 100)
        return orderRepository.save(order)
    }

    private fun assertCount(status: String, expected: Double) {
        assertEquals(expected, registry.get("incoming_payment_requests").tag("status", status).counter().count())
    }
}
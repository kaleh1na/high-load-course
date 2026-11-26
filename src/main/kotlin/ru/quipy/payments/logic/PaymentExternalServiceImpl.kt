package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    processingP99: Duration,
    workerCount: Int,
    private val registry: MeterRegistry,
    private val admissionBurst: Int? = null,
    private val callTimeout: Duration? = null,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        private const val RESULT_RESERVE_MILLIS = 500L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    init {
        require(callTimeout == null || callTimeout.toMillis() > 0)
    }

    private val client = OkHttpClient.Builder()
        .retryOnConnectionFailure(callTimeout == null)
        .build()
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)
    private val paymentLoadManager = PaymentLoadManager(properties, processingP99, workerCount)
    // Keep the short-deadline burst; bank extra permits only for time beyond this reserve.
    private val admissionReserveMillis = maxOf(2_500L, processingP99.toMillis())
    private val admissionLimiter = admissionBurst?.let {
        TokenBucketRateLimiter(rateLimitPerSec, it, 1, TimeUnit.SECONDS)
    }
    private val requestDuration = Timer.builder("payment_provider_request_duration")
        .description("Payment provider HTTP request duration including response body reading")
        .tags("service", serviceName, "account", accountName)
        .publishPercentileHistogram()
        .register(registry)

    override fun tryReservePayment(deadline: Long): PaymentReservation {
        val reservation = paymentLoadManager.tryReserve(deadline)
            ?: throw PaymentRejectedException(paymentLoadManager.retryAfter, "Payment cannot meet its deadline")
        val delay = admissionBurst?.let { baseBurst ->
            val extraMillis = (deadline - now() - admissionReserveMillis).coerceAtLeast(0)
            val capacity = (baseBurst + extraMillis / 1000.0 * rateLimitPerSec).toInt()
            admissionLimiter?.tryAcquire(capacity)
        }
        if (delay != null) {
            reservation.close()
            throw PaymentRejectedException(delay, "Payment admission rate exceeded")
        }
        return reservation
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        ongoingWindow.acquire()
        try {
            logger.warn("[$accountName] Submitting payment request for payment $paymentId")

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            awaitRatePermit(deadline)
            paymentLoadManager.processing {
                val firstAttemptStartedAt = System.nanoTime()
                val (responseCode, responseBody) = try {
                    executeAttempt(request, deadline)
                } catch (e: InterruptedIOException) {
                    if (callTimeout == null || !e.isRequestTimeout()) throw e

                    val firstAttemptMillis = elapsedMillis(firstAttemptStartedAt)
                    val remainingAfterTimeoutMillis = remainingHttpMillis(deadline)
                    var retryStartedAt: Long? = null
                    var retryBudgetMillis: Long? = null
                    var outcome = "deadline_before_retry"
                    try {
                        awaitRatePermit(deadline, requestAverageProcessingTime.toMillis().coerceAtLeast(1L))
                        retryBudgetMillis = remainingHttpMillis(deadline)
                        retryStartedAt = System.nanoTime()
                        executeAttempt(request, deadline, useRemainingBudget = true).also { (status, _) ->
                            outcome = "http_$status"
                        }
                    } catch (retryError: Exception) {
                        outcome = when {
                            !retryError.isRequestTimeout() -> "error"
                            retryStartedAt != null -> "retry_timeout"
                            else -> outcome
                        }
                        throw retryError
                    } finally {
                        logger.info(
                            "[{}] Payment retry txId={} firstAttemptMs={} remainingAfterTimeoutMs={} retryBudgetMs={} retryAttemptMs={} outcome={}",
                            accountName, transactionId, firstAttemptMillis, remainingAfterTimeoutMillis,
                            retryBudgetMillis, retryStartedAt?.let(::elapsedMillis), outcome
                        )
                    }
                }
                val body = try {
                    mapper.readValue(responseBody, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: $responseCode, reason: $responseBody")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when {
                e.isRequestTimeout() -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            ongoingWindow.release()
        }
    }

    private fun remainingHttpMillis(deadline: Long): Long = deadline - now() - RESULT_RESERVE_MILLIS

    private fun elapsedMillis(startedAt: Long): Long =
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt)

    private fun awaitRatePermit(deadline: Long, minimumMillis: Long = 1L) {
        if (!rateLimiter.tickBlocking { callTimeout == null || remainingHttpMillis(deadline) >= minimumMillis }) {
            throw SocketTimeoutException("Payment deadline reached while waiting for rate limit")
        }
    }

    private fun executeAttempt(
        request: Request,
        deadline: Long,
        useRemainingBudget: Boolean = false,
    ): Pair<Int, String?> {
        val call = client.newCall(request)
        if (callTimeout != null) {
            val remainingMillis = remainingHttpMillis(deadline)
            val timeoutMillis = if (useRemainingBudget) remainingMillis else minOf(callTimeout.toMillis(), remainingMillis)
            if (timeoutMillis <= 0) throw SocketTimeoutException("Payment deadline reached")
            call.timeout().timeout(timeoutMillis, TimeUnit.MILLISECONDS)
        }
        val sample = Timer.start(registry)
        try {
            return call.execute().use { response ->
                response.code to response.body?.string()
            }
        } finally {
            sample.stop(requestDuration)
        }
    }

    private fun Throwable.isRequestTimeout(): Boolean =
        this is SocketTimeoutException || (this is InterruptedIOException && message == "timeout")

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()

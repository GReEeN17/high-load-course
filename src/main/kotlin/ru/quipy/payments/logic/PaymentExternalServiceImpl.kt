package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.payments.api.PaymentAggregate
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
    meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        private const val QUANTILE_0_85_MULTIPLIER = 1.9
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val requestTimeoutMillis: Long = (requestAverageProcessingTime.toMillis() * QUANTILE_0_85_MULTIPLIER).toLong()

    private var payCounter = Counter
        .builder("requests.count")
        .tag("action", "asyncPayment")
        .tag("account", accountName)
        .register(meterRegistry)

    private val retryCounter = Counter
        .builder("payment.retries.count")
        .description("Number of retry attempts for payment requests")
        .tag("account", accountName)
        .register(meterRegistry)

    private val requestLatencySummary = DistributionSummary
        .builder("payment.request.latency")
        .description("Request latency in milliseconds")
        .tag("account", accountName)
        .publishPercentiles(0.5, 0.85, 0.99)
        .register(meterRegistry)

    private val requestLatencyTimer = Timer
        .builder("payment.request.duration")
        .description("Request duration in milliseconds")
        .tag("account", accountName)
        .publishPercentiles(0.5, 0.85, 0.99)
        .register(meterRegistry)

    private val client = OkHttpClient.Builder().build()

    private val outboundLimiter =
        SlidingWindowRateLimiter(rate = rateLimitPerSec.toLong(), window = Duration.ofSeconds(1))

    private val ongoingWindow = OngoingWindow(maxWinSize = parallelRequests, fair = true)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            var attempt = 0
            val maxRetries = 100
            var lastError: Exception? = null
            var finalBody: ExternalSysResponse? = null
            var success = false

            while (attempt <= maxRetries) {
                attempt++
                val remaining = deadline - System.currentTimeMillis()
                if (remaining <= 0) {
                    lastError = SocketTimeoutException("Deadline exceeded before attempt ${attempt + 1}")
                    break
                }

                if (!ongoingWindow.tryAcquire(Duration.ofMillis(remaining))) {
                    lastError = RuntimeException("Window-limited before network call on attempt $attempt")
                    break
                }

                val acquiredRate = outboundLimiter.tickBlocking(Duration.ofMillis(remaining))
                if (!acquiredRate) {
                    lastError = RuntimeException("Rate-limited during retry before attempt $attempt")
                    break
                }

                if (attempt > 1) {
                    retryCounter.increment()
                }

                var requestStartTime: Long? = null
                try {
                    requestStartTime = System.currentTimeMillis()
                    val call = client.newCall(request)

                    val timeoutMillis = minOf(requestTimeoutMillis, remaining)
                    call.timeout().timeout(timeoutMillis, TimeUnit.MILLISECONDS)
                    
                    call.execute().use { response ->
                        val requestEndTime = System.currentTimeMillis()
                        val requestDuration = requestEndTime - requestStartTime

                        requestLatencySummary.record(requestDuration.toDouble())
                        requestLatencyTimer.record(requestDuration, TimeUnit.MILLISECONDS)
                        
                        val bodyJson = response.body?.string()
                        val parsed = try {
                            mapper.readValue(bodyJson, ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            lastError = e
                            logger.error(
                                "[$accountName] Attempt ${attempt + 1}: failed to parse response. Body=$bodyJson",
                                e
                            )
                            null
                        }

                        if (parsed != null) {
                            if (parsed.result) {
                                finalBody = parsed
                                success = true
                            } else {
                                lastError = RuntimeException(parsed.message ?: "External system returned failure")
                            }
                        }
                    }
                } catch (e: Exception) {
                    if (requestStartTime != null) {
                        val requestEndTime = System.currentTimeMillis()
                        val requestDuration = requestEndTime - requestStartTime
                        if (requestDuration > 0) {
                            requestLatencySummary.record(requestDuration.toDouble())
                            requestLatencyTimer.record(requestDuration, TimeUnit.MILLISECONDS)
                        }
                    }
                    
                    lastError = e
                    logger.error("[$accountName] Attempt ${attempt + 1}: request failed.")
                } finally {
                    ongoingWindow.release()
                }

                if (success) break

                if (attempt <= maxRetries) {
                    val backoffMs = 200L
                    val sleepFor = minOf(backoffMs, maxOf(0L, deadline - System.currentTimeMillis()))
                    if (sleepFor > 0) {
                        try {
                            Thread.sleep(sleepFor)
                        } catch (_: InterruptedException) {
                        }
                    }
                }
            }

            if (finalBody != null) {
                val body = finalBody!!
                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
                payCounter.increment()
            } else {
                val err = lastError ?: RuntimeException("Unknown error")
                when (err) {
                    is SocketTimeoutException -> {
                        logger.error(
                            "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                            err
                        )
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", err)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = err.message)
                        }
                    }
                }
            }
        } finally {
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
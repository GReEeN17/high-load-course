package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.config.PaymentMetrics
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val paymentMetrics: PaymentMetrics,
    private val httpExecutor: java.util.concurrent.ExecutorService,
    private val dbExecutor: java.util.concurrent.ExecutorService
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = org.slf4j.LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private val mapper = ObjectMapper().registerKotlinModule()
        private val hedgeScheduler = Executors.newScheduledThreadPool(8,
            ru.quipy.common.utils.NamedThreadFactory("hedge-scheduler"))
    }

    private sealed class PaymentResult {
        data class Success(val message: String?) : PaymentResult()
        data class Failure(val message: String?, val status: Int?) : PaymentResult()
        object DeadlineExceeded : PaymentResult()

        fun isSuccess(): Boolean = this is Success

        companion object {
            fun fromResponse(bodyObj: ExternalSysResponse, status: Int): PaymentResult {
                return if (bodyObj.result) {
                    Success(bodyObj.message)
                } else {
                    Failure(bodyObj.message, status)
                }
            }

            fun fromException(error: Throwable): PaymentResult {
                return Failure(error.message, null)
            }
        }
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val client: HttpClient = HttpClient.newBuilder()
        .executor(httpExecutor)
        .connectTimeout(Duration.ofMillis(300))
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val hedgeCount = 10
    private val hedgeDelayMs = 100L

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.debug("[$accountName] Submitting payment request for payment $paymentId")

        val adapterEntryNanos = System.nanoTime()
        val primaryTxId = UUID.randomUUID()

        if (!rateLimiter.tick()) {
            logger.warn("[$accountName] Rate limit exceeded for payment $paymentId")
            paymentMetrics.failedIncomingRequests()
            paymentMetrics.rejectedByRateLimit()
            val currentTime = System.currentTimeMillis()
            safeDbSubmit(paymentId) {
                paymentESService.update(paymentId) {
                    it.logSubmission(false, primaryTxId, currentTime, Duration.ofMillis(currentTime - paymentStartedAt))
                }
            }
            return
        }

        if (!ongoingWindow.tryAcquire(Duration.ZERO)) {
            logger.warn("[$accountName] Ongoing window full for payment $paymentId")
            paymentMetrics.failedIncomingRequests()
            paymentMetrics.rejectedByOngoingWindow()
            val currentTime = System.currentTimeMillis()
            safeDbSubmit(paymentId) {
                paymentESService.update(paymentId) {
                    it.logSubmission(false, primaryTxId, currentTime, Duration.ofMillis(currentTime - paymentStartedAt))
                }
            }
            return
        }

        paymentMetrics.ongoingWindowAcquired()

        val currentTime = System.currentTimeMillis()
        if (currentTime > deadline) {
            paymentMetrics.failedIncomingRequests()
            paymentMetrics.rejectedByDeadline()
            safeDbSubmit(paymentId) {
                paymentESService.update(paymentId) {
                    it.logSubmission(false, primaryTxId, currentTime, Duration.ofMillis(currentTime - paymentStartedAt))
                }
            }
            ongoingWindow.release()
            paymentMetrics.ongoingWindowReleased()
            return
        }

        paymentMetrics.outgoingRequests()
        paymentMetrics.recordAdapterOverhead(Duration.ofNanos(System.nanoTime() - adapterEntryNanos))

        val submissionTime = System.currentTimeMillis()
        safeDbSubmit(paymentId) {
            paymentESService.update(paymentId) {
                it.logSubmission(true, primaryTxId, submissionTime, Duration.ofMillis(submissionTime - paymentStartedAt))
            }
        }

        logger.debug("[$accountName] Submit: $paymentId, hedgeCount: $hedgeCount")

        val resultFuture = CompletableFuture<PaymentResult>()
        val esLogged = AtomicBoolean(false)
        val pending = AtomicInteger(hedgeCount)

        val onResult = fun(result: PaymentResult, txId: UUID) {
            if (result.isSuccess()) {
                if (esLogged.compareAndSet(false, true)) {
                    val ts = System.currentTimeMillis()
                    safeDbSubmit(paymentId) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(true, ts, txId, reason = (result as PaymentResult.Success).message)
                        }
                    }
                    resultFuture.complete(result)
                }
            } else {
                if (pending.decrementAndGet() == 0 && esLogged.compareAndSet(false, true)) {
                    val ts = System.currentTimeMillis()
                    val reason = when (result) {
                        is PaymentResult.Failure -> result.message
                        is PaymentResult.DeadlineExceeded -> "Deadline exceeded (all hedges)."
                        else -> "Unknown"
                    }
                    safeDbSubmit(paymentId) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, ts, txId, reason = reason)
                        }
                    }
                    resultFuture.complete(result)
                }
            }
        }

        sendSingleRequest(paymentId, amount, primaryTxId, deadline)
            .whenComplete { result, error ->
                val r = error?.let { PaymentResult.fromException(it) } ?: result ?: PaymentResult.DeadlineExceeded
                onResult(r, primaryTxId)
            }

        for (i in 1 until hedgeCount) {
            val delayMs = hedgeDelayMs * i
            val hedgeTxId = UUID.randomUUID()
            hedgeScheduler.schedule({
                if (resultFuture.isDone) {
                    pending.decrementAndGet()
                    return@schedule
                }
                if (System.currentTimeMillis() > deadline) {
                    onResult(PaymentResult.DeadlineExceeded, hedgeTxId)
                    return@schedule
                }
                paymentMetrics.outgoingRequests()
                sendSingleRequest(paymentId, amount, hedgeTxId, deadline)
                    .whenComplete { result, error ->
                        val r = error?.let { PaymentResult.fromException(it) } ?: result ?: PaymentResult.DeadlineExceeded
                        onResult(r, hedgeTxId)
                    }
            }, delayMs, TimeUnit.MILLISECONDS)
        }

        resultFuture.whenComplete { result, error ->
            ongoingWindow.release()
            paymentMetrics.ongoingWindowReleased()
            paymentMetrics.recordEndToEnd(Duration.ofMillis(System.currentTimeMillis() - paymentStartedAt))

            when (result) {
                is PaymentResult.Success -> paymentMetrics.completedSuccess()
                is PaymentResult.Failure -> paymentMetrics.completedFailure()
                is PaymentResult.DeadlineExceeded -> paymentMetrics.completedDeadlineExceeded()
                null -> {}
            }

            if (error != null) {
                logger.error("[$accountName] Unexpected error processing payment $paymentId", error)
            }
        }
    }

    /**
     * Pure HTTP call — no ES side effects. Multiple hedged calls can race safely.
     */
    private fun sendSingleRequest(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ): CompletableFuture<PaymentResult> {
        val nowTime = System.currentTimeMillis()
        if (nowTime > deadline) {
            paymentMetrics.failedOutgoingRequests()
            return CompletableFuture.completedFuture(PaymentResult.DeadlineExceeded)
        }

        val timeoutMillis = (deadline - nowTime - 50).coerceIn(50, 1450)

        val uri = URI.create(
            "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
        )

        val request = HttpRequest.newBuilder()
            .uri(uri)
            .timeout(Duration.ofMillis(timeoutMillis))
            .POST(HttpRequest.BodyPublishers.noBody())
            .header("deadline", deadline.toString())
            .header("timeout", timeoutMillis.toString())
            .build()

        val start = System.nanoTime()

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                val status = response.statusCode()
                paymentMetrics.recordExternalStatus(status)

                val bodyStr = try { response.body() } catch (e: Exception) { null }

                val bodyObj = try {
                    if (bodyStr != null) mapper.readValue(bodyStr, ExternalSysResponse::class.java)
                    else ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "empty body")
                } catch (e: Exception) {
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                paymentMetrics.recordExternalLatency(Duration.ofNanos(System.nanoTime() - start))

                if (!bodyObj.result) {
                    paymentMetrics.failedOutgoingRequests()
                }

                PaymentResult.fromResponse(bodyObj, status)
            }
            .exceptionally { error ->
                paymentMetrics.recordExternalLatency(Duration.ofNanos(System.nanoTime() - start))
                paymentMetrics.failedOutgoingRequests()
                PaymentResult.fromException(error)
            }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName

    private fun safeDbSubmit(paymentId: UUID, block: () -> Unit) {
        try {
            dbExecutor.submit(block)
        } catch (e: RejectedExecutionException) {
            logger.warn("[$accountName] DB executor rejected write for payment $paymentId")
        }
    }
}

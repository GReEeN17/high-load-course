package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.config.PaymentMetrics
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
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
import kotlin.random.Random

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
        private val retryScheduler = Executors.newScheduledThreadPool(8,
            ru.quipy.common.utils.NamedThreadFactory("retry-scheduler"))
    }

    private sealed class PaymentResult {
        data class Success(val message: String?) : PaymentResult()
        data class Failure(val message: String?, val retriable: Boolean, val status: Int?) : PaymentResult()
        object DeadlineExceeded : PaymentResult()

        fun shouldRetry(): Boolean = when (this) {
            is Failure -> retriable
            else -> false
        }

        companion object {
            fun fromResponse(bodyObj: ExternalSysResponse, status: Int): PaymentResult {
                return if (bodyObj.result) {
                    Success(bodyObj.message)
                } else {
                    val retriable = status == 408 || status == 429 || status in 500..599
                    Failure(bodyObj.message, retriable, status)
                }
            }

            fun fromException(error: Throwable): PaymentResult {
                val retriable = error is SocketTimeoutException ||
                              error is java.net.ConnectException ||
                              error is java.net.SocketException ||
                              error is java.io.InterruptedIOException
                return Failure(error.message, retriable, null)
            }
        }
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val parallelRequests = properties.parallelRequests
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val client: HttpClient = HttpClient.newBuilder()
        .executor(httpExecutor)
        .connectTimeout(Duration.ofMillis(500))
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val maxRetries = 3
    private val baseBackoff = Duration.ofMillis(30)
    private val maxBackoff = Duration.ofMillis(200)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): Boolean {
        logger.debug("[$accountName] Submitting payment request for payment $paymentId")

        val adapterEntryNanos = System.nanoTime()
        val transactionId = UUID.randomUUID()

        if (!ongoingWindow.tryAcquire(Duration.ZERO)) {
            paymentMetrics.rejectedByOngoingWindow()
            return false
        }

        paymentMetrics.ongoingWindowAcquired()

        val currentTime = System.currentTimeMillis()
        if (currentTime > deadline) {
            paymentMetrics.rejectedByDeadline()
            ongoingWindow.release()
            paymentMetrics.ongoingWindowReleased()
            return false
        }

        paymentMetrics.outgoingRequests()
        paymentMetrics.recordAdapterOverhead(Duration.ofNanos(System.nanoTime() - adapterEntryNanos))

        val submissionTime = System.currentTimeMillis()
        safeDbSubmit(paymentId) {
            paymentESService.update(paymentId) {
                it.logSubmission(true, transactionId, submissionTime, Duration.ofMillis(submissionTime - paymentStartedAt))
            }
        }

        logger.debug("[$accountName] Submit: $paymentId , txId: $transactionId")

        // Fire and forget async payment with proper cleanup
        attemptPayment(paymentId, amount, transactionId, paymentStartedAt, deadline, 0)
            .whenComplete { result, error ->
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

        return true
    }

    private fun attemptPayment(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int
    ): CompletableFuture<PaymentResult> {

        // Deadline check
        val nowTime = System.currentTimeMillis()
        if (nowTime > deadline) {
            paymentMetrics.failedOutgoingRequests()
            safeDbSubmit(paymentId) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, nowTime, transactionId, reason = "Deadline exceeded.")
                }
            }
            return CompletableFuture.completedFuture(PaymentResult.DeadlineExceeded)
        }

        val timeoutMillis = (deadline - nowTime - 50).coerceIn(50, 950)

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

        // Async HTTP call
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                val status = response.statusCode()
                paymentMetrics.recordExternalStatus(status)

                val bodyStr = try {
                    response.body()
                } catch (e: Exception) {
                    null
                }

                val bodyObj = try {
                    if (bodyStr != null) mapper.readValue(bodyStr, ExternalSysResponse::class.java)
                    else ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "empty body")
                } catch (e: Exception) {
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                val latency = Duration.ofNanos(System.nanoTime() - start)
                paymentMetrics.recordExternalLatency(latency)

                if (!bodyObj.result) {
                    paymentMetrics.failedOutgoingRequests()
                }

                val processingTime = System.currentTimeMillis()
                safeDbSubmit(paymentId) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(bodyObj.result, processingTime, transactionId, reason = bodyObj.message)
                    }
                }

                PaymentResult.fromResponse(bodyObj, status)
            }
            .exceptionally { error ->
                val latency = Duration.ofNanos(System.nanoTime() - start)
                paymentMetrics.recordExternalLatency(latency)
                paymentMetrics.failedOutgoingRequests()

                val errorTime = System.currentTimeMillis()
                val reason = if (error.cause is SocketTimeoutException) "Request timeout." else error.message
                safeDbSubmit(paymentId) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, errorTime, transactionId, reason = reason)
                    }
                }

                PaymentResult.fromException(error)
            }
            .thenCompose { result ->
                // Retry logic
                if (result.shouldRetry() && attempt < maxRetries && System.currentTimeMillis() <= deadline) {
                    paymentMetrics.incrementExternalRetry()
                    val backoffMillis = calcBackoffMillis(attempt + 1)

                    logger.debug("[$accountName] Retrying payment $paymentId, attempt ${attempt + 1}, backoff ${backoffMillis}ms")

                    // Schedule async retry
                    val retryFuture = CompletableFuture<PaymentResult>()
                    retryScheduler.schedule({
                        attemptPayment(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1)
                            .whenComplete { res, err ->
                                if (err != null) retryFuture.completeExceptionally(err)
                                else retryFuture.complete(res)
                            }
                    }, backoffMillis, TimeUnit.MILLISECONDS)

                    retryFuture
                } else {
                    // No more retries, return final result
                    when (result) {
                        is PaymentResult.Success -> logger.info("[$accountName] Payment $paymentId succeeded")
                        is PaymentResult.Failure -> logger.warn("[$accountName] Payment $paymentId failed: ${result.message}")
                        is PaymentResult.DeadlineExceeded -> logger.warn("[$accountName] Payment $paymentId deadline exceeded")
                    }
                    CompletableFuture.completedFuture(result)
                }
            }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName

    private fun safeDbSubmit(paymentId: UUID, block: () -> Unit) {
        try {
            dbExecutor.submit {
                try {
                    block()
                } catch (e: Exception) {
                    logger.debug("[$accountName] DB write failed for payment $paymentId: ${e.message}")
                }
            }
        } catch (e: RejectedExecutionException) {
            logger.warn("[$accountName] DB executor rejected write for payment $paymentId")
        }
    }

    private fun calcBackoffMillis(attempt: Int): Long {
        val exp = baseBackoff.multipliedBy((1L shl (attempt - 1).coerceAtLeast(0)))
        val capped = if (exp > maxBackoff) maxBackoff else exp
        return Random.nextLong(0, capped.toMillis().coerceAtLeast(1))
    }
}

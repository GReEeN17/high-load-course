package ru.quipy.config

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicInteger

@Component
class PaymentMetrics(private val registry: MeterRegistry) {

    // --- existing counters ---
    private val incomingRequests = registry.counter("incoming_requests")
    private val failedIncomingRequests = registry.counter("failed_incoming_requests")
    private val failedOutgoingRequests = registry.counter("failed_outgoing_requests")
    private val outgoingRequests = registry.counter("outgoing_requests")
    private val externalRetries = registry.counter("external_request_retries")

    // --- pipeline stage timers ---
    private val endToEndTimer: Timer = buildTimer("payment_end_to_end", "Total time from API entry to final payment result")
    private val queueWaitTimer: Timer = buildTimer("payment_queue_wait", "Time task waited in processPaymentExecutor queue")
    private val esCreateTimer: Timer = buildTimer("payment_es_create", "Time to create payment aggregate in DB")
    private val adapterOverheadTimer: Timer = buildTimer("payment_adapter_overhead", "Time from adapter entry to HTTP call dispatch")
    private val externalLatencyTimer: Timer = buildTimer("external_request_latency", "Latency of calls to external payment system")

    // --- rejection counters by reason ---
    private val rejectedByRateLimit = registry.counter("payment_rejected", "reason", "rate_limit")
    private val rejectedByOngoingWindow = registry.counter("payment_rejected", "reason", "ongoing_window")
    private val rejectedByDeadline = registry.counter("payment_rejected", "reason", "deadline_exceeded")
    private val rejectedByExecutor = registry.counter("payment_rejected", "reason", "executor_full")

    // --- outcome counters ---
    private val outcomeSuccess = registry.counter("payment_completed", "outcome", "success")
    private val outcomeFailure = registry.counter("payment_completed", "outcome", "failure")
    private val outcomeDeadline = registry.counter("payment_completed", "outcome", "deadline_exceeded")

    // --- ongoing window gauge ---
    private val ongoingWindowActive = AtomicInteger(0)

    init {
        registry.gauge("payment_ongoing_window_active", ongoingWindowActive)
    }

    fun registerExecutor(executor: ThreadPoolExecutor) {
        registry.gauge("payment_executor_queue_size", executor) { it.queue.size.toDouble() }
        registry.gauge("payment_executor_active_threads", executor) { it.activeCount.toDouble() }
        registry.gauge("payment_executor_pool_size", executor) { it.poolSize.toDouble() }
    }

    // --- existing ---
    fun incomingRequests() = incomingRequests.increment()
    fun failedIncomingRequests() = failedIncomingRequests.increment()
    fun outgoingRequests() = outgoingRequests.increment()
    fun failedOutgoingRequests() = failedOutgoingRequests.increment()
    fun incrementExternalRetry() = externalRetries.increment()

    // --- stage timers ---
    fun recordEndToEnd(duration: Duration) = endToEndTimer.record(duration)
    fun recordQueueWait(duration: Duration) = queueWaitTimer.record(duration)
    fun recordEsCreate(duration: Duration) = esCreateTimer.record(duration)
    fun recordAdapterOverhead(duration: Duration) = adapterOverheadTimer.record(duration)
    fun recordExternalLatency(duration: Duration) = externalLatencyTimer.record(duration)

    // --- rejection reasons ---
    fun rejectedByRateLimit() = rejectedByRateLimit.increment()
    fun rejectedByOngoingWindow() = rejectedByOngoingWindow.increment()
    fun rejectedByDeadline() = rejectedByDeadline.increment()
    fun rejectedByExecutor() = rejectedByExecutor.increment()

    // --- outcomes ---
    fun completedSuccess() = outcomeSuccess.increment()
    fun completedFailure() = outcomeFailure.increment()
    fun completedDeadlineExceeded() = outcomeDeadline.increment()

    // --- ongoing window tracking ---
    fun ongoingWindowAcquired() = ongoingWindowActive.incrementAndGet()
    fun ongoingWindowReleased() = ongoingWindowActive.decrementAndGet()

    fun recordExternalStatus(code: Int) {
        registry.counter("external_request_status", "code", code.toString()).increment()
    }

    private fun buildTimer(name: String, description: String): Timer = Timer
        .builder(name)
        .description(description)
        .publishPercentileHistogram()
        .minimumExpectedValue(Duration.ofMillis(1))
        .maximumExpectedValue(Duration.ofSeconds(60))
        .register(registry)
}

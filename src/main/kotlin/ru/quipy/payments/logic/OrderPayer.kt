package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    private val meterRegistry: MeterRegistry,
    @Value("\${app.intake.rateLimitPerSec:16}") private val intakeRateLimitPerSec: Int,
    @Value("\${app.intake.queueCapacity:8000}") private val intakeQueueCapacity: Int,
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(intakeQueueCapacity),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val inboundLimiter = SlidingWindowRateLimiter(
        rate = intakeRateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val acceptedCounter: Counter = Counter
        .builder("intake.payments.accepted")
        .register(meterRegistry)

    private val rejectedCounter: Counter = Counter
        .builder("intake.payments.rejected")
        .tag("reason", "429")
        .register(meterRegistry)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        if (deadline <= createdAt) {
            rejectedCounter.increment()
            throw TooManyRequestsException(System.currentTimeMillis() + 500)
        }

        if (!inboundLimiter.tick()) {
            rejectedCounter.increment()
            val retryAfter = System.currentTimeMillis() + 1000
            throw TooManyRequestsException(retryAfter)
        }

        // Backpressure
        if (paymentExecutor.queue.remainingCapacity() == 0) {
            rejectedCounter.increment()
            val retryAfter = System.currentTimeMillis() + 200
            throw TooManyRequestsException(retryAfter)
        }

        acceptedCounter.increment()

        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }
}
package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.*
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.*

@Service
class OrderPayer(
    private val meterRegistry: MeterRegistry,
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    // Тест 1: 15 rps - в тесте самом 15
    // Тест 2: 11 rps - в тесте самом 11
    // Тест 3: 100 rps - в тесте самом 3
    private val intakeRateLimitPerSec = 11
    // Тест 1: 5
    // Тест 2: 100 - 4 минуты (надо 3 мин. 30 сек.)
    // Тест 3: 300 - 5 минут (надо 6 мин.)
    private val intakeQueueCapacity = 80

    private val paymentExecutor = ThreadPoolExecutor(
        16, 16, 0L, TimeUnit.MILLISECONDS,
        LinkedBlockingQueue<Runnable>(intakeQueueCapacity),
        NamedThreadFactory("payment-submission-executor"),
        RejectedExecutionHandler { _, _ ->
            rejectedCounter.increment()
            logger.warn("Backpressure triggered: queue full")
            throw TooManyRequestsException(System.currentTimeMillis() + 300)
        }
    )

    private val inboundLimiter: RateLimiter = CompositeRateLimiter(
        rl1 = SlidingWindowRateLimiter(rate = intakeRateLimitPerSec.toLong(), window = Duration.ofSeconds(1)),
        rl2 = TokenBucketRateLimiter(
            ratePerSecond = intakeRateLimitPerSec,
            bucketMaxCapacity = intakeRateLimitPerSec * 2,
            ticksPerSecond = 20
        ),
        mode = CompositeMode.OR
    )

    private val acceptedCounter: Counter = Counter.builder("intake.payments.accepted").register(meterRegistry)
    private val rejectedCounter: Counter = Counter.builder("intake.payments.rejected").tag("reason", "429").register(meterRegistry)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        if (deadline <= createdAt) {
            rejectedCounter.increment()
            throw TooManyRequestsException(createdAt + 500)
        }

        val remainingCap = paymentExecutor.queue.remainingCapacity()
        if (remainingCap <= 2) {
            rejectedCounter.increment()
            val retryAfter = createdAt + 300
            throw TooManyRequestsException(retryAfter)
        }

        val waitTime = if (paymentExecutor.queue.size > intakeQueueCapacity * 0.7) 30 else 80
        val allowed = inboundLimiter.tickBlocking(Duration.ofMillis(waitTime.toLong()))

        if (!allowed) {
            rejectedCounter.increment()
            val retryAfter = createdAt + 250
            throw TooManyRequestsException(retryAfter)
        }

        acceptedCounter.increment()

        try {
            paymentExecutor.submit {
                try {
                    val createdEvent = paymentESService.create {
                        it.create(paymentId, orderId, amount)
                    }
                    logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
                    paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                } catch (ex: Exception) {
                    logger.error("Error in payment submission task", ex)
                }
            }
        } catch (e: TooManyRequestsException) {
            rejectedCounter.increment()
            throw e
        } catch (e: RejectedExecutionException) {
            rejectedCounter.increment()
            throw TooManyRequestsException(System.currentTimeMillis() + 400)
        }

        return createdAt
    }
}
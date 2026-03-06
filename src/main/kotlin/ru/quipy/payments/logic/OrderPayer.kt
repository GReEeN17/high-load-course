package ru.quipy.payments.logic

import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.config.PaymentMetrics
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var paymentMetrics: PaymentMetrics

    private val processPaymentExecutor = ThreadPoolExecutor(
        200,
        200,
        5L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue<Runnable>(500),
        NamedThreadFactory("pse"),
        ThreadPoolExecutor.AbortPolicy()
    )

    var rateLimiter = SlidingWindowRateLimiter(5000L, Duration.ofSeconds(1))

    @PostConstruct
    fun init() {
        paymentMetrics.registerExecutor(processPaymentExecutor)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()

        if (!rateLimiter.tick()) {
            paymentMetrics.rejectedByRateLimit()
            return null
        }

        val submittedAt = System.nanoTime()

        processPaymentExecutor.submit {
            val queueWait = Duration.ofNanos(System.nanoTime() - submittedAt)
            paymentMetrics.recordQueueWait(queueWait)

            if (System.currentTimeMillis() > deadline) {
                paymentMetrics.rejectedByDeadline()
                return@submit
            }

            try {
                val esStart = System.nanoTime()
                val createdEvent = paymentESService.create {
                    it.create(paymentId, orderId, amount)
                }
                paymentMetrics.recordEsCreate(Duration.ofNanos(System.nanoTime() - esStart))

                logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            } catch (e: Exception) {
                logger.error("Error processing payment $paymentId for order $orderId", e)
            }
        }

        return createdAt
    }
}
package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
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

    private val processPaymentExecutor = ThreadPoolExecutor(
        110,
        110,
        5L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue<Runnable>(8_000),
        NamedThreadFactory("pse"),
        CallerBlockingRejectedExecutionHandler()
    )

    var rateLimiter = LeakingBucketRateLimiter(
        rate = 1250,
        window = Duration.ofMillis(1000),
        bucketSize = 15000
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()

        // LeakingBucket queues task submissions instead of rejecting immediately
        // The rate limiter controls when to submit to processPaymentExecutor
        val accepted = rateLimiter.tick {
            processPaymentExecutor.submit {
                try {
                    val createdEvent = paymentESService.create {
                        it.create(paymentId, orderId, amount)
                    }
                    logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)

                    paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                } catch (e: Exception) {
                    logger.error("Error processing payment $paymentId for order $orderId", e)
                }
            }
        }

        return if (accepted) createdAt else null
    }
}
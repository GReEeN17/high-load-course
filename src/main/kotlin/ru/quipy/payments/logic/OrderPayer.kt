package ru.quipy.payments.logic

import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import ru.quipy.config.PaymentMetrics
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor

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

    @Autowired
    @Qualifier("sharedDbExecutor")
    private lateinit var dbExecutor: ExecutorService

    @PostConstruct
    fun init() {
        paymentMetrics.registerExecutor(dbExecutor as ThreadPoolExecutor)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()

        if (System.currentTimeMillis() > deadline) {
            paymentMetrics.rejectedByDeadline()
            return null
        }

        try {
            dbExecutor.submit {
                try {
                    paymentESService.create { it.create(paymentId, orderId, amount) }
                } catch (e: Exception) {
                    logger.debug("Async ES create failed for payment $paymentId: ${e.message}")
                }
            }
        } catch (e: RejectedExecutionException) {
            logger.warn("DB executor rejected ES create for payment $paymentId")
        }

        if (!paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)) {
            return null
        }

        return createdAt
    }
}

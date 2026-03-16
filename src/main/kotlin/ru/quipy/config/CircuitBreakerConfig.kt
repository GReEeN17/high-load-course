package ru.quipy.config

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
class CircuitBreakerConfiguration {

    private val logger = LoggerFactory.getLogger(CircuitBreakerConfiguration::class.java)

    @Bean
    fun paymentCircuitBreakerConfig(): CircuitBreakerConfig = CircuitBreakerConfig.custom()
        .slidingWindowType(SlidingWindowType.TIME_BASED)
        .slidingWindowSize(10)
        .failureRateThreshold(50f)
        .slowCallRateThreshold(80f)
        .slowCallDurationThreshold(Duration.ofMillis(500))
        .waitDurationInOpenState(Duration.ofSeconds(5))
        .permittedNumberOfCallsInHalfOpenState(10)
        .minimumNumberOfCalls(20)
        .build()

    @Bean
    fun paymentCircuitBreaker(config: CircuitBreakerConfig): CircuitBreaker {
        val cb = CircuitBreaker.of("payment", config)

        cb.eventPublisher
            .onStateTransition { event ->
                logger.warn("CircuitBreaker '${cb.name}': ${event.stateTransition}")
            }
            .onSlowCallRateExceeded { event ->
                logger.warn("CircuitBreaker '${cb.name}': slow call rate exceeded — ${event.slowCallRate}%")
            }
            .onFailureRateExceeded { event ->
                logger.warn("CircuitBreaker '${cb.name}': failure rate exceeded — ${event.failureRate}%")
            }

        return cb
    }
}

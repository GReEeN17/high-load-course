package ru.quipy.config

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class MeterConfig {

    @Bean
    fun registry(): MeterRegistry {
        return PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    }
}
package ru.quipy.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Configuration
class ExecutorConfig {

    @Bean
    fun sharedHttpExecutor(): ExecutorService {
        return ThreadPoolExecutor(
            200,
            400,
            60L,
            TimeUnit.SECONDS,
            LinkedBlockingQueue(10_000),
            NamedThreadFactory("shared-http"),
            CallerBlockingRejectedExecutionHandler(Duration.ofMinutes(30))
        )
    }

    @Bean
    fun sharedDbExecutor(): ExecutorService {
        return ThreadPoolExecutor(
            100,
            200,
            60L,
            TimeUnit.SECONDS,
            LinkedBlockingQueue(5_000),
            NamedThreadFactory("shared-db"),
            CallerBlockingRejectedExecutionHandler(Duration.ofMinutes(30))
        )
    }
}

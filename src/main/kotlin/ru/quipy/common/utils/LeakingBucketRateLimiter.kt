package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    bucketSize: Int,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val queue = LinkedBlockingQueue<() -> Unit>(bucketSize)

    override fun tick(): Boolean = false

    override fun tickBlocking() {
        // Note: LeakingBucketRateLimiter uses task-based tick(task) instead of no-arg tick()
        // This method is not supported and will spin indefinitely since tick() always returns false
        while (!tick()) {
            // Tight spin-wait without sleep to avoid blocking threads
        }
    }

    override fun tickBlocking(timeout: Duration): Boolean {
        // Note: LeakingBucketRateLimiter uses task-based tick(task) instead of no-arg tick()
        // This method is not supported and will always return false since tick() always returns false
        val deadline = System.currentTimeMillis() + timeout.toMillis()
        while (System.currentTimeMillis() <= deadline) {
            if (tick()) return true
            // Tight spin-wait without sleep to avoid blocking threads
        }
        return false
    }

    fun tick(task: () -> Unit): Boolean {
        return queue.offer(task)
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            delay(window.toMillis())
            for (i in 0..<rate) {
                queue.poll()?.invoke()
            }
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }
}
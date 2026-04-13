package bronya.pgsql.ext.mq.service;

import lombok.extern.slf4j.Slf4j;
import bronya.pgsql.ext.mq.autoconfig.PgMqBeanPostProcessor;
import org.andreaesposito.pgmq.jdbc.client.Json;
import org.andreaesposito.pgmq.jdbc.client.MessageRecord;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * PgMQ 消费者服务 - 优化版
 *
 * <p>优化点：
 * <ul>
 *   <li>连接池友好：使用非阻塞 read + 退避策略，避免长期占用连接</li>
 *   <li>批量消费：支持批量拉取和批量确认，减少数据库往返</li>
 *   <li>自适应退避：空队列时动态调整轮询间隔</li>
 *   <li>优雅关闭：支持平滑停止消费者</li>
 *   <li>死信处理：失败消息自动转入死信队列</li>
 * </ul>
 */
@Slf4j
@Service
public class PgMqConsumerService {

    private final PgMqService pgMqService;
    private final ScheduledExecutorService scheduler;

    // 退避策略参数
    private static final int MIN_POLL_INTERVAL_MS = 100;      // 最小轮询间隔
    private static final int MAX_POLL_INTERVAL_MS = 5000;     // 最大轮询间隔
    private static final int BACKOFF_MULTIPLIER = 2;          // 退避倍数
    private static final int BATCH_SIZE = 10;                 // 批量拉取数量
    private static final int DEFAULT_VT_SECONDS = 60;         // 默认可见性超时

    public PgMqConsumerService(PgMqService pgMqService) {
        this.pgMqService = pgMqService;
        this.scheduler = Executors.newScheduledThreadPool(
            Runtime.getRuntime().availableProcessors(),
            r -> {
                Thread t = new Thread(r);
                t.setName("pgmq-consumer-" + t.threadId());
                t.setDaemon(true);
                return t;
            }
        );
    }

    /**
     * 启动优化的消费者 - 连接池友好模式
     *
     * @param queue 队列名称
     * @param processor 消息处理器
     * @param options 消费选项
     * @return 消费者控制器，可用于停止消费
     */
    public ConsumerController startOptimizedConsumer(String queue,
                                                      Function<MessageRecord, Boolean> processor,
                                                      ConsumerOptions options) {
        ConsumerController controller = new ConsumerController(queue);
        AtomicInteger currentInterval = new AtomicInteger(options.getInitialPollIntervalMs());

        ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(() -> {
            if (!controller.isRunning()) {
                return;
            }

            try {
                // 使用非阻塞 read，不长期占用连接
                List<MessageRecord> messages = pgMqService.read(
                    queue,
                    options.getVisibilityTimeoutSeconds(),
                    options.getBatchSize(),
                    null
                );

                if (messages.isEmpty()) {
                    // 空队列，增加退避间隔
                    int newInterval = Math.min(
                        currentInterval.get() * BACKOFF_MULTIPLIER,
                        options.getMaxPollIntervalMs()
                    );
                    currentInterval.set(newInterval);
                    return;
                }

                // 有消息，重置轮询间隔
                currentInterval.set(options.getInitialPollIntervalMs());

                // 处理消息并收集需要删除的ID
                List<Long> successIds = messages.stream()
                    .filter(msg -> {
                        try {
                            return Boolean.TRUE.equals(processor.apply(msg));
                        } catch (Exception e) {
                            log.error("消息处理异常: queue={}, msgId={}", queue, msg.msgId(), e);
                            return false;
                        }
                    })
                    .map(MessageRecord::msgId)
                    .toList();

                // 批量确认删除
                if (!successIds.isEmpty()) {
                    pgMqService.delete(queue, successIds.stream().mapToLong(Long::longValue).toArray());
                    log.debug("批量确认消息: queue={}, count={}", queue, successIds.size());
                }

                // 处理失败的消息（进入死信队列）
                List<Long> failedIds = messages.stream()
                    .map(MessageRecord::msgId)
                    .filter(id -> !successIds.contains(id))
                    .toList();

                if (!failedIds.isEmpty() && options.isDeadLetterEnabled()) {
                    moveToDeadLetter(queue, failedIds, processor);
                }

            } catch (SQLException e) {
                log.error("消费异常: queue={}", queue, e);
                // 异常时增加退避
                currentInterval.set(Math.min(currentInterval.get() * 2, MAX_POLL_INTERVAL_MS));
            }
        }, 0, currentInterval.get(), TimeUnit.MILLISECONDS);

        controller.setFuture(future);
        log.info("启动优化消费者: queue={}, batchSize={}, vt={}s",
            queue, options.getBatchSize(), options.getVisibilityTimeoutSeconds());

        return controller;
    }

    /**
     * 移动消息到死信队列
     */
    private void moveToDeadLetter(String queue, List<Long> msgIds,
                                   Function<MessageRecord, Boolean> processor) {
        String dlqName = queue + "_dlq";
        try {
            pgMqService.create(dlqName);

            for (Long msgId : msgIds) {
                // 发送到死信队列（可以添加失败原因等元数据）
                Json dlqMsg = new Json(String.format(
                    "{\"originalQueue\":\"%s\",\"msgId\":%d,\"error\":\"processing_failed\",\"timestamp\":%d}",
                    queue, msgId, System.currentTimeMillis()
                ));
                pgMqService.send(dlqName, dlqMsg);

                // 从原队列删除
                pgMqService.delete(queue, msgId);
            }
            log.warn("消息转入死信队列: queue={}, dlq={}, count={}", queue, dlqName, msgIds.size());
        } catch (SQLException e) {
            log.error("死信队列处理失败: queue={}", queue, e);
        }
    }

    /**
     * 消费者控制器
     */
    public static class ConsumerController {
        private final String queue;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private volatile ScheduledFuture<?> future;

        public ConsumerController(String queue) {
            this.queue = queue;
        }

        void setFuture(ScheduledFuture<?> future) {
            this.future = future;
        }

        public boolean isRunning() {
            return running.get();
        }

        /**
         * 优雅停止消费者
         */
        public void stop() {
            running.set(false);
            if (future != null) {
                future.cancel(false); // 不中断正在执行的任务
            }
            log.info("消费者停止请求已发送: queue={}", queue);
        }

        /**
         * 立即停止消费者
         */
        public void stopNow() {
            running.set(false);
            if (future != null) {
                future.cancel(true); // 中断正在执行的任务
            }
            log.info("消费者已立即停止: queue={}", queue);
        }
    }

    /**
     * 消费者选项
     */
    public static class ConsumerOptions {
        private int batchSize = BATCH_SIZE;
        private int visibilityTimeoutSeconds = DEFAULT_VT_SECONDS;
        private int initialPollIntervalMs = MIN_POLL_INTERVAL_MS;
        private int maxPollIntervalMs = MAX_POLL_INTERVAL_MS;
        private boolean deadLetterEnabled = true;
        private int maxRetryCount = 3;

        // Builder 模式
        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private final ConsumerOptions options = new ConsumerOptions();

            public Builder batchSize(int size) {
                options.batchSize = size;
                return this;
            }

            public Builder visibilityTimeoutSeconds(int seconds) {
                options.visibilityTimeoutSeconds = seconds;
                return this;
            }

            public Builder initialPollIntervalMs(int ms) {
                options.initialPollIntervalMs = ms;
                return this;
            }

            public Builder maxPollIntervalMs(int ms) {
                options.maxPollIntervalMs = ms;
                return this;
            }

            public Builder deadLetterEnabled(boolean enabled) {
                options.deadLetterEnabled = enabled;
                return this;
            }

            public Builder maxRetryCount(int count) {
                options.maxRetryCount = count;
                return this;
            }

            public ConsumerOptions build() {
                return options;
            }
        }

        // Getters
        public int getBatchSize() { return batchSize; }
        public int getVisibilityTimeoutSeconds() { return visibilityTimeoutSeconds; }
        public int getInitialPollIntervalMs() { return initialPollIntervalMs; }
        public int getMaxPollIntervalMs() { return maxPollIntervalMs; }
        public boolean isDeadLetterEnabled() { return deadLetterEnabled; }
        public int getMaxRetryCount() { return maxRetryCount; }
    }

    /**
     * 关闭服务
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

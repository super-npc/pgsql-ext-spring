package bronya.pgsql.ext.mq.service;

import bronya.pgsql.ext.mq.mapper.PgmqMapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import bronya.pgsql.ext.mq.domain.*;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * PgMQ 消息队列服务
 *
 * <p>基于 PostgreSQL pgmq 扩展的消息队列封装服务，提供完整的消息队列操作能力。
 *
 * <h2>功能概述</h2>
 * <ul>
 *   <li><b>消息发送</b>: 支持普通消息、延迟消息、带 Headers 消息、批量发送</li>
 *   <li><b>消息消费</b>: 支持 read 读取、readWithPoll 轮询读取、pop 弹出</li>
 *   <li><b>消息确认</b>: 支持 delete 删除、archive 归档、setVisibilityTimeout 设置可见性超时</li>
 *   <li><b>队列管理</b>: 支持 create 创建、drop 删除、purge 清空队列</li>
 *   <li><b>监控查询</b>: 支持 listQueues 列表、metrics 指标查询</li>
 * </ul>
 *
 * <h2>消费模式选择指南</h2>
 * <ul>
 *   <li><b>readWithPoll</b>: 推荐用于常驻消费者、低延迟、手动提交、支持重试的场景（90%生产场景首选）</li>
 *   <li><b>read</b>: 用于定时/主动拉取、非阻塞、手动控制频率的场景</li>
 *   <li><b>pop</b>: 用于一次性消费、不重试、不保留、简单快速的场景</li>
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li>pgmq 是"拉取(Pull)"模式的消息队列，没有原生的"监听/推送"机制</li>
 *   <li>使用前需确保数据库已启用 pgmq 扩展: CREATE EXTENSION IF NOT EXISTS pgmq</li>
 *   <li>消息被 read 后需要手动调用 delete 才算消费完成，否则超时后自动重试</li>
 * </ul>
 *
 * @author olive
 * @see <a href="https://github.com/tembo-io/pgmq">pgmq</a>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PgMqService {
    private final PgmqMapper mapper;

    // ==================== 消息发送 ====================

    public List<Long> send(String queue, Json message) throws SQLException {
        log.debug("发送消息到队列: {}, 内容: {}", queue, message);
        return mapper.send(queue, message);
    }

    public List<Long> send(String queue, Json message, int delaySeconds) throws SQLException {
        log.debug("发送延迟消息到队列: {}, 延迟: {}秒, 内容: {}", queue, delaySeconds, message);
        return mapper.sendWithDelay(queue, message, delaySeconds);
    }

    public List<Long> send(String queue, Json message, Json headers) throws SQLException {
        log.debug("发送带Headers消息到队列: {}, Headers: {}, 内容: {}", queue, headers, message);
        return mapper.sendWithHeaders(queue, message, headers);
    }

    public List<Long> send(String queue, Json message, OffsetDateTime delayAt) throws SQLException {
        log.debug("发送定时延迟消息到队列: {}, 延迟至: {}, 内容: {}", queue, delayAt, message);
        return mapper.sendWithTimestamp(queue, message, delayAt);
    }

    public List<Long> send(String queue, Json message, Json headers, int delaySeconds) throws SQLException {
        log.debug("发送带Headers延迟消息到队列: {}, Headers: {}, 延迟: {}秒, 内容: {}", queue, headers, delaySeconds, message);
        return mapper.sendWithHeadersAndDelay(queue, message, headers, delaySeconds);
    }

    public List<Long> send(String queue, Json message, Json headers, OffsetDateTime delayAt) throws SQLException {
        log.debug("发送带Headers定时延迟消息到队列: {}, Headers: {}, 延迟至: {}, 内容: {}", queue, headers, delayAt, message);
        return mapper.sendWithHeadersAndTimestamp(queue, message, headers, delayAt);
    }

    public List<Long> sendBatch(String queue, Json[] messages) throws SQLException {
        log.debug("批量发送{}条消息到队列（无Headers）: {}", messages.length, queue);
        return mapper.sendBatch(queue, messages);
    }

    public List<Long> sendBatch(String queue, Json[] messages, int delaySeconds) throws SQLException {
        log.debug("批量发送{}条延迟消息到队列: {}, 延迟: {}秒", messages.length, queue, delaySeconds);
        return mapper.sendBatchWithDelay(queue, messages, delaySeconds);
    }

    public List<Long> sendBatch(String queue, Json[] messages, OffsetDateTime delayAt) throws SQLException {
        log.debug("批量发送{}条定时延迟消息到队列: {}, 延迟至: {}", messages.length, queue, delayAt);
        return mapper.sendBatchWithTimestamp(queue, messages, delayAt);
    }

    public List<Long> sendBatch(String queue, Json[] messages, Json[] headers) throws SQLException {
        log.debug("批量发送{}条消息到队列: {}", messages.length, queue);
        return mapper.sendBatchWithHeaders(queue, messages, headers);
    }

    public List<Long> sendBatch(String queue, Json[] messages, Json[] headers, int delaySeconds) throws SQLException {
        log.debug("批量发送{}条带Headers延迟消息到队列: {}, 延迟: {}秒", messages.length, queue, delaySeconds);
        return mapper.sendBatchWithHeadersAndDelay(queue, messages, headers, delaySeconds);
    }

    public List<Long> sendBatch(String queue, Json[] messages, Json[] headers, OffsetDateTime delayAt) throws SQLException {
        log.debug("批量发送{}条带Headers定时延迟消息到队列: {}, 延迟至: {}", messages.length, queue, delayAt);
        return mapper.sendBatchWithHeadersAndTimestamp(queue, messages, headers, delayAt);
    }

    // ==================== 消息读取 ====================

    public List<MessageRecord> read(String queue, int visibilityTimeoutSeconds, int quantity, Json filter) throws SQLException {
        log.debug("读取队列: {}, VT: {}秒, 数量: {}", queue, visibilityTimeoutSeconds, quantity);
        return mapper.read(queue, visibilityTimeoutSeconds, quantity, filter);
    }

//    public List<MessageRecord> readWithPoll(String queue, int visibilityTimeoutSeconds, int pollSeconds, Json filter) throws SQLException {
//        log.debug("轮询读取队列: {}, VT: {}秒, 轮询: {}秒", queue, visibilityTimeoutSeconds, pollSeconds);
//        return mapper.readWithPoll(queue, visibilityTimeoutSeconds, pollSeconds, filter);
//    }

    public List<MessageRecord> readWithPoll(String queue, int visibilityTimeoutSeconds, int quantity,
                                             int pollSeconds, int pollIntervalMs, Json filter) throws SQLException {
        log.debug("高级轮询读取队列: {}, VT: {}秒, 数量: {}, 轮询: {}秒, 间隔: {}ms",
                queue, visibilityTimeoutSeconds, quantity, pollSeconds, pollIntervalMs);
        return mapper.readWithPollFull(queue, visibilityTimeoutSeconds, quantity, pollSeconds, pollIntervalMs, filter);
    }

    public Optional<MessageRecord> pop(String queue) throws SQLException {
        log.debug("弹出队列消息: {}", queue);
        return mapper.pop(queue);
    }

    // ==================== 消息确认 ====================

    public List<Long> delete(String queue, long... messageIds) throws SQLException {
        log.debug("删除队列: {}, 消息IDs: {}", queue, java.util.Arrays.toString(messageIds));
        return mapper.deleteBatch(queue, messageIds);
    }

    public boolean delete(String queue, long messageId) throws SQLException {
        log.debug("删除单条消息: 队列={}, 消息ID={}", queue, messageId);
        return mapper.deleteMessage(queue, messageId);
    }

    public boolean archive(String queue, long messageId) throws SQLException {
        log.debug("归档消息: 队列={}, 消息ID={}", queue, messageId);
        return mapper.archiveMessage(queue, messageId);
    }

    public List<Long> archive(String queue, long... messageIds) throws SQLException {
        log.debug("批量归档消息: 队列={}, 消息IDs: {}", queue, java.util.Arrays.toString(messageIds));
        return mapper.archiveBatch(queue, messageIds);
    }

    public Optional<MessageRecord> setVisibilityTimeout(String queue, long messageId, int visibilityTimeoutSeconds) throws SQLException {
        log.debug("设置消息可见性超时: 队列={}, 消息ID={}, VT={}秒", queue, messageId, visibilityTimeoutSeconds);
        return mapper.setVisibilityTimeout(queue, messageId, visibilityTimeoutSeconds);
    }

    // ==================== 队列管理 ====================

    @SneakyThrows
    public void create(String queue) {
        log.info("创建队列: {}", queue);
        mapper.create(queue);
    }

    @SneakyThrows
    public void createUnlogged(String queue) {
        log.info("创建非持久化队列: {}", queue);
        mapper.createUnlogged(queue);
    }

    @SneakyThrows
    public boolean drop(String queue) {
        log.info("删除队列: {}", queue);
        return mapper.drop(queue);
    }

    @SneakyThrows
    public long purge(String queue) {
        log.info("清空队列: {}", queue);
        return mapper.purge(queue);
    }

    public List<QueueRecord> listQueues() throws SQLException {
        log.debug("列出所有队列");
        return mapper.listQueues();
    }

    public MetricResult metrics(String queue) throws SQLException {
        log.debug("获取队列指标: {}", queue);
        return mapper.metrics(queue);
    }

    public List<MetricResult> metricsAll() throws SQLException {
        log.debug("获取所有队列指标");
        return mapper.metricsAll();
    }

    // ==================== 便捷方法 ====================

    /**
     * 单次消费 - 使用非阻塞 read 配合应用层休眠，避免占用数据库长连接
     */
    public <T> boolean consumeOnce(String queue, T methodInfo, int visibilityTimeout, int batchSize, BiFunction<T, Json, Boolean> processor) throws SQLException {
        // 使用非阻塞的 read，拿不到消息立刻返回空列表，迅速释放数据库连接
        List<MessageRecord> messages = read(queue, visibilityTimeout, batchSize, null);

        if (messages.isEmpty()) {
            return false; // 返回 false 表示没有消息
        }

        for (MessageRecord msg : messages) {
            try {
                Boolean success = processor.apply(methodInfo, msg.message());
                if (Boolean.TRUE.equals(success)) {
                    delete(queue, msg.msgId());
                    log.debug("消息消费成功: queue={}, msgId={}", queue, msg.msgId());
                } else {
                    log.warn("消息处理失败，将重试: queue={}, msgId={}", queue, msg.msgId());
                }
            } catch (Exception e) {
                log.error("消息处理异常: queue={}, msgId={}, error={}", queue, msg.msgId(), e.getMessage());
                try {
                    Thread.sleep(1000); // 消息处理异常时稍作休眠，避免错误死循环过快
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return true; // 返回 true 表示处理了消息
    }

    /**
     * 消息处理结果
     */
    @Getter
    @RequiredArgsConstructor
    public static class MessageProcessResult {
        private final boolean success;
        private final boolean moveToDlq;
        private final int retryCount;
        private final int maxRetry;
        private final String errorMessage;

        public static MessageProcessResult success() {
            return new MessageProcessResult(true, false, 0, 0, null);
        }

        public static MessageProcessResult failure(int retryCount, int maxRetry, String errorMessage) {
            boolean moveToDlq = retryCount >= maxRetry;
            return new MessageProcessResult(false, moveToDlq, retryCount, maxRetry, errorMessage);
        }
    }

    /**
     * 重试感知的单次消费 - 支持重试计数和 DLQ
     * <p>提供完整的消息信息，包括 headers，用于重试控制
     */
    public <T> boolean consumeOnceWithRetry(String queue, T methodInfo, int visibilityTimeout, int batchSize, BiFunction<T, MessageRecord, MessageProcessResult> processor) throws SQLException {
        // 使用非阻塞的 read，拿不到消息立刻返回空列表，迅速释放数据库连接
        List<MessageRecord> messages = read(queue, visibilityTimeout, batchSize, null);

        if (messages.isEmpty()) {
            return false; // 返回 false 表示没有消息
        }

        for (MessageRecord msg : messages) {
            try {
                MessageProcessResult result = processor.apply(methodInfo, msg);

                if (result.isSuccess()) {
                    // 处理成功，删除消息
                    delete(queue, msg.msgId());
                    log.debug("消息消费成功: queue={}, msgId={}", queue, msg.msgId());
                } else if (result.isMoveToDlq()) {
                    // 超过最大重试次数，归档到 DLQ
                    boolean archived = archive(queue, msg.msgId());
                    if (archived) {
                        log.warn("消息超过最大重试次数，已归档到 DLQ: queue={}, msgId={}, retryCount={}/{}", queue, msg.msgId(), result.getRetryCount(), result.getMaxRetry());
                    } else {
                        // 归档失败，尝试删除
                        delete(queue, msg.msgId());
                        log.error("归档失败，已删除消息: queue={}, msgId={}", queue, msg.msgId());
                    }
                } else {
                    // 处理失败但未超过重试次数，等待重试
                    log.warn("消息处理失败，将重试: queue={}, msgId={}, retryCount={}/{}", queue, msg.msgId(), result.getRetryCount(), result.getMaxRetry());
                }
            } catch (Exception e) {
                log.error("消息处理异常: queue={}, msgId={}, error={}", queue, msg.msgId(), e.getMessage());
                try {
                    Thread.sleep(1000); // 消息处理异常时稍作休眠，避免错误死循环过快
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return true; // 返回 true 表示处理了消息
    }
}

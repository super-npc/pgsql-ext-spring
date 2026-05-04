package bronya.pgsql.ext.mq.consumer;

import bronya.pgsql.ext.mq.annotation.PgMqDeadLetterConfig;
import bronya.pgsql.ext.mq.annotation.PgMqListener.SubscribeType;

/**
 * 记录监听器方法信息
 */
public record ListenerMethodInfo(String queueName, String className, String methodName, String beanName,
                                 Class<?> msgDtoClass, SubscribeType subscribeType, int visibilityTimeout,
                                 int batchSize, PgMqDeadLetterConfig deadLetterConfig) {

    /**
     * 是否启用死信队列
     */
    public boolean enableDlq() {
        return deadLetterConfig != null && deadLetterConfig.enableDlq();
    }

    /**
     * 获取最大重试次数
     */
    public int maxRetry() {
        return deadLetterConfig != null ? deadLetterConfig.maxRetry() : 3;
    }

    /**
     * 获取重试延迟时间
     */
    public int retryDelaySeconds() {
        return deadLetterConfig != null ? deadLetterConfig.retryDelaySeconds() : 0;
    }
}
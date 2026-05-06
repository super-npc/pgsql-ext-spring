package bronya.pgsql.ext.mq.consumer;

import bronya.pgsql.ext.mq.annotation.PgMqListener;
import bronya.pgsql.ext.mq.annotation.PgMqListener.SubscribeType;
import bronya.pgsql.ext.mq.autoconfig.strategy.SubscribeStrategy;
import bronya.pgsql.ext.mq.constant.PgmqFinalConstant;
import bronya.pgsql.ext.mq.service.PgMqService;
import bronya.pgsql.ext.mq.service.PgMqService.MessageProcessResult;
import bronya.pgsql.ext.mq.util.SysPgMqUtil;
import cn.hutool.v7.extra.spring.SpringUtil;
import com.alibaba.fastjson2.JSON;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.andreaesposito.pgmq.jdbc.client.Json;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * 消费者作为一个独立的实体,所有消费者的方法都在这个类实现
 */
@Slf4j
@RequiredArgsConstructor
public class QueueConsumer {
    private final Class<?> clazz;
    private final String beanName;
    private final Method method;
    private final PgMqListener annotation;

    @Getter
    private ListenerMethodInfo methodInfo;
    private boolean needCreateQueue;

    public SubscribeType getSubscribeType() {
        return annotation.subscribeType();
    }

    /**
     * 启动消费者线程
     */
    public void start(int consumerIndex) {
        if (methodInfo == null) {
            log.warn("消费者未注册: class={}, method={}", clazz.getName(), method.getName());
            return;
        }
        startConsumer(methodInfo.queueName(), methodInfo, consumerIndex, needCreateQueue);
    }

    /**
     * 启动消费者线程
     */
    private void startConsumer(String queueName, ListenerMethodInfo methodInfo, int consumerIndex, boolean needCreateQueue) {
        String threadName = String.format("%s-%d", queueName, consumerIndex);
        Thread.currentThread().setName(threadName);

        log.info("启动消费者: queue={}, consumer={}, class={}, method={}", queueName, consumerIndex, methodInfo.className(), methodInfo.methodName());

        if (needCreateQueue) {
            this.ensureQueueCreated(queueName);
        }

        // 【修复内存泄漏】仅启动时获取一次Bean，不再循环获取
        PgMqService pgMqService = SpringUtil.getBean(PgMqService.class);
        Object targetBean = SpringUtil.getBean(beanName);

        // 初始休眠间隔（毫秒）
        long sleepMs = 100;
        final long MAX_SLEEP_MS = 5000;

        while (true) {
            try {
                boolean hasMessage;
                if (methodInfo.enableDlq()) {
                    hasMessage = pgMqService.consumeOnceWithRetry(
                            queueName,
                            methodInfo,
                            methodInfo.visibilityTimeout(),
                            methodInfo.batchSize(),
                            (info, record) -> processMessageWithRetry(targetBean, info, record)
                    );
                } else {
                    hasMessage = pgMqService.consumeOnce(
                            queueName,
                            methodInfo,
                            methodInfo.visibilityTimeout(),
                            methodInfo.batchSize(),
                            (info, msg) -> processMessage(targetBean, info, msg)
                    );
                }

                if (hasMessage) {
                    sleepMs = 100;
                } else {
                    try {
                        Thread.sleep(sleepMs);
                        sleepMs = Math.min(sleepMs * 2, MAX_SLEEP_MS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("消费者异常: queue={}, consumer={}, error={}", queueName, consumerIndex, e.getMessage());
                try {
                    Thread.sleep(5000); // 异常时休眠避免死循环导致 CPU 和内存飙升
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * 处理消息（不带重试支持）
     */
    private Boolean processMessage(Object bean, ListenerMethodInfo methodInfo, Json messageRecord) {
        try {
            Object dto = JSON.parseObject(messageRecord.value(), methodInfo.msgDtoClass());
            method.invoke(bean, dto);
            log.info("消息处理成功: 队列={}, 类={}, 方法={}", methodInfo.queueName(), methodInfo.className(), methodInfo.methodName());
            return true;
        } catch (Exception e) {
            log.error("消息处理失败: 队列={}, 类={}, 方法={}, 错误={}", methodInfo.queueName(), methodInfo.className(), methodInfo.methodName(), e.getMessage(), e);
            return false;
        }
    }

    /**
     * 处理消息（带重试支持）
     */
    private MessageProcessResult processMessageWithRetry(Object bean, ListenerMethodInfo methodInfo, org.andreaesposito.pgmq.jdbc.client.MessageRecord messageRecord) {
        try {
            Json headers = SysPgMqUtil.extractHeadersFromMessageRecord(messageRecord);
            int retryCount = SysPgMqUtil.getRetryCountFromHeaders(headers);
            String traceId = SysPgMqUtil.getTraceIdFromHeaders(headers);

            try {
                Object dto = JSON.parseObject(messageRecord.message().value(), methodInfo.msgDtoClass());
                method.invoke(bean, dto);
                log.info("消息处理成功: 队列={}, traceId={}, retryCount={}", methodInfo.queueName(), traceId, retryCount);
                return MessageProcessResult.success();
            } catch (Exception e) {
                log.error("消息处理失败: 队列={}, traceId={}, retryCount={}, 错误={}", methodInfo.queueName(), traceId, retryCount, e.getMessage(), e);
                return MessageProcessResult.failure(retryCount + 1, methodInfo.maxRetry(), e.getMessage());
            }
        } catch (Exception e) {
            log.error("消息处理系统异常: 队列={}, 错误={}", methodInfo.queueName(), e.getMessage());
            return MessageProcessResult.failure(1, methodInfo.maxRetry(), e.getMessage());
        }
    }

    /**
     * 注册监听器
     */
    public void registerListener() {
        Class<?> msgDtoClass = annotation.bindMsgDto();
        String msgType = msgDtoClass.getName();
        String className = clazz.getName();
        String methodName = method.getName();
        SubscribeType subscribeType = annotation.subscribeType();

        for (SubscribeType existingType : PgmqFinalConstant.consumerTable.rowKeySet()) {
            if (existingType != subscribeType && PgmqFinalConstant.consumerTable.contains(existingType, msgType)) {
                throw new IllegalStateException(String.format("消息类型 [%s] 已注册为 [%s] 模式，不能同时注册为 [%s] 模式。一个 bindMsgDto 只能对应一种 SubscribeType。冲突位置: %s.%s", msgType, existingType, subscribeType, className, methodName));
            }
        }

        SubscribeStrategy strategy = SubscribeStrategy.forType(subscribeType);
        List<QueueConsumer> existingConsumers = PgmqFinalConstant.consumerTable.get(subscribeType, msgType);
        List<ListenerMethodInfo> existingListeners = existingConsumers != null ?
                existingConsumers.stream().map(QueueConsumer::getMethodInfo).toList() : new ArrayList<>();

        String queueName = strategy.generateQueueName(msgDtoClass, className, methodName, existingListeners);
        this.methodInfo = new ListenerMethodInfo(queueName, className, methodName, beanName, msgDtoClass, subscribeType, annotation.visibilityTimeout(), annotation.batchSize(), annotation.deadLetterConfig());

        List<QueueConsumer> consumerList = PgmqFinalConstant.consumerTable.get(subscribeType, msgType);
        if (consumerList == null) {
            consumerList = new ArrayList<>();
            PgmqFinalConstant.consumerTable.put(subscribeType, msgType, consumerList);
        }
        consumerList.add(this);

        this.needCreateQueue = strategy.shouldCreateQueue(existingListeners);

        if (this.needCreateQueue) {
            log.info("检测到新队列，启动消费者时创建: {} (类型={}, VT={}s, 批量={})", queueName, subscribeType, methodInfo.visibilityTimeout(), methodInfo.batchSize());
        } else {
            log.info("复用队列: {} (类型={}, 类={}, 方法={})", queueName, subscribeType, className, methodName);
        }
        log.debug("注册PgMqListener: 订阅类型={}, 消息类型={}, 队列={}, 类={}, 方法={}", subscribeType, msgType, queueName, className, methodName);
    }

    private void ensureQueueCreated(String queueName) {
        PgMqService pgMqService = SpringUtil.getBean(PgMqService.class);
        pgMqService.create(queueName);
        log.info("创建队列成功: {}", queueName);
    }
}
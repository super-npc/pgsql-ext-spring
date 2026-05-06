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
import bronya.pgsql.ext.mq.domain.Json;

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

    private PgMqService getPgMqService() {
        return SpringUtil.getBean(PgMqService.class);
    }

    private Object getBeanInstance() {
        return SpringUtil.getBean(beanName);
    }

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

        // 初始休眠间隔（毫秒）
        long sleepMs = 100;
        final long MAX_SLEEP_MS = 5000; // 最大休眠 5 秒

        while (true) {
            try {
                boolean hasMessage;
                if (methodInfo.enableDlq()) {
                    // 使用重试感知的消费方法
                    hasMessage = getPgMqService().consumeOnceWithRetry(queueName, methodInfo, methodInfo.visibilityTimeout(), methodInfo.batchSize(), this::processMessageWithRetry);
                } else {
                    // 使用原始的消费方法
                    hasMessage = getPgMqService().consumeOnce(queueName, methodInfo, methodInfo.visibilityTimeout(), methodInfo.batchSize(), this::processMessage);
                }

                if (hasMessage) {
                    // 如果有消息处理，重置休眠时间，以最高速度处理积压消息
                    sleepMs = 100;
                } else {
                    // 如果没有消息，应用层休眠，并逐步增加休眠时间（退避算法）
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
    private Boolean processMessage(ListenerMethodInfo methodInfo, Json messageRecord) {
        try {
            // 将 JSON 消息转换为 DTO 对象
            Object dto = JSON.parseObject(messageRecord.value(), methodInfo.msgDtoClass());
            // 直接使用当前对象的 method 和 bean 实例，避免反射查找和 SpringUtil.getBean 导致的性能和内存问题
            method.invoke(getBeanInstance(), dto);
            log.info("消息处理成功: 队列={}, 类={}, 方法={}", methodInfo.queueName(), methodInfo.className(), methodInfo.methodName());
            return true;
        } catch (Exception e) {
            log.error("消息处理失败: 队列={}, 类={}, 方法={}, 错误={}", methodInfo.queueName(), methodInfo.className(), methodInfo.methodName(), e.getMessage(), e);
            return false;
        }
    }

    /**
     * 处理消息（带重试支持）- 通过反射调用实际的监听方法
     */
    private MessageProcessResult processMessageWithRetry(ListenerMethodInfo methodInfo, bronya.pgsql.ext.mq.domain.MessageRecord messageRecord) {
        try {
            // 尝试获取消息的 headers，解析重试次数
            Json headers = SysPgMqUtil.extractHeadersFromMessageRecord(messageRecord);
            int retryCount = SysPgMqUtil.getRetryCountFromHeaders(headers);
            String traceId = SysPgMqUtil.getTraceIdFromHeaders(headers);

            try {
                // 将 JSON 消息转换为 DTO 对象
                Object dto = JSON.parseObject(messageRecord.message().value(), methodInfo.msgDtoClass());
                // 直接使用当前对象的 method 和 bean 实例
                method.invoke(getBeanInstance(), dto);
                log.info("消息处理成功: 队列={}, 类={}, 方法={}, traceId={}, retryCount={}", methodInfo.queueName(), methodInfo.className(), methodInfo.methodName(), traceId, retryCount);
                return MessageProcessResult.success();

            } catch (Exception e) {
                // 处理方法调用异常
                log.error("消息处理失败: 队列={}, 类={}, 方法={}, traceId={}, retryCount={}, 错误={}", methodInfo.queueName(), methodInfo.className(), methodInfo.methodName(), traceId, retryCount, e.getMessage(), e);
                // 返回失败结果，包含重试信息
                return MessageProcessResult.failure(retryCount + 1, methodInfo.maxRetry(), e.getMessage());
            }

        } catch (Exception e) {
            // 处理获取Bean或其他系统异常
            log.error("消息处理系统异常: 队列={}, 类={}, 方法={}, 错误={}", methodInfo.queueName(), methodInfo.className(), methodInfo.methodName(), e.getMessage(), e);

            // 返回失败结果，使用默认重试配置
            return MessageProcessResult.failure(1, methodInfo.maxRetry(), e.getMessage());
        }
    }

    /**
     * 注册监听器
     *
     * @throws IllegalStateException 当同一个消息类型存在多种 SubscribeType 时抛出
     */
    public void registerListener() {
        Class<?> msgDtoClass = annotation.bindMsgDto();
        String msgType = msgDtoClass.getName();
        String className = clazz.getName();
        String methodName = method.getName();
        SubscribeType subscribeType = annotation.subscribeType();

        // 验证：检查该消息类型是否已存在其他 SubscribeType 的注册
        for (SubscribeType existingType : PgmqFinalConstant.consumerTable.rowKeySet()) {
            if (existingType != subscribeType && PgmqFinalConstant.consumerTable.contains(existingType, msgType)) {
                throw new IllegalStateException(String.format("消息类型 [%s] 已注册为 [%s] 模式，不能同时注册为 [%s] 模式。" + "一个 bindMsgDto 只能对应一种 SubscribeType。冲突位置: %s.%s", msgType, existingType, subscribeType, className, methodName));
            }
        }

        // 使用策略模式获取订阅策略
        SubscribeStrategy strategy = SubscribeStrategy.forType(subscribeType);
        
        List<QueueConsumer> existingConsumers = PgmqFinalConstant.consumerTable.get(subscribeType, msgType);
        List<ListenerMethodInfo> existingListeners = existingConsumers != null ? 
                existingConsumers.stream().map(QueueConsumer::getMethodInfo).toList() : new ArrayList<>();

        // 使用策略生成队列名称
        String queueName = strategy.generateQueueName(msgDtoClass, className, methodName, existingListeners);

        // 创建监听器信息（包含注解配置）
        this.methodInfo = new ListenerMethodInfo(queueName, className, methodName, beanName, msgDtoClass, subscribeType, annotation.visibilityTimeout(), annotation.batchSize(), annotation.deadLetterConfig());

        // 存储到 Table
        List<QueueConsumer> consumerList = PgmqFinalConstant.consumerTable.get(subscribeType, msgType);
        if (consumerList == null) {
            consumerList = new ArrayList<>();
            PgmqFinalConstant.consumerTable.put(subscribeType, msgType, consumerList);
        }
        consumerList.add(this);

        // 使用策略判断是否需要创建队列
        this.needCreateQueue = strategy.shouldCreateQueue(existingListeners);

        if (this.needCreateQueue) {
            log.info("检测到新队列，启动消费者时创建: {} (类型={}, VT={}s, 批量={})", queueName, subscribeType, methodInfo.visibilityTimeout(), methodInfo.batchSize());
        } else {
            log.info("复用队列: {} (类型={}, 类={}, 方法={})", queueName, subscribeType, className, methodName);
        }
        log.debug("注册PgMqListener: 订阅类型={}, 消息类型={}, 队列={}, 类={}, 方法={}", subscribeType, msgType, queueName, className, methodName);
    }

    private void ensureQueueCreated(String queueName) {
        getPgMqService().create(queueName);
        log.info("创建队列成功: {}", queueName);
    }
}

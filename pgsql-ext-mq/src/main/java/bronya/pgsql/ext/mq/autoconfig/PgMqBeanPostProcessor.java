package bronya.pgsql.ext.mq.autoconfig;


import cn.hutool.v7.extra.spring.SpringUtil;
import com.alibaba.fastjson2.JSON;
import com.google.common.collect.TreeBasedTable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import bronya.pgsql.ext.mq.annotation.PgMqDeadLetterConfig;
import bronya.pgsql.ext.mq.annotation.PgMqListener;
import bronya.pgsql.ext.mq.annotation.PgMqListener.SubscribeType;
import bronya.pgsql.ext.mq.autoconfig.strategy.SubscribeStrategy;
import bronya.pgsql.ext.mq.service.PgMqService;
import org.andreaesposito.pgmq.jdbc.client.Json;
import org.jetbrains.annotations.NotNull;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import jakarta.annotation.PreDestroy;

@Slf4j
@Component
@RequiredArgsConstructor
public class PgMqBeanPostProcessor implements BeanPostProcessor, ApplicationListener<ApplicationReadyEvent> {
    private final PgMqService pgMqService;
    private final PgMqYaml pgMqYaml;
    /**
     * 监听器信息表 (按订阅类型和消息类型分组)
     * rowKey: 订阅类型 (CLUSTERING/BROADCASTING)
     * columnKey: 消息类型全限定名 (msgType)
     * value: 该组合下的所有监听方法信息列表
     */
    @Getter
    private final TreeBasedTable<SubscribeType, String, List<ListenerMethodInfo>> listenerTable = TreeBasedTable.create();

    /**
     * 延迟启动的消费者任务列表
     */
    private final List<ConsumerTask> pendingConsumers = new ArrayList<>();

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(true);

    @Override
    public void onApplicationEvent(@NonNull ApplicationReadyEvent event) {
        if (!pgMqYaml.isConsumerEnabled()) {
            log.info("PgMq 消费者已禁用，跳过启动消费者");
            pendingConsumers.clear();
            return;
        }
        this.startAllConsumers();
    }

    @EventListener(ContextClosedEvent.class)
    public void onContextClosed(ContextClosedEvent event) {
        stopConsumers();
    }

    /**
     * 记录监听器方法信息
     */
    public record ListenerMethodInfo(String queueName, String className, String methodName, String beanName,
                                      Class<?> msgDtoClass, SubscribeType subscribeType,
                                      int visibilityTimeout, int batchSize,
                                      PgMqDeadLetterConfig deadLetterConfig) {
        
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

    /**
     * 消费者任务
     */
    private record ConsumerTask(String queueName, ListenerMethodInfo methodInfo, boolean needCreateQueue) {
    }

    @NotNull
    @Override
    public Object postProcessAfterInitialization(@NotNull Object bean, @NotNull String beanName) {
        Class<?> clazz = ClassUtils.getUserClass(bean); // 获取实际类，避免代理类
        for (Method method : ReflectionUtils.getAllDeclaredMethods(clazz)) {
            if (method.isBridge() || method.isSynthetic()) {
                continue;
            }
            PgMqListener annotation = AnnotationUtils.findAnnotation(method, PgMqListener.class);
            if (annotation != null) {
                registerListener(clazz, beanName, method, annotation);
            }
        }
        return bean;
    }

    @SneakyThrows
    @NotNull
    @Override
    public Object postProcessBeforeInitialization(@NotNull Object bean, @NotNull String beanName) {
        return bean;
    }

    /**
     * 注册监听器
     * @throws IllegalStateException 当同一个消息类型存在多种 SubscribeType 时抛出
     */
    private void registerListener(Class<?> clazz, String beanName, Method method, PgMqListener annotation) {
        Class<?> msgDtoClass = annotation.bindMsgDto();
        String msgType = msgDtoClass.getName();
        String className = clazz.getName();
        String methodName = method.getName();
        SubscribeType subscribeType = annotation.subscribeType();

        // 验证：检查该消息类型是否已存在其他 SubscribeType 的注册
        for (SubscribeType existingType : listenerTable.rowKeySet()) {
            if (existingType != subscribeType && listenerTable.contains(existingType, msgType)) {
                throw new IllegalStateException(
                    String.format("消息类型 [%s] 已注册为 [%s] 模式，不能同时注册为 [%s] 模式。" +
                                  "一个 bindMsgDto 只能对应一种 SubscribeType。冲突位置: %s.%s",
                        msgType, existingType, subscribeType, className, methodName)
                );
            }
        }

        // 使用策略模式获取订阅策略
        SubscribeStrategy strategy = SubscribeStrategy.forType(subscribeType);
        List<ListenerMethodInfo> existingListeners = listenerTable.get(subscribeType, msgType);
        
        // 使用策略生成队列名称
        String queueName = strategy.generateQueueName(msgDtoClass, className, methodName, existingListeners);

        // 创建监听器信息（包含注解配置）
        ListenerMethodInfo methodInfo = new ListenerMethodInfo(
                queueName, className, methodName, beanName, msgDtoClass, subscribeType,
                annotation.visibilityTimeout(),
                annotation.batchSize(), annotation.deadLetterConfig());

        // 存储到 Table
        List<ListenerMethodInfo> methodList = listenerTable.get(subscribeType, msgType);
        if (methodList == null) {
            methodList = new ArrayList<>();
            listenerTable.put(subscribeType, msgType, methodList);
        }
        methodList.add(methodInfo);

        // 使用策略判断是否需要创建队列
        boolean needCreateQueue = strategy.shouldCreateQueue(existingListeners);

        // 暂存消费者任务
        pendingConsumers.add(new ConsumerTask(queueName, methodInfo, needCreateQueue));

        if (needCreateQueue) {
            log.info("检测到新队列，启动消费者时创建: {} (类型={}, VT={}s, 批量={})",
                queueName, subscribeType,
                methodInfo.visibilityTimeout(), methodInfo.batchSize());
        } else {
            log.info("复用队列: {} (类型={}, 类={}, 方法={})",
                queueName, subscribeType, className, methodName);
        }

        log.debug("注册PgMqListener: 订阅类型={}, 消息类型={}, 队列={}, 类={}, 方法={}",
                subscribeType, msgType, queueName, className, methodName);
    }

    private final ExecutorService consumerExecutor = Executors.newVirtualThreadPerTaskExecutor();
    /**
     * Spring 上下文完全初始化后启动所有消费者
     */
    public void startAllConsumers() {
        if (pendingConsumers.isEmpty() || !running.get()) {
            return;
        }
        if (!started.compareAndSet(false, true)) {
            return;
        }

        log.info("开始启动所有 PgMq 消费者，共 {} 个队列",
            pendingConsumers.size());

        // 使用虚拟线程池 - 每个任务一个虚拟线程
        // 虚拟线程适合 I/O 密集型任务，如数据库轮询等待
            int index = 0;
            for (ConsumerTask task : pendingConsumers) {
                ListenerMethodInfo info = task.methodInfo();
                final int consumerIndex = index++;
                final String threadName = String.format("pgmq-consumer-%s-%d", task.queueName(), consumerIndex);

                consumerExecutor.submit(() -> {
                    // 设置虚拟线程名称
                    Thread.currentThread().setName(threadName);

                    try {
                        Thread.sleep(100L * consumerIndex); // 错峰启动
                        startConsumer(task.queueName(), info, consumerIndex, task.needCreateQueue());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.warn("消费者线程被中断: 队列={}", task.queueName());
                    }
                });
            }

            pendingConsumers.clear();
            log.info("所有 PgMq 消费者已提交启动");
    }

    

    /**
     * 启动消费者线程
     */
    private void startConsumer(String queueName, ListenerMethodInfo methodInfo, int consumerIndex, boolean needCreateQueue) {
        String threadName = String.format("%s-%d", queueName, consumerIndex);
        Thread.currentThread().setName(threadName);

        log.info("启动消费者: queue={}, consumer={}, class={}, method={}",
                queueName, consumerIndex,
                methodInfo.className(), methodInfo.methodName());

        int consecutiveErrors = 0;
        if (needCreateQueue) {
            this.ensureQueueCreated(queueName);
        }

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                if (methodInfo.enableDlq()) {
                    // 使用重试感知的消费方法
                    pgMqService.consumeOnceWithRetry(queueName, methodInfo, methodInfo.visibilityTimeout(),
                        methodInfo.batchSize(), this::processMessageWithRetry);
                } else {
                    // 使用原始的消费方法
                    pgMqService.consumeOnce(queueName, methodInfo, methodInfo.visibilityTimeout(),
                        methodInfo.batchSize(), this::processMessage);
                }
                consecutiveErrors = 0; // 重置错误计数
            } catch (SQLException e) {
                consecutiveErrors++;
                log.error("消费者异常: queue={}, consumer={}, error={}",
                    queueName, consumerIndex, e.getMessage());

                try {
                    Thread.sleep(Math.min(1000L * consecutiveErrors, 10000)); // 指数退避
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.info("消费者线程已结束: queue={}, consumer={}", queueName, consumerIndex);
    }

    private void ensureQueueCreated(String queueName) {
        int retry = 0;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                pgMqService.create(queueName);
                log.info("创建队列成功: {}", queueName);
                return;
            } catch (Exception e) {
                retry++;
                long sleepMs = Math.min(1000L * retry, 10000);
                log.warn("创建队列失败，将重试: queue={}, retry={}, error={}", queueName, retry, e.getMessage());
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /**
     * 处理消息 - 通过反射调用实际的监听方法
     */
    private Boolean processMessage(ListenerMethodInfo methodInfo, Json message) {
        try {
            // 从 Spring 上下文获取 Bean
            Object bean = SpringUtil.getBean(methodInfo.beanName());

            // 获取方法
            Method method = findMethod(bean.getClass(), methodInfo.methodName(), methodInfo.msgDtoClass());
            if (method == null) {
                log.error("未找到方法: {}.{}", methodInfo.className(), methodInfo.methodName());
                return false;
            }

            // 将 JSON 消息转换为 DTO 对象
            Object dto = JSON.parseObject(message.value(), methodInfo.msgDtoClass());

            // 调用方法
            method.invoke(bean, dto);

            log.info("消息处理成功: 队列={}, 类={}, 方法={}",
                    methodInfo.queueName(), methodInfo.className(), methodInfo.methodName());
            return true;
        } catch (Exception e) {
            log.error("消息处理失败: 队列={}, 类={}, 方法={}, 错误={}",
                    methodInfo.queueName(), methodInfo.className(),
                    methodInfo.methodName(), e.getMessage(), e);
            return false;
        }
    }

    /**
     * 处理消息（带重试支持）- 通过反射调用实际的监听方法
     */
    private PgMqService.MessageProcessResult processMessageWithRetry(
            ListenerMethodInfo methodInfo, org.andreaesposito.pgmq.jdbc.client.MessageRecord messageRecord) {
        try {
            // 尝试获取消息的 headers，解析重试次数
            Json headers = extractHeadersFromMessageRecord(messageRecord);
            int retryCount = getRetryCountFromHeaders(headers);
            String traceId = getTraceIdFromHeaders(headers);
            
            // 从 Spring 上下文获取 Bean
            Object bean = SpringUtil.getBean(methodInfo.beanName());

            // 获取方法
            Method method = findMethod(bean.getClass(), methodInfo.methodName(), methodInfo.msgDtoClass());
            if (method == null) {
                log.error("未找到方法: {}.{}", methodInfo.className(), methodInfo.methodName());
                return PgMqService.MessageProcessResult.failure(
                    retryCount + 1, methodInfo.maxRetry(), "Method not found: " + methodInfo.className() + "." + methodInfo.methodName());
            }

            try {
                // 将 JSON 消息转换为 DTO 对象
                Object dto = JSON.parseObject(messageRecord.message().value(), methodInfo.msgDtoClass());

                // 调用方法
                method.invoke(bean, dto);

                log.info("消息处理成功: 队列={}, 类={}, 方法={}, traceId={}, retryCount={}",
                        methodInfo.queueName(), methodInfo.className(), methodInfo.methodName(),
                        traceId, retryCount);
                return PgMqService.MessageProcessResult.success();
                
            } catch (Exception e) {
                // 处理方法调用异常
                log.error("消息处理失败: 队列={}, 类={}, 方法={}, traceId={}, retryCount={}, 错误={}",
                        methodInfo.queueName(), methodInfo.className(),
                        methodInfo.methodName(), traceId, retryCount, e.getMessage(), e);
                
                // 返回失败结果，包含重试信息
                return PgMqService.MessageProcessResult.failure(
                    retryCount + 1, methodInfo.maxRetry(), e.getMessage());
            }
            
        } catch (Exception e) {
            // 处理获取Bean或其他系统异常
            log.error("消息处理系统异常: 队列={}, 类={}, 方法={}, 错误={}",
                    methodInfo.queueName(), methodInfo.className(),
                    methodInfo.methodName(), e.getMessage(), e);
            
            // 返回失败结果，使用默认重试配置
            return PgMqService.MessageProcessResult.failure(
                1, methodInfo.maxRetry(), e.getMessage());
        }
    }

    /**
     * 从 MessageRecord 中提取 Headers
     */
    private Json extractHeadersFromMessageRecord(
            org.andreaesposito.pgmq.jdbc.client.MessageRecord messageRecord) {
        try {
            // 尝试通过反射获取 headers 字段
            var headersField = messageRecord.getClass().getMethod("headers");
            Object headersObj = headersField.invoke(messageRecord);
            
            if (headersObj instanceof Json) {
                return (Json) headersObj;
            } else if (headersObj != null) {
                return new Json(headersObj.toString());
            }
        } catch (Exception e) {
            // 如果无法获取 headers，返回空对象
        }
        
        return new Json("{}");
    }
    
    // 修复：更改返回类型为 MessageProcessResult，但保留原有参数
    /**
     * 从 Headers 中解析重试次数
     */
    private int getRetryCountFromHeaders(Json headers) {
        if (headers == null || headers.value() == null || "{}".equals(headers.value())) {
            return 0;
        }
        
        try {
            String value = headers.value();
            // 简单的 JSON 解析，查找 retry-count 字段
            String key = "\"x-retry-count\":";
            int startIdx = value.indexOf(key);
            if (startIdx == -1) {
                return 0;
            }
            
            startIdx += key.length();
            int endIdx = value.indexOf(",", startIdx);
            if (endIdx == -1) {
                endIdx = value.indexOf("}", startIdx);
            }
            
            if (endIdx > startIdx) {
                String countStr = value.substring(startIdx, endIdx).trim();
                return Integer.parseInt(countStr);
            }
        } catch (Exception e) {
            log.warn("解析重试次数失败: {}", e.getMessage());
        }
        
        return 0;
    }
    
    /**
     * 从 Headers 中解析跟踪 ID
     */
    private String getTraceIdFromHeaders(Json headers) {
        if (headers == null || headers.value() == null || "{}".equals(headers.value())) {
            return "unknown";
        }
        
        try {
            String value = headers.value();
            String key = "\"x-retry-trace-id\":\"";
            int startIdx = value.indexOf(key);
            if (startIdx == -1) {
                return "unknown";
            }
            
            startIdx += key.length();
            int endIdx = value.indexOf("\"", startIdx);
            
            if (endIdx > startIdx) {
                return value.substring(startIdx, endIdx);
            }
        } catch (Exception e) {
            log.warn("解析跟踪 ID 失败: {}", e.getMessage());
        }
        
        return "unknown";
    }
    
    /**
     * 查找匹配的方法
     */
    private Method findMethod(Class<?> clazz, String methodName, Class<?> paramType) {
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.getName().equals(methodName) && method.getParameterCount() == 1) {
                if (method.getParameterTypes()[0].isAssignableFrom(paramType)) {
                    method.setAccessible(true);
                    return method;
                }
            }
        }
        return null;
    }

    /**
     * 获取指定消息类型的订阅类型
     * 一个消息类型只能对应一种订阅类型
     */
    public SubscribeType getSubscribeType(String msgType) {
        for (SubscribeType type : listenerTable.rowKeySet()) {
            if (listenerTable.contains(type, msgType)) {
                return type;
            }
        }
        return null;
    }


    /**
     * 获取指定消息类型的监听方法列表
     */
    public List<ListenerMethodInfo> getListenerMethods(String msgType) {
        SubscribeType type = getSubscribeType(msgType);
        if (type == null) {
            return List.of();
        }
        List<ListenerMethodInfo> methods = listenerTable.get(type, msgType);
        return methods != null ? List.copyOf(methods) : List.of();
    }

    /**
     * 获取指定消息类型+订阅类型的队列名称列表
     */
    public List<String> getQueueNames(String msgType) {
        return getListenerMethods(msgType).stream()
                .map(ListenerMethodInfo::queueName)
                .toList();
    }

    /**
     * 应用关闭时优雅关闭消费者线程池
     */
    @PreDestroy
    public void shutdown() {
        stopConsumers();
    }

    private void stopConsumers() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        pendingConsumers.clear();
        listenerTable.clear();


        if (!consumerExecutor.isShutdown()) {
            consumerExecutor.shutdownNow();
            try {
                consumerExecutor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("PgMq 消费者线程池已关闭");
    }
}

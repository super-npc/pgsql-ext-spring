package bronya.pgsql.ext.mq.autoconfig;

import bronya.pgsql.ext.mq.annotation.PgMqListener;
import bronya.pgsql.ext.mq.annotation.PgMqListener.SubscribeType;
import bronya.pgsql.ext.mq.constant.PgmqFinalConstant;
import bronya.pgsql.ext.mq.consumer.ListenerMethodInfo;
import bronya.pgsql.ext.mq.consumer.QueueConsumer;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Component
@RequiredArgsConstructor
public class PgMqBeanPostProcessor implements BeanPostProcessor, ApplicationListener<ApplicationReadyEvent> {
    private final PgMqYaml pgMqYaml;

    @Override
    public void onApplicationEvent(@NonNull ApplicationReadyEvent event) {
        if (!pgMqYaml.isConsumerEnabled()) {
            log.info("PgMq 消费者已禁用，跳过启动消费者");
            return;
        }
        this.startAllConsumers();
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
                // 应该是注册对象
                QueueConsumer queueConsumer = new QueueConsumer(clazz, beanName, method, annotation);
                queueConsumer.registerListener();
            }
        }
        return bean;
    }

    private final ExecutorService consumerExecutor = Executors.newVirtualThreadPerTaskExecutor();

    /**
     * Spring 上下文完全初始化后启动所有消费者
     */
    public void startAllConsumers() {
        List<QueueConsumer> allConsumers = PgmqFinalConstant.consumerTable.values().stream()
                .flatMap(List::stream)
                .toList();

        log.info("开始启动所有 PgMq 消费者，共 {} 个队列", allConsumers.size());

        // 使用虚拟线程池 - 每个任务一个虚拟线程
        // 虚拟线程适合 I/O 密集型任务，如数据库轮询等待
        int index = 0;
        for (QueueConsumer consumer : allConsumers) {
            final int consumerIndex = index++;
            String queueName = consumer.getMethodInfo() != null ? consumer.getMethodInfo().queueName() : "unknown";
            final String threadName = String.format("pgmq-consumer-%s-%d", queueName, consumerIndex);

            consumerExecutor.submit(() -> {
                // 设置虚拟线程名称
                Thread.currentThread().setName(threadName);

                try {
                    Thread.sleep(100L * consumerIndex); // 错峰启动
                    consumer.start(consumerIndex);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("消费者线程被中断: 队列={}", queueName);
                }
            });
        }

        log.info("所有 PgMq 消费者已提交启动");
    }


    /**
     * 获取指定消息类型的订阅类型
     * 一个消息类型只能对应一种订阅类型
     */
    public SubscribeType getSubscribeType(String msgType) {
        for (SubscribeType type : PgmqFinalConstant.consumerTable.rowKeySet()) {
            if (PgmqFinalConstant.consumerTable.contains(type, msgType)) {
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
        List<QueueConsumer> consumers = PgmqFinalConstant.consumerTable.get(type, msgType);
        return consumers != null ? consumers.stream()
                .map(QueueConsumer::getMethodInfo)
                .filter(info -> info != null)
                .toList() : List.of();
    }

    /**
     * 获取指定消息类型+订阅类型的队列名称列表
     */
    public List<String> getQueueNames(String msgType) {
        return getListenerMethods(msgType).stream().map(ListenerMethodInfo::queueName).toList();
    }


    @SneakyThrows
    @NotNull
    @Override
    public Object postProcessBeforeInitialization(@NotNull Object bean, @NotNull String beanName) {
        return bean;
    }
}

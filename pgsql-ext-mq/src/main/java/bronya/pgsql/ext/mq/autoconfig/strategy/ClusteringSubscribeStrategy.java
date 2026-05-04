package bronya.pgsql.ext.mq.autoconfig.strategy;

import bronya.pgsql.ext.mq.annotation.PgMqListener.SubscribeType;
import bronya.pgsql.ext.mq.consumer.ListenerMethodInfo;
import cn.hutool.v7.core.text.StrUtil;

import java.util.List;

/**
 * 集群订阅策略（竞争消费）
 * 特点：
 * 1. 同消息类型的多个监听器共用同一个队列
 * 2. 每条消息只被一个消费者消费一次
 * 3. 只有第一个监听器需要创建队列，后续监听器复用已有队列
 */
public class ClusteringSubscribeStrategy implements SubscribeStrategy {

    public static final ClusteringSubscribeStrategy INSTANCE = new ClusteringSubscribeStrategy();

    private ClusteringSubscribeStrategy() {
    }

    @Override
    public SubscribeType getSubscribeType() {
        return SubscribeType.CLUSTERING;
    }

    @Override
    public String generateQueueName(Class<?> msgDtoClass, String className, String methodName,
                                    List<ListenerMethodInfo> existingListeners) {
        // CLUSTERING 模式：复用已有队列（如果存在）
        if (existingListeners != null && !existingListeners.isEmpty()) {
            return existingListeners.getFirst().queueName();
        }

        // 获取消息类的简单名称
        String msgDtoName = getQueueBaseName(msgDtoClass, methodName);

        // CLUSTERING 模式：只用消息类名命名，不包含方法名
        return formatQueueName(msgDtoName);
    }

    @Override
    public boolean shouldCreateQueue(List<ListenerMethodInfo> existingListeners) {
        // CLUSTERING 模式下，只有第一个监听器需要创建队列
        return existingListeners == null || existingListeners.isEmpty();
    }

    @Override
    public String getQueueBaseName(Class<?> msgDtoClass, String methodName) {
        // CLUSTERING 模式：只用消息类名，不包含方法名
        return StrUtil.toUnderlineCase(msgDtoClass.getSimpleName());
    }
}
package bronya.pgsql.ext.mq.autoconfig.strategy;

import bronya.pgsql.ext.mq.annotation.PgMqListener.SubscribeType;
import bronya.pgsql.ext.mq.autoconfig.PgMqBeanPostProcessor.ListenerMethodInfo;

import java.util.List;

/**
 * 订阅策略接口
 * 根据不同的订阅类型（CLUSTERING/BROADCASTING）实现不同的行为
 */
public interface SubscribeStrategy {
    
    /**
     * 获取订阅类型
     */
    SubscribeType getSubscribeType();
    
    /**
     * 生成队列名称
     * 
     * @param msgDtoClass 消息DTO类
     * @param className 监听器类名
     * @param methodName 监听器方法名
     * @param existingListeners 已存在的监听器列表（同消息类型）
     * @return 队列名称
     */
    String generateQueueName(Class<?> msgDtoClass, String className, String methodName, 
                            List<ListenerMethodInfo> existingListeners);
    
    /**
     * 判断是否需要创建新队列
     * 
     * @param existingListeners 已存在的监听器列表（同消息类型）
     * @return true表示需要创建队列，false表示可以复用现有队列
     */
    boolean shouldCreateQueue(List<ListenerMethodInfo> existingListeners);
    
    /**
     * 是否复用现有队列
     * 
     * @param existingListeners 已存在的监听器列表（同消息类型）
     * @return true表示复用现有队列，false表示需要创建新队列
     */
    default boolean shouldReuseExistingQueue(List<ListenerMethodInfo> existingListeners) {
        return !shouldCreateQueue(existingListeners);
    }
    
    /**
     * 获取队列名称的基础部分（不包含类型后缀）
     */
    default String getQueueBaseName(Class<?> msgDtoClass, String methodName) {
        return msgDtoClass.getSimpleName();
    }
    
    /**
     * 获取订阅类型简写（用于队列名称后缀）
     */
    default String getTypeShort() {
        return getSubscribeType() == SubscribeType.CLUSTERING ? "clu" : "bro";
    }
    
    /**
     * 格式化队列名称（添加类型后缀）
     */
    default String formatQueueName(String baseName) {
        return String.format("%s_%s", getTypeShort(),baseName);
    }
    
    
    
    /**
     * 根据订阅类型获取对应的策略实现
     */
    static SubscribeStrategy forType(SubscribeType type) {
        return type == SubscribeType.CLUSTERING ? 
               ClusteringSubscribeStrategy.INSTANCE : 
               BroadcastingSubscribeStrategy.INSTANCE;
    }
}
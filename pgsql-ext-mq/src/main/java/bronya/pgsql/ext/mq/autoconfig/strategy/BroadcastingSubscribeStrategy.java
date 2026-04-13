package bronya.pgsql.ext.mq.autoconfig.strategy;

import cn.hutool.core.lang.Assert;
import cn.hutool.v7.core.text.StrUtil;
import bronya.pgsql.ext.mq.annotation.PgMqListener.SubscribeType;
import bronya.pgsql.ext.mq.autoconfig.PgMqBeanPostProcessor.ListenerMethodInfo;

import java.util.List;

/**
 * 广播订阅策略（独立消费）
 * 特点：
 * 1. 每个监听器方法都有独立的队列
 * 2. 每条消息会被所有消费者消费
 * 3. 每个监听器都需要创建独立的队列
 */
public class BroadcastingSubscribeStrategy implements SubscribeStrategy {
    
    public static final BroadcastingSubscribeStrategy INSTANCE = new BroadcastingSubscribeStrategy();
    
    private BroadcastingSubscribeStrategy() {}
    
    @Override
    public SubscribeType getSubscribeType() {
        return SubscribeType.BROADCASTING;
    }
    
    @Override
    public String generateQueueName(Class<?> msgDtoClass, String className, String methodName, 
                                   List<ListenerMethodInfo> existingListeners) {
        // BROADCASTING 模式：每个方法独立队列，不检查已存在的监听器
        
        // 获取消息类的简单名称
        String msgDtoName = StrUtil.toUnderlineCase(msgDtoClass.getSimpleName());
        
        // BROADCASTING 模式：方法名取前20字符（保留原大小写），每个方法独立队列
        
        // BROADCASTING 模式：msgDtoName_methodName_bro
        String queueName = String.format("%s_%s_%s", getTypeShort(), methodName, msgDtoName);
        
        // 确保不超过47字符（pgmq限制）
        Assert.isTrue(queueName.length() < 47,"队列名称太长:"+methodName);
        return queueName;
    }
    
    @Override
    public boolean shouldCreateQueue(List<ListenerMethodInfo> existingListeners) {
        // BROADCASTING 模式：每个监听器都需要创建独立的队列
        return true;
    }
    
    /**
     * BROADCASTING 模式需要完整的队列名称格式化，重写此方法
     */
    @Override
    public String formatQueueName(String baseName) {
        // BROADCASTING 模式不使用此方法，需要在 generateQueueName 中完整构建
        throw new UnsupportedOperationException("BroadcastingSubscribeStrategy does not use formatQueueName");
    }
}
package bronya.pgsql.ext.mq.annotation;

import java.lang.annotation.*;

/**
 * 死信队列配置
 * 包含死信队列启用状态及相关的重试配置
 */
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface PgMqDeadLetterConfig {
    
    /** 是否启用死信队列 */
    boolean enableDlq() default true;
    
    /** 最大重试次数（当消息处理失败时） */
    int maxRetry() default 3;
    
    /** 重试延迟时间（秒，0表示立即重试） */
    int retryDelaySeconds() default 0;
}
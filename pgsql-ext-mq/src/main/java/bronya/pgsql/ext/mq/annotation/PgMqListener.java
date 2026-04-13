package bronya.pgsql.ext.mq.annotation;

import org.springframework.messaging.handler.annotation.MessageMapping;

import java.io.Serializable;
import java.lang.annotation.*;


@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
public @interface PgMqListener {
    /** 订阅模型 */
    SubscribeType subscribeType();

    /** 绑定消息对象 */
    Class<? extends Serializable> bindMsgDto();

    /** 可见性超时时间（秒） */
    int visibilityTimeout() default 60;

    /** 批量拉取数量,多余的,目前没有想好怎么处理 */
    int batchSize() default 1;

    /** 死信队列配置 */
    PgMqDeadLetterConfig deadLetterConfig() default @PgMqDeadLetterConfig;

    // 订阅类型
    enum SubscribeType {
        /** 集群：多个消费者竞争消费，每条消息只被消费一次 */
        CLUSTERING,
        /** 广播：每个消费者独立消费，每条消息被所有消费者消费 */
        BROADCASTING
    }
}

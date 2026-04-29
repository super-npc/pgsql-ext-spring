package bronya.pgsql.ext.mq.annotation;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.messaging.handler.annotation.MessageMapping;

import bronya.pgsql.ext.mq.annotation.PgMqListener.SubscribeType;

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

    /** 可见性超时时间（秒） 
     *消费者把消息 read/readWithPoll 拉出来以后，这条消息会在 VT 秒内对其他消费者“不可见” ，避免被并发重复消费；
     如果在这段时间内没有被成功确认（这里是 delete ），VT 到期后消息会 再次变为可见 ，从而触发重投/重试。
    */
    int visibilityTimeout() default 60;

    /** 批量拉取数量,多余的,目前没有想好怎么处理 */
    int batchSize() default 1;

    /** 死信队列配置 */
    PgMqDeadLetterConfig deadLetterConfig() default @PgMqDeadLetterConfig;

    // 订阅类型
    @Getter
    @AllArgsConstructor
    enum SubscribeType {
        /** 集群：多个消费者竞争消费，每条消息只被消费一次 */
        CLUSTERING("集群"),
        /** 广播：每个消费者独立消费，每条消息被所有消费者消费 */
        BROADCASTING("广播");

        private final String desc;
    }
}

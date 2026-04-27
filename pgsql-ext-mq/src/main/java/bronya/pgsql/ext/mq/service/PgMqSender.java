package bronya.pgsql.ext.mq.service;

import com.alibaba.fastjson2.JSONObject;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import bronya.pgsql.ext.mq.annotation.PgMqListener.SubscribeType;
import bronya.pgsql.ext.mq.autoconfig.PgMqBeanPostProcessor;
import bronya.pgsql.ext.mq.service.PgMqService;
import org.andreaesposito.pgmq.jdbc.client.Json;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * PgMq 消息发送器
 *
 * <p>使用说明：
 * <ul>
 *   <li>一个 bindMsgDto (消息类型) 只能对应一种 SubscribeType，否则会产生冲突</li>
 *   <li>发送时只需传入消息对象，会自动根据消息类型找到对应的订阅模式和队列</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PgMqSender<T> {

    private final PgMqBeanPostProcessor pgMqBeanPostProcessor;
    private final PgMqService pgMqService;

    public List<Long> send(T t) {
        return this.send(t,0);
    }

    /**
     * 发送消息
     *
     * <p>根据消息对象的 Class 类型自动查找对应的 SubscribeType 和队列进行发送
     *
     * @param t 消息对象
     * @return 发送成功的消息 ID 列表（可能发送到多个队列，返回所有消息 ID）
     */
    @SneakyThrows
    public List<Long> send(T t,int delaySeconds) {
        if (t == null) {
            log.warn("发送的消息对象为空");
            return List.of();
        }

        String msgType = t.getClass().getName();

        // 获取订阅类型
        SubscribeType subscribeType = pgMqBeanPostProcessor.getSubscribeType(msgType);
        if (subscribeType == null) {
            log.warn("没有找到消息类型 [{}] 的订阅配置", msgType);
            return List.of();
        }

        // 获取队列列表
        List<String> queueNames = pgMqBeanPostProcessor.getQueueNames(msgType);
        if (queueNames.isEmpty()) {
            log.warn("没有找到消息类型 [{}] 的队列配置", msgType);
            return List.of();
        }

        // 将消息对象转换为 JSON
        String jsonMsg = JSONObject.toJSONString(t);
        Json message = new Json(jsonMsg);

        // 发送到所有相关队列，收集消息 ID
        List<Long> allMsgIds = new ArrayList<>();

        if (subscribeType == SubscribeType.CLUSTERING) {
            // CLUSTERING 模式：无论有多少个 PgMqListener，只发送到一个队列（第一个）
            String queueName = queueNames.getFirst();
            List<Long> msgIds = pgMqService.send(queueName, message);
            allMsgIds.addAll(msgIds);
            log.debug("发送消息(CLUSTERING): 队列={}, msgIds={}, 消息={}",
                    queueName, msgIds, jsonMsg);
            log.info("消息发送成功: 类型={}, 订阅模式=CLUSTERING, 队列数=1, 消息ID数={}",
                    msgType, allMsgIds.size());
        } else {
            // BROADCASTING 模式：发送到所有队列
            for (String queueName : queueNames) {
                List<Long> msgIds = pgMqService.send(queueName, message,delaySeconds);
                allMsgIds.addAll(msgIds);
                log.debug("发送消息(BROADCASTING): 队列={}, msgIds={}, 消息={}",
                        queueName, msgIds, jsonMsg);
            }
            log.info("消息发送成功: 类型={}, 订阅模式=BROADCASTING, 队列数={}, 消息ID数={}",
                    msgType, queueNames.size(), allMsgIds.size());
        }

        return allMsgIds;
    }

}
